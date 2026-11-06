"""Conferir planos finais de M2, depois e fora das medições de desempenho.

Lê os mesmos dados a 100%, materializa clean uma vez e recolhe M2 com AQE
ligado/desligado. A cache é reutilizada nesta inspeção: não comparar tempos.
"""
import contextlib
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace

project = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project))
import projeto1 as pl

args = SimpleNamespace(years=[2023, 2024], no_aqe=False, master=None,
                       driver_memory=None, shuffle_partitions=200)
spark = pl.build_spark(args)
spark.sparkContext.setLogLevel('WARN')
out = project/'experiencias/resultados/planos_aqe'
out.mkdir(parents=True, exist_ok=True)
try:
    settings = {name: spark.conf.get(name) for name in [
        'spark.sql.adaptive.enabled',
        'spark.sql.adaptive.coalescePartitions.enabled',
        'spark.sql.adaptive.skewJoin.enabled',
        'spark.sql.adaptive.localShuffleReader.enabled',
        'spark.sql.shuffle.partitions'
    ]}
    trips, files = pl.read_trips(spark, str(project/'data/raw'), args.years, 1.0)
    zones = pl.read_zones(spark, str(project/'data/raw'))
    clean = pl.clean_trips(pl.add_quality_flags(trips)).cache()
    clean_rows = clean.count()
    evidence = []
    for enabled in [True, False]:
        spark.conf.set('spark.sql.adaptive.enabled', str(enabled).lower())
        query = pl.m2_demand(pl.add_pickup_location(clean, zones, True))
        rows = query.collect()
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            query.explain(mode='formatted')
        plan = stream.getvalue()
        filename = 'm2_aqe_' + ('ligado' if enabled else 'desligado') + '.txt'
        (out/filename).write_text(plan, encoding='utf-8')
        operators = [line.strip() for line in plan.splitlines()
                     if any(word in line for word in ['AdaptiveSparkPlan', 'AQEShuffleRead', 'coalesced', 'isFinalPlan'])]
        evidence.append(dict(aqe=enabled, rows=len(rows), file=filename, operators=operators))
        print('AQE', enabled, 'M2 rows', len(rows), operators, flush=True)
    (out/'resumo.json').write_text(json.dumps({
        'scope': 'inspecao separada: clean materializada uma vez; M2 collect e plano final; nao e benchmark',
        'files': files, 'fraction': 1.0, 'clean_rows': clean_rows,
        'spark_version': spark.version, 'application_id': spark.sparkContext.applicationId,
        'settings': settings, 'plans': evidence
    }, ensure_ascii=False, indent=2), encoding='utf-8')
finally:
    spark.stop()
