"""Inspecionar M2 a 12/24/200 partições, fora das medições de desempenho.

Reutiliza a mesma cache entre consultas. Os tempos desta inspeção não são
comparados com as corridas completas da pipeline.
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
out = project/'experiencias/resultados/planos_particoes'
out.mkdir(parents=True, exist_ok=True)
try:
    trips, files = pl.read_trips(spark, str(project/'data/raw'), args.years, 1.0)
    input_partitions = trips.rdd.getNumPartitions()
    zones = pl.read_zones(spark, str(project/'data/raw'))
    clean = pl.clean_trips(pl.add_quality_flags(trips)).cache()
    clean_rows = clean.count()
    evidence = []
    for partitions in [12, 24, 200]:
        spark.conf.set('spark.sql.shuffle.partitions', partitions)
        query = pl.m2_demand(pl.add_pickup_location(clean, zones, True))
        rows = query.collect()
        result_partitions = query.rdd.getNumPartitions()
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            query.explain(mode='formatted')
        plan = stream.getvalue()
        filename = f'm2_shuffle_{partitions}.txt'
        (out/filename).write_text(plan, encoding='utf-8')
        operators = [line.strip() for line in plan.splitlines()
                     if any(word in line for word in ['hashpartitioning', 'rangepartitioning',
                         'AQEShuffleRead', 'coalesced', 'isFinalPlan'])]
        evidence.append(dict(shuffle_partitions=partitions, rows=len(rows),
                             result_partitions=result_partitions, file=filename,
                             operators=operators))
        print('shuffle', partitions, 'M2 rows', len(rows), 'result partitions', result_partitions, flush=True)
    (out/'resumo.json').write_text(json.dumps({
        'scope': 'inspecao de M2 collect, cache partilhada entre consultas; nao e benchmark',
        'files': files, 'fraction': 1.0, 'input_partitions': input_partitions,
        'clean_rows': clean_rows, 'aqe': spark.conf.get('spark.sql.adaptive.enabled'),
        'spark_version': spark.version, 'application_id': spark.sparkContext.applicationId,
        'plans': evidence
    }, ensure_ascii=False, indent=2), encoding='utf-8')
finally:
    spark.stop()
