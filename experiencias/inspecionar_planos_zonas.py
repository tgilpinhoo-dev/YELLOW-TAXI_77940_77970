"""Guardar planos iniciais dos joins, fora das medições de desempenho.

Executar com spark-submit, as mesmas opções de ligação e /project no PYTHONPATH.
Não materializa a cache nem calcula os resultados completos.
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
out = project / 'experiencias/resultados/planos_zonas'
out.mkdir(parents=True, exist_ok=True)
try:
    trips, files = pl.read_trips(spark, str(project/'data/raw'), args.years, 1.0)
    zones = pl.read_zones(spark, str(project/'data/raw'))
    clean = pl.clean_trips(pl.add_quality_flags(trips)).cache()
    evidence = []
    for strategy, use_broadcast in [('broadcast', True), ('sortmerge', False)]:
        pickup = pl.add_pickup_location(clean, zones, use_broadcast)
        routes, _, _ = pl.o2_routes(clean, zones, use_broadcast)
        for name, query in [('m2', pl.m2_demand(pickup)), ('o2_routes', routes)]:
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                query.explain(mode='formatted')
            plan = stream.getvalue()
            filename = f'{name}_{strategy}.txt'
            (out/filename).write_text(plan, encoding='utf-8')
            operators = [line.strip() for line in plan.splitlines()
                         if any(word in line for word in ['BroadcastHashJoin', 'SortMergeJoin', 'BroadcastExchange'])]
            expected = 'BroadcastHashJoin' if use_broadcast else 'SortMergeJoin'
            assert expected in plan, (name, strategy)
            evidence.append(dict(query=name, strategy=strategy, file=filename, operators=operators))
            print(name, strategy, expected, flush=True)
    (out/'resumo.json').write_text(json.dumps({
        'scope': 'planos iniciais; cache declarada mas nao materializada; fora das medicoes',
        'files': files, 'fraction': 1.0, 'spark_version': spark.version,
        'application_id': spark.sparkContext.applicationId, 'plans': evidence
    }, ensure_ascii=False, indent=2), encoding='utf-8')
finally:
    spark.stop()
