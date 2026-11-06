"""Conferir as corridas completas a 10%, depois das medições."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

root=Path(__file__).resolve().parents[1]
results=root/'experiências docker/resultados'
source=root/'data/experiencias'
keys={
    'm1_cleaning_stats.csv':['source_year'],
    'm2_demand':['pickup_borough','day_of_week','hour_of_day'],
    'm3_zone_month':['PULocationID','pickup_year','pickup_month'],
    'o1_anomaly_rates.csv':['pickup_borough','payment_type','day_type'],
    'o2_route_month':['PULocationID','DOLocationID','pickup_year','pickup_month'],
    'o2_q1_q3_reversal':['PULocationID','DOLocationID','pickup_year'],
    'o2_top_routes.csv':['PULocationID','DOLocationID']
}
comparisons=[]
invariants=[]

def read(path,name):
    if path.is_dir():
        assert (path/'_SUCCESS').exists(),path
        frame=ds.dataset(path,format='parquet',partitioning='hive').to_table().to_pandas()
    else:frame=pd.read_csv(path)
    assert not frame.duplicated(keys[name]).any(),(path,'duplicate keys')
    return frame

def compare(label,name,a,b):
    a=a.set_index(keys[name]).sort_index().sort_index(axis=1)
    b=b.set_index(keys[name]).sort_index().sort_index(axis=1)
    assert a.index.equals(b.index) and a.columns.equals(b.columns),(label,name,'keys/columns')
    differences=0
    maximum=0.0
    for column in a.columns:
        x,y=a[column],b[column]
        if pd.api.types.is_float_dtype(x.dtype):
            same=np.isclose(x.to_numpy(dtype=float),y.to_numpy(dtype=float),atol=1e-9,rtol=1e-12,equal_nan=True)
            delta=(x-y).abs().max()
            if pd.notna(delta):maximum=max(maximum,float(delta))
        else:same=(x.eq(y)|(x.isna()&y.isna())).fillna(False).to_numpy()
        differences+=int((~same).sum())
    comparisons.append(dict(comparison=label,table=name,rows=len(a),differences=differences,max_float_difference=maximum))
    print(label,name,len(a),differences,flush=True)

def check(name,value):
    invariants.append(dict(test=name,passed=bool(value)))
    assert value,name

folder=source/'amostra_10pct_cache_01'
info=json.loads((folder/'run_info.json').read_text(encoding='utf-8'))
reference=json.loads((source/'escala_3w_cache_01/run_info.json').read_text(encoding='utf-8'))
frames={name:read(folder/name,name) for name in keys}
for name in ['years','files','modules','master','parallelism','spark_version','cache','broadcast','aqe','o1_method','shuffle_partitions','executor_memory']:
    check('configuracao igual a 100%: '+name,info[name]==reference[name])
check('fracao pedida 10%',info['fraction']==0.1)
valid=info['clean_rows']
stats=frames['m1_cleaning_stats.csv']
full_stats=pd.read_csv(source/'escala_3w_cache_01/m1_cleaning_stats.csv')
check('M1 anos 2023 e 2024',set(stats.source_year)=={2023,2024})
check('M1 mantidas igual a clean.count',int(stats.kept.sum())==valid)
check('M1 total menos invalidas igual a mantidas',stats.total.sub(stats.f_any).eq(stats.kept).all())
check('M2 soma das viagens',int(frames['m2_demand'].trip_count.sum())==valid)
m3=frames['m3_zone_month']
check('M3 grelha completa',len(m3)==263*24 and set(m3.pickup_year)=={2023,2024} and set(m3.pickup_month)==set(range(1,13)))
check('M3 soma das viagens',int(m3.trip_count.sum())==valid)
o1=frames['o1_anomaly_rates.csv']
check('O1 soma das viagens',int(o1.trips.sum())==valid)
check('O1 contagens e taxas validas',o1.anomalies.ge(0).all() and o1.anomalies.le(o1.trips).all() and o1.anomaly_rate_pct.between(0,100).all())
routes=frames['o2_route_month']
check('O2 soma das viagens',int(routes.trip_count.sum())==valid)
check('O2 shares validas',routes.route_share.between(0,1).all() and np.isclose(routes.groupby(['PULocationID','pickup_year','pickup_month']).route_share.sum(),1,atol=1e-9,rtol=1e-12).all())
top=frames['o2_top_routes.csv']
check('O2 top e anos com inversao',top.route_rank.between(1,20).all() and top.groupby('pickup_borough').size().le(20).all() and top.years_with_reversal.between(0,2).all())
for number in [2,3]:
    other=source/f'amostra_10pct_cache_{number:02d}'
    other_info=json.loads((other/'run_info.json').read_text(encoding='utf-8'))
    assert other_info['clean_rows']==valid and other_info['fraction']==0.1
    for name in keys:compare(f'repeticao {number} vs 1',name,frames[name],read(other/name,name))

# Conferir também as tabelas a 10% já validadas, se estiverem neste PC.
legacy={
    'm2_demand':root/'data/output-notebook/validacao_m2',
    'm3_zone_month':root/'data/output-notebook/validacao_m3',
    'o2_route_month':root/'data/output-notebook/validacao_o2/route_month',
    'o2_q1_q3_reversal':root/'data/output-notebook/validacao_o2/reversal',
    'o2_top_routes.csv':root/'data/output-notebook/validacao_o2/top'
}
legacy_available=all(path.is_dir() for path in legacy.values())
if legacy_available:
    for name,path in legacy.items():compare('vs validacao anterior a 10%',name,frames[name],read(path,name))
report=dict(fraction=0.1,seed_in_read_trips=42,source_files=24,
            sample_raw_rows=int(stats.total.sum()),sample_valid_rows=valid,
            full_raw_rows=int(full_stats.total.sum()),full_valid_rows=reference['clean_rows'],
            observed_raw_fraction=float(stats.total.sum()/full_stats.total.sum()),
            observed_valid_fraction=float(valid/reference['clean_rows']),
            invariants=invariants,comparisons=comparisons,
            legacy_validation_available=legacy_available,float_atol=1e-9,float_rtol=1e-12)
(results/'verificacao_amostra_10pct.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
assert all(x['differences']==0 for x in comparisons),'Tabelas diferentes'
print('PASSOU:',len(invariants),'verificacoes;',len(comparisons),'comparacoes',flush=True)
