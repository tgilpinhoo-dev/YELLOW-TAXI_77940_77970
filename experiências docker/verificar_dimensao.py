"""Conferir saídas e metadados das três dimensões, depois das medições."""
from pathlib import Path
import itertools
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

root=Path(__file__).resolve().parents[1]
output=root/'experiências docker/resultados'
cases={
    '2023':([2023],'dimensao_2023_cache'),
    '2023+2024':([2023,2024],'escala_3w_cache'),
    '2022+2023+2024':([2022,2023,2024],'dimensao_2022_2023_2024_cache')
}
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

def check(period,test,condition):
    invariants.append(dict(period=period,test=test,passed=bool(condition)))
    assert condition,(period,test)

def read(folder,name):
    path=folder/name
    if path.is_dir():
        assert (path/'_SUCCESS').exists(),path
        frame=ds.dataset(path,format='parquet',partitioning='hive').to_table().to_pandas()
    else:
        frame=pd.read_csv(path)
    assert not frame.duplicated(keys[name]).any(),(folder.name,name,'duplicate keys')
    return frame

def compare(label,name,a,b,columns=None):
    a=a.set_index(keys[name]).sort_index()
    b=b.set_index(keys[name]).sort_index()
    if columns is not None:
        a=a[columns]
        b=b[columns]
    a=a.sort_index(axis=1)
    b=b.sort_index(axis=1)
    assert a.index.equals(b.index),(label,name,'keys')
    assert a.columns.equals(b.columns),(label,name,'columns')
    differences=0
    maximum=0.0
    for column in a.columns:
        x,y=a[column],b[column]
        if pd.api.types.is_float_dtype(x.dtype):
            same=np.isclose(x.to_numpy(dtype=float),y.to_numpy(dtype=float),atol=1e-9,rtol=1e-12,equal_nan=True)
            delta=(x-y).abs().max()
            if pd.notna(delta):maximum=max(maximum,float(delta))
        else:
            same=(x.eq(y)|(x.isna()&y.isna())).fillna(False).to_numpy()
        differences+=int((~same).sum())
    comparisons.append(dict(comparison=label,table=name,rows=len(a),differences=differences,max_float_difference=maximum))
    print(label,name,len(a),differences,flush=True)

inventory=[]
for year in [2022,2023,2024]:
    files=sorted((root/f'data/raw/yellow/{year}').glob('*.parquet'))
    assert {p.name for p in files}=={f'yellow_tripdata_{year}-{m:02d}.parquet' for m in range(1,13)}
    inventory.append(dict(year=year,files=len(files),bytes=sum(p.stat().st_size for p in files),
                          raw_rows=sum(pq.ParquetFile(p).metadata.num_rows for p in files)))
raw_counts={x['year']:x['raw_rows'] for x in inventory}
representatives={}
info_rows=[]
for period,(years,prefix) in cases.items():
    folder=root/'data/experiencias'/f'{prefix}_01'
    info=json.loads((folder/'run_info.json').read_text(encoding='utf-8'))
    frames={name:read(folder,name) for name in keys}
    representatives[period]=frames
    stats=frames['m1_cleaning_stats.csv']
    valid=info['clean_rows']
    check(period,'12 ficheiros por ano; fraction=1',info['files']==12*len(years) and info['fraction']==1.0)
    check(period,'anos pedidos em M1',set(stats.source_year)==set(years))
    check(period,'total bruto confere com metadados Parquet',all(int(row.total)==raw_counts[int(row.source_year)] for row in stats.itertuples()))
    check(period,'total menos invalidas igual a mantidas',stats.total.sub(stats.f_any).eq(stats.kept).all())
    check(period,'mantidas igual a clean.count',int(stats.kept.sum())==valid)
    flag_columns=[c for c in stats.columns if c.startswith('f_')]
    check(period,'flags entre zero e total',all(stats[c].ge(0).all() and stats[c].le(stats.total).all() for c in flag_columns))
    m2=frames['m2_demand']
    m3=frames['m3_zone_month']
    o1=frames['o1_anomaly_rates.csv']
    routes=frames['o2_route_month']
    reversal=frames['o2_q1_q3_reversal']
    top=frames['o2_top_routes.csv']
    check(period,'M2 soma das viagens',int(m2.trip_count.sum())==valid)
    check(period,'M3 grelha 263 zonas x meses',len(m3)==263*12*len(years) and set(m3.PULocationID)==set(range(1,264)) and set(m3.pickup_year)==set(years) and set(m3.pickup_month)==set(range(1,13)))
    check(period,'M3 soma das viagens',int(m3.trip_count.sum())==valid)
    check(period,'M3 contagens nao negativas',m3.trip_count.ge(0).all())
    check(period,'O1 soma das viagens',int(o1.trips.sum())==valid)
    check(period,'O1 anomalias dentro das contagens',o1.anomalies.ge(0).all() and o1.anomalies.le(o1.trips).all())
    check(period,'O1 taxas coerentes com contagens',np.isclose(o1.anomaly_rate_pct,100*o1.anomalies/o1.trips,atol=0.000500001,rtol=0).all())
    check(period,'O2 soma das viagens',int(routes.trip_count.sum())==valid)
    check(period,'O2 anos e meses',set(routes.pickup_year)==set(years) and set(routes.pickup_month)==set(range(1,13)))
    check(period,'O2 shares entre zero e um',routes.route_share.between(0,1).all())
    sums=routes.groupby(['PULocationID','pickup_year','pickup_month']).route_share.sum()
    check(period,'O2 shares somam um por origem e mes',np.isclose(sums,1,atol=1e-9,rtol=1e-12).all())
    check(period,'O2 inversoes apenas dos anos pedidos',set(reversal.pickup_year)==set(years))
    check(period,'O2 top ate 20 por borough',top.groupby('pickup_borough').size().le(20).all() and top.route_rank.between(1,20).all())
    check(period,'O2 anos com inversao dentro do periodo',top.years_with_reversal.between(0,len(years)).all())
    for number in [1,2,3]:
        run=root/'data/experiencias'/f'{prefix}_{number:02d}'
        run_info=json.loads((run/'run_info.json').read_text(encoding='utf-8'))
        assert run_info['years']==years and run_info['clean_rows']==valid
        info_rows.append(dict(period=period,run=run.name,years=years,valid_rows=valid,
                              raw_rows=sum(raw_counts[y] for y in years),
                              source_bytes=sum(x['bytes'] for x in inventory if x['year'] in years)))
        if number>1:
            for name in keys:compare(f'{period}: repeticao {number} vs 1',name,frames[name],read(run,name))

# Comparar apenas resultados cuja definição não muda com os outros anos.
for left,right in itertools.combinations(cases,2):
    common=set(cases[left][0])&set(cases[right][0])
    a,b=representatives[left],representatives[right]
    label=f'{left} vs {right}: anos comuns'
    for name,year_column in [('m1_cleaning_stats.csv','source_year'),('o2_route_month','pickup_year'),('o2_q1_q3_reversal','pickup_year')]:
        compare(label,name,a[name][a[name][year_column].isin(common)],b[name][b[name][year_column].isin(common)])
    name='m3_zone_month'
    ma=a[name][a[name].pickup_year.isin(common)]
    mb=b[name][b[name].pickup_year.isin(common)]
    window_columns=['share_3m','rolling_mean_3m','rolling_std_3m','volume_anomaly']
    base_columns=[c for c in ma.columns if c not in keys[name]+window_columns]
    compare(label+' (M3 sem janelas)',name,ma,mb,base_columns)
    first_left=min(cases[left][0])*12+1
    first_right=min(cases[right][0])*12+1
    start=max(first_left,first_right)+(3 if first_left!=first_right else 0)
    compare(label+' (M3 historico comparavel)',name,ma[ma.month_index>=start],mb[mb.month_index>=start])

assert len(comparisons)==57
report=dict(scope='invariantes por dimensao na primeira corrida; repeticoes e resultados anuais comparaveis',
            float_atol=1e-9,float_rtol=1e-12,inventory=inventory,runs=info_rows,
            invariants=invariants,comparisons=comparisons)
(output/'verificacao_dimensao.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
assert all(x['differences']==0 for x in comparisons),'Tabelas diferentes'
assert all(x['passed'] for x in invariants),'Invariantes falharam'
print('PASSOU:',len(invariants),'invariantes;',len(comparisons),'comparacoes',flush=True)
