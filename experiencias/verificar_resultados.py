"""Comparar as sete saídas de três corridas, depois de medir a pipeline."""
import json
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--prefix', default='baseline_com_cache')
parser.add_argument('--reference', default='baseline_sem_cache_01')
parser.add_argument('--report', default='comparacao_cache_resultados.json')
args = parser.parse_args()

project = Path(__file__).resolve().parents[1]
root = project / 'data/experiencias'
tables = {
    'm1_cleaning_stats.csv': ['source_year'],
    'm2_demand': ['pickup_borough', 'day_of_week', 'hour_of_day'],
    'm3_zone_month': ['PULocationID', 'pickup_year', 'pickup_month'],
    'o1_anomaly_rates.csv': ['pickup_borough', 'payment_type', 'day_type'],
    'o2_route_month': ['PULocationID', 'DOLocationID', 'pickup_year', 'pickup_month'],
    'o2_q1_q3_reversal': ['PULocationID', 'DOLocationID', 'pickup_year'],
    'o2_top_routes.csv': ['PULocationID', 'DOLocationID'],
}

def read(folder, name, keys):
    path = folder / name
    if path.is_dir():
        assert (path / '_SUCCESS').exists(), path
        frame = ds.dataset(path, format='parquet', partitioning='hive').to_table().to_pandas()
    else:
        frame = pd.read_csv(path)
    assert not frame.duplicated(keys).any(), name
    return frame.set_index(keys).sort_index().sort_index(axis=1)

reference = root / args.reference
report = []
for name, keys in tables.items():
    a = read(reference, name, keys)
    for number in range(1, 4):
        run = f'{args.prefix}_{number:02d}'
        b = read(root / run, name, keys)
        assert a.index.equals(b.index), (run, name, 'keys')
        assert a.columns.equals(b.columns), (run, name, 'columns')
        differences = 0
        maximum = 0.0
        for column in a.columns:
            x, y = a[column], b[column]
            if pd.api.types.is_float_dtype(x.dtype):
                same = np.isclose(x.to_numpy(dtype=float), y.to_numpy(dtype=float), atol=1e-9, rtol=1e-12, equal_nan=True)
                delta = (x-y).abs().max()
                if pd.notna(delta):
                    maximum = max(maximum, float(delta))
            else:
                same = (x.eq(y) | (x.isna() & y.isna())).fillna(False).to_numpy()
            differences += int((~same).sum())
        report.append(dict(run=run, table=name, rows=len(a), differences=differences, max_float_difference=maximum))
        print(run, name, len(a), differences, maximum, flush=True)
out = project / 'experiencias/resultados' / args.report
out.write_text(json.dumps(report, indent=2), encoding='utf-8')
assert all(item['differences'] == 0 for item in report), 'Resultados diferentes'
