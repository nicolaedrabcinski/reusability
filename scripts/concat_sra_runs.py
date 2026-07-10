import os
import glob
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

DATA_DIR = '../data'

input_folder = f'{DATA_DIR}/sra_complete_runs/'
output_file = f'{DATA_DIR}/sra_complete_runs.csv'

all_files = [f for f in glob.glob(os.path.join(input_folder, '*.csv'))
             if os.path.getsize(f) > 0]

if not all_files:
    print('No data to merge.')
else:
    workers = min(32, len(all_files))
    print(f"Reading {len(all_files):,} files with {workers} threads...")

    def safe_read(path):
        try:
            return pd.read_csv(path, engine='c', low_memory=False, on_bad_lines='skip')
        except Exception:
            print(f"Error reading file: {path}")
            return None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(safe_read, all_files))

    dfs = [df for df in results if df is not None]
    merged_data = pd.concat(dfs, ignore_index=True)
    merged_data.to_csv(output_file, index=False)
    print(f'Merged {len(merged_data):,} rows saved to {output_file}')
