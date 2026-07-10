import os
import glob
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

DATA_DIR = '../data'

input_folder = f'{DATA_DIR}/pre_filter_matrices/'
output_file = f'{DATA_DIR}/pre_filter_matrix.csv'

csv_files = glob.glob(os.path.join(input_folder, '*.csv'))

if not csv_files:
    print("No CSV files found in input folder.")
else:
    workers = min(32, len(csv_files))
    print(f"Reading {len(csv_files)} CSV files with {workers} threads...")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        dfs = list(ex.map(pd.read_csv, csv_files))

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(output_file, index=False)
    print(f"Saved {len(combined_df):,} rows to {output_file}")
