import os
import time
import pandas as pd
import requests
from io import StringIO
from tqdm import tqdm

DATA_DIR = '../data'

local_dir_temp = {
    'samples':   f'{DATA_DIR}/geo_samples/',
    'series':    f'{DATA_DIR}/geo_series/',
    'platforms': f'{DATA_DIR}/geo_platforms/',
}

output_file = {
    'samples':   f'{DATA_DIR}/geo_samples.csv',
    'series':    f'{DATA_DIR}/geo_series.csv',
    'platforms': f'{DATA_DIR}/geo_platforms.csv',
}

URL_GEO = {
    'samples':   'https://www.ncbi.nlm.nih.gov/geo/browse/?view=samples&sort=date&mode=csv&page={}&display=5000',
    'series':    'https://www.ncbi.nlm.nih.gov/geo/browse/?view=series&sort=date&mode=csv&page={}&display=5000',
    'platforms': 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=platforms&sort=date&mode=csv&page={}&display=5000',
}

desired_headers = {
    'samples':   "Accession,Title,Sample Type,Taxonomy,Channels,Platform,Series,Supplementary Types,Supplementary Links,SRA Accession,Contact,Release Date",
    'series':    "Accession,Title,Series Type,Taxonomy,Sample Count,Datasets,Supplementary Types,Supplementary Links,PubMed ID,SRA Accession,Contact,Release Date",
    'platforms': "Accession,Title,Technology,Taxonomy,Data Rows,Samples Count,Series Count,Contact,Release Date",
}

MAX_RETRIES = 5
TIMEOUT = 60


def fetch_with_retry(url):
    """GET with exponential backoff. Returns response or raises."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT)
            return response
        except Exception as e:
            wait = 2 ** attempt
            print(f"  Connection error (attempt {attempt}/{MAX_RETRIES}): {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} retries")


def download_and_process_data(data_type):
    local_dir = local_dir_temp[data_type]
    os.makedirs(local_dir, exist_ok=True)

    # Найти уже скачанные страницы (кеш)
    cached = {int(f.split('_')[-1].replace('.csv', ''))
              for f in os.listdir(local_dir)
              if f.startswith(data_type) and f.endswith('.csv')}
    start_page = max(cached) + 1 if cached else 1
    if cached:
        print(f"[{data_type}] Resuming from page {start_page} ({len(cached)} pages cached)")

    page_number = start_page
    with tqdm(desc=f"{data_type}", unit="page", initial=len(cached)) as pbar:
        while True:
            # Пропустить уже скачанные страницы
            if page_number in cached:
                page_number += 1
                continue

            url = URL_GEO[data_type].format(page_number)
            response = fetch_with_retry(url)

            if response.status_code != 200:
                print(f"[{data_type}] HTTP {response.status_code} on page {page_number}, stopping.")
                break

            if response.text.strip() == desired_headers[data_type]:
                print(f"[{data_type}] No more data after page {page_number - 1}.")
                break

            temp_df = pd.read_csv(StringIO(response.text))
            temp_csvfile_path = os.path.join(local_dir, f'{data_type}_{page_number}.csv')
            temp_df.to_csv(temp_csvfile_path, index=False)

            page_number += 1
            pbar.update(1)

    csv_files = sorted(f for f in os.listdir(local_dir) if f.endswith('.csv'))
    if not csv_files:
        print(f"[{data_type}] No CSV files found.")
        return

    print(f"[{data_type}] Combining {len(csv_files)} pages...")
    combined_df = pd.concat(
        [pd.read_csv(os.path.join(local_dir, f)) for f in csv_files],
        ignore_index=True
    )
    combined_df.to_csv(output_file[data_type], index=False)
    print(f"[{data_type}] Saved {len(combined_df):,} rows → {output_file[data_type]}")


from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=3) as ex:
    list(ex.map(download_and_process_data, ['samples', 'series', 'platforms']))
