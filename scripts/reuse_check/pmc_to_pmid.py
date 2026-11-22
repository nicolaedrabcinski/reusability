"""Map PMC IDs to PubMed IDs with the NCBI PMC ID Converter.

Input: a CSV with a pmc_ID column (e.g. metadata_matrix_filtered.csv); --repository keeps only
rows of that repository. Output: pmc_pmid.csv (pmc_ID, pmid); pmid is empty when NCBI has none.
Exits non-zero if any batch still fails after retries, so Nextflow can retry the task.
"""
import argparse
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/'
BATCH = 200          # IDs per request (converter limit)
DELAY = 0.35         # seconds between requests (< 3 requests/s)

ap = argparse.ArgumentParser()
ap.add_argument('--matrix', required=True)
ap.add_argument('--repository', default=None)
ap.add_argument('--out', default='pmc_pmid.csv')
ap.add_argument('--email', default='drabcinski@gmail.com')
args = ap.parse_args()

cols = ['pmc_ID', 'repository'] if args.repository else ['pmc_ID']
df = pd.read_csv(args.matrix, usecols=cols, low_memory=False)
if args.repository:
    df = df[df['repository'] == args.repository]
pmcs = sorted(p for p in df['pmc_ID'].dropna().unique() if str(p).startswith('PMC'))
print(f'{len(pmcs)} PMC IDs to convert', file=sys.stderr)

s = requests.Session()
s.mount('https://', HTTPAdapter(max_retries=Retry(total=6, backoff_factor=2,
                                                  status_forcelist=[429, 500, 502, 503, 504])))
rows, failed = [], 0
for i in range(0, len(pmcs), BATCH):
    chunk = pmcs[i:i + BATCH]
    try:
        r = s.get(URL, params={'ids': ','.join(chunk), 'format': 'json', 'idtype': 'pmcid',
                               'tool': 'reusability', 'email': args.email}, timeout=60)
        r.raise_for_status()
        got = {rec.get('pmcid'): rec.get('pmid', '') for rec in r.json().get('records', [])}
    except Exception as e:  # keep going, report at the end
        print(f'batch {i // BATCH} failed: {e}', file=sys.stderr)
        failed += 1
        continue
    rows += [(p, got.get(p, '')) for p in chunk]
    if (i // BATCH) % 100 == 0:
        print(f'{i + len(chunk)}/{len(pmcs)}', file=sys.stderr)
    time.sleep(DELAY)

pd.DataFrame(rows, columns=['pmc_ID', 'pmid']).to_csv(args.out, index=False)
print(f'written {len(rows)} rows, {sum(1 for _, p in rows if p)} with PMID, failed batches: {failed}',
      file=sys.stderr)
sys.exit(1 if failed else 0)
