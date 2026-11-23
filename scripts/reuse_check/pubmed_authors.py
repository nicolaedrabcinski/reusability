"""Author names of papers from PubMed (E-utilities esummary), for the author-overlap signal.

Input: one or more CSV files with a pmid column. Output: pubmed_authors.csv.gz (pmid, authors) with
authors as PubMed writes them ("Lima JA") joined by ";". Exits non-zero if a batch still fails.
"""
import argparse
import os
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
BATCH = 200
KEY = os.environ.get('NCBI_API_KEY', '')

ap = argparse.ArgumentParser()
ap.add_argument('--pmids', required=True, nargs='+')
ap.add_argument('--out', default='pubmed_authors.csv.gz')
args = ap.parse_args()

pmids = sorted({p for f in args.pmids for p in pd.read_csv(f, dtype=str)['pmid'].dropna() if p.isdigit()})
print(f'{len(pmids)} PMIDs', file=sys.stderr)
s = requests.Session()
s.mount('https://', HTTPAdapter(max_retries=Retry(total=8, backoff_factor=2, allowed_methods=None,
                                                  status_forcelist=[429, 500, 502, 503, 504])))
rows, failed = [], 0
for i in range(0, len(pmids), BATCH):
    chunk = pmids[i:i + BATCH]
    data = {'db': 'pubmed', 'id': ','.join(chunk), 'retmode': 'json', 'tool': 'reusability',
            'email': 'drabcinski@gmail.com'}
    if KEY:
        data['api_key'] = KEY
    try:
        r = s.post(URL, data=data, timeout=120)
        r.raise_for_status()
        res = r.json()['result']
    except Exception as e:
        print(f'batch {i // BATCH} failed: {e}', file=sys.stderr)
        failed += 1
        continue
    for u in res.get('uids', []):
        rows.append((u, ';'.join(a['name'] for a in res[u].get('authors', []) if a.get('authtype') == 'Author')))
    time.sleep(0.12 if KEY else 0.4)
pd.DataFrame(rows, columns=['pmid', 'authors']).to_csv(args.out, index=False)
print(f'written {len(rows)}, failed batches: {failed}', file=sys.stderr)
sys.exit(1 if failed else 0)
