"""Build pubmed_mappings.tsv: the gold standard used for QC in notebooks/create.ipynb.

RNA-SEQ experiments from the refine.bio API (SRA / GEO accession + PubMed ID), with each
PubMed ID converted to a PMC ID through the NCBI ID Converter.
Same procedure and columns as generate_gold_standard.py in the open-science-analytics repo
(originally from the Penn group, Casey + Kurt).
"""
import argparse
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

REFINE_BIO_API = 'https://api.refine.bio/v1/experiments/'
PMC_CONVERTER_URL = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/'
PAGE_SIZE = 100
CONVERT_BATCH = 100  # PubMed IDs per ID Converter request
REQUEST_DELAY = 0.4  # seconds between ID Converter requests

session = requests.Session()
adapter = HTTPAdapter(max_retries=Retry(total=5, backoff_factor=2,
                                        status_forcelist=[429, 500, 502, 503, 504]))
session.mount('https://', adapter)
session.mount('http://', adapter)


def fetch_experiments():
    """Return one row per RNA-SEQ experiment: SRA code, GEO code, PubMed ID."""
    rows, url, pbar = [], f'{REFINE_BIO_API}?limit={PAGE_SIZE}', None
    while url:
        r = session.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        if pbar is None:
            pbar = tqdm(total=data.get('count'), desc='refine.bio experiments', unit='exp')
        for exp in data.get('results', []):
            pbar.update(1)
            if 'RNA-SEQ' not in (exp.get('technologies') or []):
                continue
            pmid = exp.get('pubmed_id')
            rows.append({'SRA_accession_code': exp.get('accession_code') or '',
                         'GEO_accession_code': exp.get('alternate_accession_code') or '',
                         'pubmed_id': str(pmid) if pmid else ''})
        url = data.get('next')
    if pbar:
        pbar.close()
    return pd.DataFrame(rows)


def pubmed_to_pmc(pmids):
    """Convert PubMed IDs to PMC IDs in batches. Returns {pmid: pmcid}."""
    out = {}
    pmids = sorted(set(pmids))
    for i in tqdm(range(0, len(pmids), CONVERT_BATCH), desc='PubMed -> PMC', unit='batch'):
        chunk = pmids[i:i + CONVERT_BATCH]
        time.sleep(REQUEST_DELAY)
        r = session.get(PMC_CONVERTER_URL, params={'ids': ','.join(chunk), 'format': 'json'}, timeout=60)
        r.raise_for_status()
        for rec in r.json().get('records', []):
            if rec.get('pmcid'):
                out[str(rec.get('requested-id') or rec.get('pmid'))] = rec['pmcid']
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--out', default='pubmed_mappings.tsv')
    args = ap.parse_args()

    df = fetch_experiments()
    if df.empty:
        sys.exit('refine.bio returned no RNA-SEQ experiments')
    mapping = pubmed_to_pmc([p for p in df['pubmed_id'] if p])
    df['pmcid'] = df['pubmed_id'].map(mapping).fillna('')
    df.to_csv(args.out, sep='\t', index=False)
    print(f"Wrote {len(df):,} mappings ({(df['pmcid'] != '').sum():,} with a PMC ID) to {args.out}")


if __name__ == '__main__':
    main()
