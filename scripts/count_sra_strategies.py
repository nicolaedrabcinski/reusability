"""Global SRA run counts per LibraryStrategy (total and per year) via NCBI esearch.

Writes <data-dir>/sra_strategy_counts.csv with columns strategy, year, count
(year = 'all' for the total over all years, then 2010..2026).
NCBI_API_KEY is read from the environment (never printed); without it requests are paced at 3/s.
"""
import argparse
import csv
import os
import sys
import time

import requests

URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
API_KEY = os.environ.get('NCBI_API_KEY', '')
DELAY = 0.11 if API_KEY else 0.36

STRATEGIES = [
    'AMPLICON', 'RNA-Seq', 'WGS', 'WXS', 'ChIP-Seq', 'ATAC-seq', 'Bisulfite-Seq', 'miRNA-Seq', 'Hi-C',
    'WGA', 'WCS', 'RAD-Seq', 'Targeted-Capture', 'OTHER', 'CLONE', 'POOLCLONE', 'EST', 'FL-cDNA', 'CTS',
    'MRE-Seq', 'MeDIP-Seq', 'MNase-Seq', 'DNase-Hypersensitivity', 'FAIRE-seq', 'ncRNA-Seq', 'Tn-Seq',
    'SELEX', 'Synthetic-Long-Read', 'VALIDATION',
    # further values of the SRA library_strategy vocabulary
    'ssRNA-seq', 'CLONEEND', 'FINISHING', 'MBD-Seq', 'RIP-Seq', 'ChIA-PET',
    'Tethered Chromatin Conformation Capture', 'NOMe-Seq', 'ChM-Seq', 'GBS', 'Ribo-Seq',
    'Bisulfite-Seq', 'Hi-C', 'FAIRE-seq',
]
YEARS = list(range(2010, 2027))


def count(term):
    params = {'db': 'sra', 'term': term, 'retmode': 'json', 'retmax': 0}
    if API_KEY:
        params['api_key'] = API_KEY
    for attempt in range(6):
        time.sleep(DELAY)
        try:
            r = requests.get(URL, params=params, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.RequestException(f'HTTP {r.status_code}')
            r.raise_for_status()
            return int(r.json()['esearchresult']['count'])
        except (requests.RequestException, ValueError, KeyError):
            time.sleep(2 ** attempt)
    raise RuntimeError(f'esearch failed for {term!r}')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--data-dir', default='../data')
    ap.add_argument('--extra', nargs='*', default=[], help='additional LibraryStrategy values to count')
    args = ap.parse_args()
    os.makedirs(args.data_dir, exist_ok=True)
    out = os.path.join(args.data_dir, 'sra_strategy_counts.csv')

    strategies = list(dict.fromkeys(STRATEGIES + args.extra))
    rows, failed = [], []
    for s in strategies:
        base = f'"{s}"[Strategy] AND public[Access]'
        try:
            rows.append((s, 'all', count(base)))
            for y in YEARS:
                rows.append((s, y, count(f'{base} AND {y}[PDAT]')))
        except RuntimeError as e:
            failed.append(s)
            print(e, file=sys.stderr)
        print(f'{s}: {rows[-1][2] if rows else "-"} (last count), done {len(rows)} rows', flush=True)

    with open(out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['strategy', 'year', 'count'])
        w.writerows(rows)
    print(f'Wrote {len(rows)} rows to {out}')
    totals = sorted(((c, s) for s, y, c in rows if y == 'all'), reverse=True)
    print('Top 10 by total runs:', ', '.join(f'{s}={c:,}' for c, s in totals[:10]))
    if failed:
        sys.exit(f'failed strategies: {failed}')


if __name__ == '__main__':
    main()
