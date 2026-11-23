"""Names of the people behind each GEO series: !Series_contributor and !Series_contact_name lines of
the series header (GEO accession viewer, brief text view).

Input: --pairs (pairs.csv of the text check; GEO rows, column converted_accession = GSE...).
The answers are appended to --cache (JSON lines), which is read first, so an interrupted run goes on
where it stopped. Output: geo_contributors.csv.gz (gse, names) with names as "first|middle|last"
joined by ";", empty when the series has none.
"""
import argparse
import json
import os
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi'
PAUSE = 0.3

ap = argparse.ArgumentParser()
ap.add_argument('--pairs', required=True)
ap.add_argument('--cache', required=True)
ap.add_argument('--out', default='geo_contributors.csv.gz')
args = ap.parse_args()

p = pd.read_csv(args.pairs, usecols=['repository', 'converted_accession'], dtype=str)
series = sorted(p.loc[p.repository == 'GEO', 'converted_accession'].dropna().unique())
done = {}
if os.path.exists(args.cache):
    for line in open(args.cache):
        try:
            d = json.loads(line)
            done[d['gse']] = d['names']
        except (json.JSONDecodeError, KeyError):
            continue   # a line cut by an interruption
todo = [g for g in series if g not in done]
print(f'series: {len(series)}, cached: {len(series) - len(todo)}, to fetch: {len(todo)}', file=sys.stderr)

s = requests.Session()
s.mount('https://', HTTPAdapter(max_retries=Retry(total=6, backoff_factor=2,
                                                  status_forcelist=[429, 500, 502, 503, 504])))
failed = 0
with open(args.cache, 'a') as fc:
    for i, g in enumerate(todo, 1):
        try:
            r = s.get(URL, params={'acc': g, 'targ': 'self', 'form': 'text', 'view': 'brief'}, timeout=60)
            r.raise_for_status()
        except requests.RequestException:
            failed += 1
            continue
        names = []
        for line in r.text.splitlines():
            if line.startswith(('!Series_contributor', '!Series_contact_name')):
                n = line.split('=', 1)[1].strip()
                if n not in names:
                    names.append(n)
        fc.write(json.dumps({'gse': g, 'names': names}) + '\n')
        done[g] = names
        if i % 200 == 0:
            fc.flush()
        if i % 5000 == 0:
            print(f'{i}/{len(todo)}', file=sys.stderr)
        time.sleep(PAUSE)

pd.DataFrame([(g, ';'.join(n.replace(',', '|') for n in done.get(g, []))) for g in series if g in done],
             columns=['gse', 'names']).to_csv(args.out, index=False)
print(f'written {sum(g in done for g in series)} series, failed now: {failed}', file=sys.stderr)
sys.exit(1 if failed else 0)
