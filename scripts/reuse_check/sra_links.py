"""Paper links of SRA studies: Study_Pubmed_id from the SRA run table, and the PubMed records linked
to the study's BioProject (E-utilities elink bioproject -> pubmed).

Input: --runs sra_complete_runs.csv, --pairs (SRA rows; converted_accession = SRP/ERP/DRP study).
Output: sra_study_pubmed.csv (study, run_pmids, bioproject_pmids), PMIDs joined by ";".
"""
import argparse
import json
import os
import sys
import time
from collections import defaultdict

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi'
BATCH = 50   # elink computes one linkset per id; 200 per request made NCBI drop the connection
KEY = os.environ.get('NCBI_API_KEY', '')

ap = argparse.ArgumentParser()
ap.add_argument('--runs', required=True)
ap.add_argument('--pairs', required=True)
ap.add_argument('--out', default='sra_study_pubmed.csv')
ap.add_argument('--cache', required=True, help='JSON lines of elink answers, read first (resumable)')
args = ap.parse_args()

p = pd.read_csv(args.pairs, usecols=['repository', 'converted_accession'], dtype=str)
studies = set(p.loc[p.repository == 'SRA', 'converted_accession'].dropna())
run_pm, proj = defaultdict(set), defaultdict(set)
for ch in pd.read_csv(args.runs, usecols=['SRAStudy', 'Study_Pubmed_id', 'ProjectID'], dtype=str, chunksize=500_000):
    ch = ch[ch.SRAStudy.isin(studies)]
    for st, pm, pr in ch.drop_duplicates().itertuples(index=False):
        if isinstance(pm, str) and pm.strip().isdigit() and pm.strip() != '0':
            run_pm[st].add(pm.strip())
        if isinstance(pr, str) and pr.strip().isdigit():
            proj[st].add(pr.strip())
uids = sorted({u for v in proj.values() for u in v})
print(f'studies: {len(studies)}, with Study_Pubmed_id: {len(run_pm)}, BioProject UIDs: {len(uids)}', file=sys.stderr)

s = requests.Session()
s.mount('https://', HTTPAdapter(max_retries=Retry(total=8, backoff_factor=2, allowed_methods=None,
                                                  status_forcelist=[429, 500, 502, 503, 504])))
bp_pm, failed, cached = defaultdict(set), 0, set()
if os.path.exists(args.cache):
    for line in open(args.cache):
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        cached.add(d['uid'])
        bp_pm[d['uid']].update(d['pmids'])
todo_uids = [u for u in uids if u not in cached]
print(f'BioProject UIDs cached: {len(cached)}, to link: {len(todo_uids)}', file=sys.stderr)
fc = open(args.cache, 'a')
for i in range(0, len(todo_uids), BATCH):
    chunk = todo_uids[i:i + BATCH]
    data = [('dbfrom', 'bioproject'), ('db', 'pubmed'), ('retmode', 'json'), ('tool', 'reusability'),
            ('email', 'drabcinski@gmail.com')] + [('id', u) for u in chunk]   # one linkset per id
    if KEY:
        data.append(('api_key', KEY))
    try:
        r = s.post(URL, data=data, timeout=180)
        r.raise_for_status()
        d = r.json()
        if 'linksets' not in d:
            raise ValueError(str(d)[:200])
    except Exception as e:
        print(f'batch {i // BATCH} failed: {e}', file=sys.stderr)
        failed += 1
        continue
    got = {}
    for ls in d['linksets']:
        links = [str(x) for db in ls.get('linksetdbs', []) if db.get('linkname') == 'bioproject_pubmed' for x in db.get('links', [])]
        for u in ls.get('ids', []):
            got.setdefault(str(u), set()).update(links)
    for u in chunk:   # ids without links are cached too
        bp_pm[u].update(got.get(u, ()))
        fc.write(json.dumps({'uid': u, 'pmids': sorted(got.get(u, ()))}) + '\n')
    fc.flush()
    time.sleep(0.12 if KEY else 0.4)
fc.close()

rows = [(st, ';'.join(sorted(run_pm.get(st, ()))), ';'.join(sorted({x for u in proj.get(st, ()) for x in bp_pm.get(u, ())})))
        for st in sorted(studies)]
pd.DataFrame(rows, columns=['study', 'run_pmids', 'bioproject_pmids']).to_csv(args.out, index=False)
print(f'written {len(rows)}, failed batches: {failed}', file=sys.stderr)
sys.exit(1 if failed else 0)
