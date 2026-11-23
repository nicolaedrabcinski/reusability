"""Choose the (paper, accession) pairs for the text check and split them into chunks.

Every pair gets a stratum:
  geo_G_linked       heuristic G and GEO lists this paper           -> reference: generated
  geo_R_notlinked    heuristic R, GEO lists another paper           -> reference: reused or mentioned
  geo_G_notlinked    heuristic G, GEO lists another paper           -> disagreement
  geo_R_linked       heuristic R, but GEO lists this paper          -> disagreement
  geo_no_gse_pmid    GEO has no paper for the series                -> only text can decide
  geo_no_paper_pmid  the paper has no PMID                          -> only text can decide
  sra_G, sra_R       SRA accessions (no paper link available)       -> only text can decide

--scope sample  --n pairs per stratum (fewer if the population is smaller), one chunk.
--scope all     every pair; papers ordered by the most informative stratum among their pairs
                (disagreements first, then no link, then SRA, then agreements), and cut into chunks
                of about --chunk-size pairs that never split a paper.
Outputs: pairs.csv (all chosen pairs) and chunk_0001.csv, chunk_0002.csv, ...
"""
import argparse

import pandas as pd

PRIORITY = {'geo_R_linked': 1, 'geo_G_notlinked': 1, 'geo_no_gse_pmid': 2, 'geo_no_paper_pmid': 2,
            'sra_G': 3, 'sra_R': 3, 'geo_G_linked': 4, 'geo_R_notlinked': 4}
COLS = ['pmc_ID', 'accession', 'converted_accession', 'repository', 'class', 'reuse_role',
        'link_status', 'paper_pmid', 'gse_pmids', 'stratum']

ap = argparse.ArgumentParser()
ap.add_argument('--geo-pairs', required=True, help='geo_link_pairs.csv.gz')
ap.add_argument('--matrix', required=True, help='metadata_matrix_filtered.csv (for SRA pairs)')
ap.add_argument('--scope', choices=['sample', 'all'], default='sample')
ap.add_argument('--n', type=int, default=40)
ap.add_argument('--chunk-size', type=int, default=20000)
ap.add_argument('--seed', type=int, default=20261122)
args = ap.parse_args()

geo = pd.read_csv(args.geo_pairs, dtype=str)
status_to_stratum = {('G', 'linked'): 'geo_G_linked', ('R', 'not_linked'): 'geo_R_notlinked',
                     ('G', 'not_linked'): 'geo_G_notlinked', ('R', 'linked'): 'geo_R_linked'}
geo['stratum'] = [status_to_stratum.get((r, s), 'geo_no_gse_pmid' if s == 'no_gse_pmid' else 'geo_no_paper_pmid')
                  for r, s in zip(geo.reuse_role, geo.link_status)]
m = pd.read_csv(args.matrix, usecols=['pmc_ID', 'accession', 'converted_accession', 'repository',
                                      'reuse_role', 'class'], dtype=str, low_memory=False)
sra = m[m.repository == 'SRA'].drop_duplicates(['pmc_ID', 'accession']).copy()
sra['stratum'] = 'sra_' + sra.reuse_role
allp = pd.concat([geo.reindex(columns=COLS), sra.reindex(columns=COLS)], ignore_index=True)
allp = allp[allp.pmc_ID.str.startswith('PMC', na=False)]

if args.scope == 'sample':
    parts = []
    for name in PRIORITY:
        df = allp[allp.stratum == name]
        if name == 'geo_no_paper_pmid':
            continue   # a few hundred pairs; not part of the sample design
        parts.append(df.sample(min(args.n, len(df)), random_state=args.seed))
        print(f'{name}: population {len(df):,}, sampled {len(parts[-1])}')
    chosen = pd.concat(parts)
    chosen = chosen.sort_values('pmc_ID', kind='stable')
    chunks = [chosen]
else:
    allp['priority'] = allp.stratum.map(PRIORITY)
    paper_pri = allp.groupby('pmc_ID').priority.min().rename('paper_priority')
    chosen = allp.join(paper_pri, on='pmc_ID').sort_values(['paper_priority', 'pmc_ID', 'accession'])
    chosen = chosen.drop(columns=['priority', 'paper_priority'])
    sizes = chosen.groupby('pmc_ID', sort=False).size()
    chunk_of, k, n = {}, 1, 0
    for pmc, sz in sizes.items():
        if n and n + sz > args.chunk_size:
            k, n = k + 1, 0
        chunk_of[pmc] = k
        n += sz
    chosen['chunk'] = chosen.pmc_ID.map(chunk_of)
    chunks = [g.drop(columns='chunk') for _, g in chosen.groupby('chunk', sort=True)]
    chosen = chosen.drop(columns='chunk')
    print(chosen.stratum.value_counts().to_string())
    print(f'pairs: {len(chosen):,}, papers: {chosen.pmc_ID.nunique():,}, chunks: {len(chunks)}')

chosen.to_csv('pairs.csv', index=False)
for i, c in enumerate(chunks, 1):
    c.to_csv(f'chunk_{i:04d}.csv', index=False)
