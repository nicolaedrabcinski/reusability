"""Pick (paper, accession) pairs for the text check, stratified so that every kind of case is present.

Strata (n per stratum from --n, fewer if the population is smaller):
  geo_G_linked       heuristic G and GEO lists this paper           -> reference: generated
  geo_R_notlinked    heuristic R, GEO lists another paper           -> reference: reused or mentioned
  geo_G_notlinked    heuristic G, GEO lists another paper           -> disagreement, needs text
  geo_R_linked       heuristic R, but GEO lists this paper          -> disagreement, needs text
  geo_no_gse_pmid    GEO has no paper for the series                -> only text can decide
  sra_G, sra_R       SRA accessions (no paper link available)       -> only text can decide
"""
import argparse

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--geo-pairs', required=True, help='geo_link_pairs.csv.gz')
ap.add_argument('--matrix', required=True, help='metadata_matrix_filtered.csv (for SRA pairs)')
ap.add_argument('--n', type=int, default=40)
ap.add_argument('--seed', type=int, default=20261122)
ap.add_argument('--out', default='pairs.csv')
args = ap.parse_args()

geo = pd.read_csv(args.geo_pairs, dtype=str)
strata = {
    'geo_G_linked': geo[(geo.reuse_role == 'G') & (geo.link_status == 'linked')],
    'geo_R_notlinked': geo[(geo.reuse_role == 'R') & (geo.link_status == 'not_linked')],
    'geo_G_notlinked': geo[(geo.reuse_role == 'G') & (geo.link_status == 'not_linked')],
    'geo_R_linked': geo[(geo.reuse_role == 'R') & (geo.link_status == 'linked')],
    'geo_no_gse_pmid': geo[geo.link_status == 'no_gse_pmid'],
}
m = pd.read_csv(args.matrix, usecols=['pmc_ID', 'accession', 'converted_accession', 'repository',
                                      'reuse_role', 'class'], dtype=str, low_memory=False)
sra = m[m.repository == 'SRA'].drop_duplicates(['pmc_ID', 'accession'])
strata['sra_G'] = sra[sra.reuse_role == 'G']
strata['sra_R'] = sra[sra.reuse_role == 'R']

cols = ['pmc_ID', 'accession', 'converted_accession', 'repository', 'class', 'reuse_role']
parts = []
for name, df in strata.items():
    df = df[df.pmc_ID.str.startswith('PMC', na=False)]
    take = df.sample(min(args.n, len(df)), random_state=args.seed)
    take = take.reindex(columns=cols + ['link_status', 'paper_pmid', 'gse_pmids'])
    take['stratum'] = name
    parts.append(take)
    print(f'{name}: population {len(df):,}, sampled {len(take)}')
pd.concat(parts).to_csv(args.out, index=False)
