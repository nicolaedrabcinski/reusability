"""Check the date-based generator/reuser roles (analyze.ipynb) against GEO's own paper link.

GEO series records carry the PubMed ID(s) of the paper the submitters attached to the dataset.
That paper is, by GEO's definition, the one that introduced (generated) the data. For every GEO
(paper, accession) pair in the analysis matrix this script records:

  link_status  linked        paper PMID is among the series' PubMed IDs
               not_linked    series has PubMed IDs, paper has a PMID, and it is not among them
               no_gse_pmid   series has no PubMed ID in GEO
               no_paper_pmid paper has no PMID (NCBI ID Converter)

and compares it with reuse_role (G/R). Outputs:
  geo_link_pairs.csv.gz   one row per pair with link_status
  geo_link_summary.md     counts, shares and what each disagreement can mean
"""
import argparse
import re

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--matrix', required=True, help='metadata_matrix_filtered.csv')
ap.add_argument('--geo-series', required=True, help='geo_series.csv')
ap.add_argument('--pmc-pmid', required=True, help='pmc_pmid.csv from pmc_to_pmid.py')
args = ap.parse_args()

m = pd.read_csv(args.matrix, usecols=['pmc_ID', 'accession', 'converted_accession', 'repository',
                                      'reuse_role', 'class', 'pmc_date', 'repository_date'],
                low_memory=False)
m = m[m['repository'] == 'GEO'].drop_duplicates(['pmc_ID', 'converted_accession'])

g = pd.read_csv(args.geo_series, usecols=['Accession', 'PubMed ID'], low_memory=False)
gse_pmids = {acc: set(re.findall(r'\d+', str(p)))
             for acc, p in zip(g['Accession'], g['PubMed ID']) if pd.notna(p)}

pp = pd.read_csv(args.pmc_pmid, dtype=str).fillna('')
pmc2pmid = dict(zip(pp['pmc_ID'], pp['pmid']))


def status(pmc, gse):
    linked_set = gse_pmids.get(gse)
    if not linked_set:
        return 'no_gse_pmid'
    pmid = pmc2pmid.get(pmc, '')
    if not pmid:
        return 'no_paper_pmid'
    return 'linked' if pmid in linked_set else 'not_linked'


m['paper_pmid'] = m['pmc_ID'].map(pmc2pmid).fillna('')
m['gse_pmids'] = m['converted_accession'].map(lambda a: ';'.join(sorted(gse_pmids.get(a, ()))))
m['link_status'] = [status(p, a) for p, a in zip(m['pmc_ID'], m['converted_accession'])]
m.to_csv('geo_link_pairs.csv.gz', index=False)

ct = pd.crosstab(m['reuse_role'], m['link_status'], margins=True)
known = m[m['link_status'].isin(['linked', 'not_linked'])]
g_known = known[known['reuse_role'] == 'G']
r_known = known[known['reuse_role'] == 'R']

# per series: is the GEO-linked paper in the corpus, and is it the one the heuristic called G?
linked_in_corpus = known[known['link_status'] == 'linked']['converted_accession'].unique()
per_gse = known[known['converted_accession'].isin(linked_in_corpus)]
gse_g_is_linked = (per_gse[per_gse['reuse_role'] == 'G'].groupby('converted_accession')['link_status']
                   .apply(lambda s: (s == 'linked').any()))


def pct(a, b):
    return f'{a:,} / {b:,} ({100 * a / b:.1f}%)' if b else f'{a} / 0'


lines = [
    '# GEO link check of generator/reuser roles', '',
    'Reference: PubMed ID(s) recorded in the GEO series = the paper that introduced the data.',
    'Pairs: GEO rows of metadata_matrix_filtered.csv, unique (pmc_ID, series).', '',
    '## Pairs by role and link status', '', '```', ct.to_string(), '```', '',
    '## Agreement where both PMIDs are known', '',
    f'- G pairs that are the GEO-linked paper: {pct((g_known.link_status == "linked").sum(), len(g_known))}',
    f'- R pairs that are NOT the GEO-linked paper: {pct((r_known.link_status == "not_linked").sum(), len(r_known))}',
    f'- R pairs that ARE the GEO-linked paper (heuristic called the generator a reuser): '
    f'{pct((r_known.link_status == "linked").sum(), len(r_known))}',
    f'- Series whose GEO-linked paper is in the corpus: {len(linked_in_corpus):,}; '
    f'of those with a G pair, the G paper is the linked one: {pct(int(gse_g_is_linked.sum()), len(gse_g_is_linked))}',
    '', '## How to read disagreements', '',
    '- G + not_linked: the earliest paper is not the one GEO lists. Either it reused the data early, or it is a '
    'companion/earlier paper of the same group, or GEO lists only a later paper. Needs the text.',
    '- R + linked: the paper GEO lists was published after another paper that already cited the series '
    '(or outside the ±270-day window). The heuristic marks the real generator as a reuser.',
    '- no_gse_pmid: GEO has no paper for the series; only the text can decide.',
]
open('geo_link_summary.md', 'w').write('\n'.join(lines) + '\n')
print('\n'.join(lines))
