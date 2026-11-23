"""Independent signals of "this paper generated this dataset" for every (paper, accession) pair.

  s_heuristic  date-based role of analyze.ipynb: G -> 1, R -> 0
  s_repo_link  the repository's own paper link: GEO series PubMed IDs; for SRA, Study_Pubmed_id of the
               runs and PubMed records linked to the BioProject. 1 = this paper, 0 = only other papers,
               empty = no link or no PMID for the paper
  s_authors    GEO only: someone named in the series (contributors + contact) is an author of the paper
               in PubMed (same surname and first initial). 1 / 0, empty when either list is missing
  s_refine     refine.bio table (pubmed_mappings.tsv): 1 = it pairs the dataset with this paper,
               0 = with another paper. It is drawn from the same GEO/SRA metadata as s_repo_link, so it
               is reported as a cross-check and is not an independent signal.
Output: pair_signals.csv.gz
"""
import argparse
import re
import unicodedata

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--pairs', required=True)
ap.add_argument('--geo-pairs', required=True)
ap.add_argument('--pmc-pmid', required=True, nargs='+')
ap.add_argument('--authors', required=True)
ap.add_argument('--geo-names', required=True)
ap.add_argument('--sra-links', required=True)
ap.add_argument('--refine', required=True)
args = ap.parse_args()


def norm(s):
    s = unicodedata.normalize('NFKD', str(s))
    return re.sub(r'[^a-z]', '', ''.join(c for c in s if not unicodedata.combining(c)).lower())


def key_geo(name):          # "first|middle|last"
    parts = name.split('|')
    if len(parts) < 3 or not norm(parts[2]):
        return None
    return norm(parts[2]), norm(parts[0])[:1]


def key_pubmed(name):       # "Lima JA", "van der Berg J"
    toks = name.split()
    if len(toks) < 2:
        return None
    return norm(' '.join(toks[:-1])), norm(toks[-1])[:1]


pairs = pd.read_csv(args.pairs, dtype=str)
pmc2pmid = {}
for f in args.pmc_pmid:
    t = pd.read_csv(f, dtype=str).fillna('')
    pmc2pmid.update({a: b for a, b in zip(t.pmc_ID, t.pmid) if b})
pairs['paper_pmid'] = pairs.pmc_ID.map(pmc2pmid)

pairs['s_heuristic'] = pairs.reuse_role.map({'G': 1, 'R': 0})

geo = pd.read_csv(args.geo_pairs, dtype=str, usecols=['pmc_ID', 'converted_accession', 'link_status'])
link = {(a, b): {'linked': 1, 'not_linked': 0}.get(s) for a, b, s in zip(geo.pmc_ID, geo.converted_accession, geo.link_status)}
sra = pd.read_csv(args.sra_links, dtype=str).fillna('')
study_pm = {st: set(filter(None, (a + ';' + b).split(';'))) for st, a, b in zip(sra.study, sra.run_pmids, sra.bioproject_pmids)}


def repo_link(r):
    if r.repository == 'GEO':
        return link.get((r.pmc_ID, r.converted_accession))
    pm = study_pm.get(r.converted_accession)
    if not pm or not isinstance(r.paper_pmid, str):
        return None
    return int(r.paper_pmid in pm)


pairs['s_repo_link'] = [repo_link(r) for r in pairs.itertuples()]

au = pd.read_csv(args.authors, dtype=str).fillna('')
paper_keys = {p: {k for k in map(key_pubmed, a.split(';')) if k} for p, a in zip(au.pmid, au.authors) if a}
gn = pd.read_csv(args.geo_names, dtype=str).fillna('')
gse_keys = {g: {k for k in map(key_geo, n.split(';')) if k} for g, n in zip(gn.gse, gn.names) if n}


def authors(r):
    if r.repository != 'GEO':
        return None
    a, b = paper_keys.get(r.paper_pmid), gse_keys.get(r.converted_accession)
    if not a or not b:
        return None
    return int(bool(a & b))


pairs['s_authors'] = [authors(r) for r in pairs.itertuples()]

rf = pd.read_csv(args.refine, sep='\t', dtype=str)
rf.columns = ['sra', 'geo', 'pmid', 'pmc']
ref_map = {}
for s, g, pmc in zip(rf.sra, rf.geo, rf.pmc):
    for d in (s, g):
        if isinstance(d, str) and d and isinstance(pmc, str) and pmc:
            ref_map.setdefault(d, set()).add(pmc)


def refine(r):
    for d in (r.converted_accession, r.accession):
        if d in ref_map:
            return int(r.pmc_ID in ref_map[d])
    return None


pairs['s_refine'] = [refine(r) for r in pairs.itertuples()]
for c in ('s_heuristic', 's_repo_link', 's_authors', 's_refine'):
    pairs[c] = pd.array(pairs[c], dtype='Int64')
pairs.to_csv('pair_signals.csv.gz', index=False)
cov = {c: f'{pairs[c].notna().mean():.1%}' for c in ('s_heuristic', 's_repo_link', 's_authors', 's_refine')}
print('coverage:', cov)
print(pairs.groupby('repository')[['s_repo_link', 's_authors', 's_refine']].agg(lambda x: x.notna().mean()).round(3))
