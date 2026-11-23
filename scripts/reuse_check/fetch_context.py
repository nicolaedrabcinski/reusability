"""Fetch the full text of every paper in a pairs file and build one excerpt per paper for the model.

Full text: NCBI efetch (db=pmc) first — the corpus comes from the PMC open-access subset and NCBI
answers in under a second — then Europe PMC fullTextXML as a fallback (it answers HTTP 500 or hangs
for many articles). NCBI_API_KEY from the environment raises the NCBI rate limit (3 -> 10 req/s).
Raw XML is cached gzipped in --cache-dir/<last 3 digits>/, so reruns do not hit the APIs again
(flat, uncompressed files of earlier runs are still read).

Excerpt per paper (at most --max-chars), shared by all of the paper's pairs so the LLM can reuse it:
  1. for each accession of the paper, the first paragraph that mentions it (section path in brackets);
     if no paragraph does, a window of text around its first mention anywhere in the article;
  2. the other paragraphs that mention one of the accessions;
  3. Methods / Data availability paragraphs, those with data-source words first.
Per pair, signals from the paragraphs that mention the accession (rule-based, no model):
  raw_terms        words of raw-read processing (FASTQ, SRR/ERR runs, prefetch, aligners, quantifiers)
  processed_terms  words of processed GEO data (series matrix, GEO2R, GEOquery, normalized/count matrix)
Output: contexts.jsonl, one JSON object per paper:
  {pmc_ID, text_source, context, pairs: [{accession, n_mentions, raw_terms, processed_terms}]}
  (text_source = ncbi | europepmc | none)
"""
import argparse
import gzip
import json
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EPMC = 'https://www.ebi.ac.uk/europepmc/webservices/rest/{pmc}/fullTextXML'
NCBI = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
METHODS_RE = re.compile(r'method|material|procedure|availab|accession|deposit|data access|resource|'
                        r'data and code|supplementa|ethics|samples', re.I)
SOURCE_RE = re.compile(r'\b(GEO|SRA|ENA|ArrayExpress|download|deposit|obtain|retriev|public|accession|'
                       r'previously (published|described|reported)|re-?analy|re-?process|FASTQ|raw)', re.I)
RAW_RE = re.compile(r'\bfastq\b|fasterq-dump|fastq-dump|\bprefetch\b|sra[- ]?toolkit|raw (sequencing )?reads|'
                    r'\b[SED]RR\d{5,}\b|re-?align|aligned (to|with|using)|mapped (to|with|using)|\bSTAR\b(?![ -]?Methods)|'
                    r'HISAT2?|Bowtie2?|\bBWA\b|kallisto|\bsalmon\b|featureCounts|HTSeq|Cell ?Ranger|'
                    r'Trimmomatic|cutadapt|fastp\b', re.I)
PROC_RE = re.compile(r'series[_ ]matrix|GEO2R|GEOquery|getGEO|normali[sz]ed (data|expression|matrix|counts?)|'
                     r'(count|expression) matri(x|ces)|processed (data|files?|matri(x|ces))|'
                     r'supplementary (file|table)s? (from|in|of) (the )?GEO|\bRMA\b', re.I)

ap = argparse.ArgumentParser()
ap.add_argument('--pairs', required=True)
ap.add_argument('--cache-dir', required=True)
ap.add_argument('--max-chars', type=int, default=12000)
ap.add_argument('--out', default='contexts.jsonl')
args = ap.parse_args()

cache = Path(args.cache_dir)
epmc = requests.Session()
ncbi = requests.Session()
ncbi.mount('https://', HTTPAdapter(max_retries=Retry(total=4, backoff_factor=2,
                                                     status_forcelist=[429, 500, 502, 503, 504])))
API_KEY = os.environ.get('NCBI_API_KEY', '')
NCBI_PAUSE = 0.11 if API_KEY else 0.35   # 10 or 3 requests per second


def cache_path(pmc, src):
    d = cache / pmc[-3:]
    d.mkdir(parents=True, exist_ok=True)
    return d / f'{pmc}.{src}.xml.gz'


def get_xml(pmc):
    for src in ('ncbi', 'europepmc'):
        f = cache_path(pmc, src)
        if f.exists() and f.stat().st_size > 0:
            return src, gzip.decompress(f.read_bytes())
        old = cache / f'{pmc}.{src}.xml'          # earlier runs: flat, uncompressed
        if old.exists() and old.stat().st_size > 0:
            return src, old.read_bytes()
    params = {'db': 'pmc', 'id': pmc.removeprefix('PMC'), 'retmode': 'xml',
              'tool': 'reusability', 'email': 'drabcinski@gmail.com'}
    if API_KEY:
        params['api_key'] = API_KEY
    try:
        r = ncbi.get(NCBI, params=params, timeout=60)
        time.sleep(NCBI_PAUSE)
        if r.ok and b'<body' in r.content:
            cache_path(pmc, 'ncbi').write_bytes(gzip.compress(r.content))
            return 'ncbi', r.content
    except requests.RequestException:
        pass
    try:
        r = epmc.get(EPMC.format(pmc=pmc), timeout=15)
        if r.ok and b'<body' in r.content:
            cache_path(pmc, 'europepmc').write_bytes(gzip.compress(r.content))
            return 'europepmc', r.content
    except requests.RequestException:
        pass
    return 'none', None


def parse(xml):
    """([(section path, paragraph text)] from body and back matter, whole article text)."""
    root = etree.fromstring(xml, parser=etree.XMLParser(recover=True, huge_tree=True))
    found = root if root.tag == 'article' else root.find('.//article')
    art = found if found is not None else root
    out = []
    for part in ('body', 'back'):
        node = art.find(part)
        if node is None:
            continue
        for p in node.iter('p', 'td', 'caption', 'title'):
            if p.tag == 'title' and p.getparent() is not None and p.getparent().tag == 'sec':
                continue
            path = []
            for anc in p.iterancestors():
                if anc.tag == 'sec':
                    t = anc.find('title')
                    path.append(' '.join(''.join(t.itertext()).split()) if t is not None else '')
                if anc.tag == part:
                    break
            sec = ' > '.join([part] + [x for x in reversed(path) if x])
            text = ' '.join(''.join(p.itertext()).split())
            if text:
                out.append((sec, text))
    whole = ' '.join(' '.join(art.itertext()).split())
    return out, whole


def terms(regex, texts):
    return sorted({m.group(0).lower() for t in texts for m in regex.finditer(t)})


pairs = pd.read_csv(args.pairs, dtype=str)
n_papers = n_none = n_window = 0
with open(args.out, 'w') as fo:
    for pmc, grp in pairs.groupby('pmc_ID', sort=False):
        n_papers += 1
        src, xml = get_xml(pmc)
        paras, whole = parse(xml) if xml else ([], '')
        n_none += src == 'none'
        first, more, per_pair, chosen = [], [], [], set()
        for _, r in grp.iterrows():
            keys = {k for k in (r.accession, r.get('converted_accession')) if isinstance(k, str) and k}
            hit = [(s, t) for s, t in paras if any(k in t for k in keys)]
            if hit:
                if hit[0] not in chosen:
                    first.append(hit[0])
                    chosen.add(hit[0])
                more += [h for h in hit[1:] if h not in chosen]
            elif whole:
                pos = min((whole.find(k) for k in keys if k in whole), default=-1)
                if pos >= 0:   # e.g. in a table footnote, a ref list or front matter
                    win = ('anywhere in the article', whole[max(0, pos - 400):pos + 400])
                    first.append(win)
                    hit = [win]
                    n_window += 1
            texts = [t for _, t in hit]
            per_pair.append({'accession': r.accession, 'n_mentions': len(hit),
                             'raw_terms': terms(RAW_RE, texts), 'processed_terms': terms(PROC_RE, texts)})
        meth = [(s, t) for s, t in paras if METHODS_RE.search(s) and (s, t) not in chosen and (s, t) not in more]
        meth.sort(key=lambda x: -len(SOURCE_RE.findall(x[1])))
        chunks, size, seen = [], 0, set()
        for s, t in first + more + meth:
            if (s, t) in seen:
                continue
            seen.add((s, t))
            piece = f'[{s}] {t}'
            if size + len(piece) > args.max_chars:
                if not chunks:
                    chunks.append(piece[:args.max_chars])
                break
            chunks.append(piece)
            size += len(piece)
        fo.write(json.dumps({'pmc_ID': pmc, 'text_source': src, 'context': '\n\n'.join(chunks),
                             'pairs': per_pair}, ensure_ascii=False) + '\n')
print(f'pairs: {len(pairs)}, papers: {n_papers}, papers without full text: {n_none}, '
      f'accessions found only outside paragraphs: {n_window}')
