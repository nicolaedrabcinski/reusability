"""Fetch full text for each paper in pairs.csv and build the excerpt the model reads for each pair.

Full text: NCBI efetch (db=pmc) first — the corpus comes from the PMC open-access subset, and NCBI
answers in under a second — then Europe PMC fullTextXML as a fallback (it answers HTTP 500 or hangs
for many articles). NCBI_API_KEY from the environment raises the NCBI rate limit.
Raw XML is cached in --cache-dir, so reruns do not hit the APIs again.

Excerpt per pair (at most --max-chars):
  1. every paragraph that mentions the accession (or its converted series accession), with its section path;
  2. then Methods / Data availability paragraphs, those with data-source words first.
Output: contexts.jsonl, one JSON object per pair:
  {pmc_ID, accession, text_source, n_mentions, context}  (text_source = europepmc | ncbi | none)
"""
import argparse
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

ap = argparse.ArgumentParser()
ap.add_argument('--pairs', required=True)
ap.add_argument('--cache-dir', required=True)
ap.add_argument('--max-chars', type=int, default=9000)
ap.add_argument('--out', default='contexts.jsonl')
args = ap.parse_args()

cache = Path(args.cache_dir)
cache.mkdir(parents=True, exist_ok=True)
# NCBI with retries; Europe PMC only as a quick fallback.
epmc = requests.Session()
ncbi = requests.Session()
ncbi.mount('https://', HTTPAdapter(max_retries=Retry(total=4, backoff_factor=2,
                                                     status_forcelist=[429, 500, 502, 503, 504])))


API_KEY = os.environ.get('NCBI_API_KEY', '')
NCBI_PAUSE = 0.11 if API_KEY else 0.35   # 10 or 3 requests per second


def get_xml(pmc):
    for src in ('ncbi', 'europepmc'):
        f = cache / f'{pmc}.{src}.xml'
        if f.exists() and f.stat().st_size > 0:
            return src, f.read_bytes()
    params = {'db': 'pmc', 'id': pmc.removeprefix('PMC'), 'retmode': 'xml',
              'tool': 'reusability', 'email': 'drabcinski@gmail.com'}
    if API_KEY:
        params['api_key'] = API_KEY
    try:
        r = ncbi.get(NCBI, params=params, timeout=60)
        time.sleep(NCBI_PAUSE)
        if r.ok and b'<body' in r.content:
            (cache / f'{pmc}.ncbi.xml').write_bytes(r.content)
            return 'ncbi', r.content
    except requests.RequestException:
        pass
    try:
        r = epmc.get(EPMC.format(pmc=pmc), timeout=15)
        if r.ok and b'<body' in r.content:
            (cache / f'{pmc}.europepmc.xml').write_bytes(r.content)
            return 'europepmc', r.content
    except requests.RequestException:
        pass
    return 'none', None


def paragraphs(xml):
    """[(section path, paragraph text)] from body and back matter, tables and figure captions included."""
    root = etree.fromstring(xml, parser=etree.XMLParser(recover=True, huge_tree=True))
    art = root if root.tag == 'article' else (root.find('.//article') if root.find('.//article') is not None else root)
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
    return out


pairs = pd.read_csv(args.pairs, dtype=str)
done_pmc = {}
n_none = 0
with open(args.out, 'w') as fo:
    for _, r in pairs.iterrows():
        if r.pmc_ID not in done_pmc:
            src, xml = get_xml(r.pmc_ID)
            done_pmc[r.pmc_ID] = (src, paragraphs(xml) if xml else [])
        src, paras = done_pmc[r.pmc_ID]
        keys = {k for k in (r.accession, r.converted_accession) if isinstance(k, str) and k}
        hit = [(sec, t) for sec, t in paras if any(k in t for k in keys)]
        meth = [(sec, t) for sec, t in paras if METHODS_RE.search(sec) and (sec, t) not in hit]
        meth.sort(key=lambda x: -len(SOURCE_RE.findall(x[1])))
        chunks, size = [], 0
        for sec, t in hit + meth:
            piece = f'[{sec}] {t}'
            if size + len(piece) > args.max_chars:
                if not chunks:
                    chunks.append(piece[:args.max_chars])
                break
            chunks.append(piece)
            size += len(piece)
        n_none += src == 'none'
        fo.write(json.dumps({'pmc_ID': r.pmc_ID, 'accession': r.accession, 'text_source': src,
                             'n_mentions': len(hit), 'context': '\n\n'.join(chunks)}, ensure_ascii=False) + '\n')
print(f'pairs: {len(pairs)}, papers: {len(done_pmc)}, pairs without full text: {n_none}')
