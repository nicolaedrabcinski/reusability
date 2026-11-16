"""Download the full GEO tables (series, samples, platforms) via NCBI E-utilities.

Replaces the GEO "browse" CSV export, which is now behind a reCAPTCHA. Produces the same
files with the same columns:
  geo_series.csv, geo_samples.csv (one row per sample), geo_platforms.csv

GEO record ids are derived from the accession number (GSE n -> 200000000+n,
GPL n -> 100000000+n, GSM n -> 300000000+n, GDS n -> n), so every number range is
walked in batches of esummary requests until the records run out.

Rows are appended as they arrive; progress is saved after every wave, so an interrupted
run continues where it stopped. Set NCBI_API_KEY to raise the rate limit (3 -> 10 req/s).
"""
import argparse
import csv
import json
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
API_KEY = os.environ.get('NCBI_API_KEY', '')
MIN_INTERVAL = 0.11 if API_KEY else 0.35  # seconds between requests (shared by all threads)
_lock = threading.Lock()
_last = [0.0]

SERIES_COLS = ['Accession', 'Title', 'Series Type', 'Taxonomy', 'Sample Count', 'Datasets',
               'Supplementary Types', 'Supplementary Links', 'PubMed ID', 'SRA Accession',
               'Contact', 'Release Date']
SAMPLES_COLS = ['Accession', 'Title', 'Sample Type', 'Taxonomy', 'Channels', 'Platform', 'Series',
                'Supplementary Types', 'Supplementary Links', 'SRA Accession', 'Contact', 'Release Date']
PLATFORMS_COLS = ['Accession', 'Title', 'Technology', 'Taxonomy', 'Data Rows', 'Samples Count',
                  'Series Count', 'Contact', 'Release Date']

MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def geo_date(pdat):
    """'2019/05/10' -> 'May 10, 2019' (the format the notebooks parse)."""
    try:
        y, m, d = pdat.split('/')[:3]
        return f"{MONTHS[int(m) - 1]} {int(d):02d}, {y}"
    except (ValueError, IndexError, AttributeError):
        return ''


def esummary(uids):
    """Return the list of records (dicts with an accession) for a batch of uids."""
    data = {'db': 'gds', 'retmode': 'json', 'id': ','.join(map(str, uids))}
    if API_KEY:
        data['api_key'] = API_KEY
    last_err = None
    for attempt in range(6):
        with _lock:
            wait = _last[0] + MIN_INTERVAL - time.time()
            if wait > 0:
                time.sleep(wait)
            _last[0] = time.time()
        try:
            r = requests.post(URL, data=data, timeout=120)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.RequestException(f'HTTP {r.status_code}')
            r.raise_for_status()
            result = r.json()['result']  # KeyError if NCBI answered with an error object
            return [result[u] for u in result.get('uids', []) if result[u].get('accession')]
        except (requests.RequestException, ValueError, KeyError) as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f'esummary failed: {last_err}')


def split_ids(value):
    return [x.strip() for x in str(value or '').split(';') if x.strip().isdigit()]


def walk(kind, offset, batch, stop_after, workers, handle_rows, state_dir, out_path, cols):
    """Walk accession numbers 1.. in batches, appending rows to out_path.

    Stops after `stop_after` consecutive batches with no records (end of the range).
    Resumable: <state_dir>/<kind>.state holds the next number and the output size.
    """
    state_path = os.path.join(state_dir, f'{kind}.state')
    nxt, size = 1, 0
    if os.path.exists(state_path):
        with open(state_path) as f:
            st = json.load(f)
        if st.get('done'):
            print(f"[{kind}] already complete", flush=True)
            return
        nxt, size = st['next'], st['size']
    if size and os.path.exists(out_path):
        with open(out_path, 'r+') as f:
            f.truncate(size)  # drop anything written after the last saved state
        mode = 'a'
    else:
        nxt, mode = 1, 'w'

    empty_run, total = 0, 0
    wave = workers * 4
    with open(out_path, mode, newline='') as f:
        writer = csv.DictWriter(f, fieldnames=cols, restval='')
        if mode == 'w':
            writer.writeheader()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            finished = False
            while not finished:
                starts = [nxt + i * batch for i in range(wave)]
                results = list(ex.map(
                    lambda s: esummary([offset + n for n in range(s, s + batch)]), starts))
                for s, recs in zip(starts, results):
                    if recs:
                        empty_run = 0
                        total += len(recs)
                        writer.writerows(handle_rows(recs))
                    else:
                        empty_run += 1
                        if empty_run >= stop_after:
                            finished = True
                            break
                nxt = starts[-1] + batch if not finished else nxt
                f.flush()
                os.fsync(f.fileno())
                state = {'next': nxt, 'size': f.tell()}
                if finished:
                    state = {'done': True}
                with open(state_path + '.tmp', 'w') as sf:
                    json.dump(state, sf)
                os.replace(state_path + '.tmp', state_path)
                print(f"[{kind}] up to number {nxt:,}, {total:,} records this run", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out-dir', default='.', help='where the final geo_*.csv files are written')
    ap.add_argument('--state-dir', required=True, help='persistent folder for partial files and progress')
    ap.add_argument('--workers', type=int, default=4)
    args = ap.parse_args()
    os.makedirs(args.state_dir, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)
    part = lambda name: os.path.join(args.state_dir, name)

    # 1. GDS -> GSE map, used to fill the "Datasets" column of the series table.
    datasets_of = {}
    ds_map_path = part('datasets_of.json')

    def ds_rows(recs):
        for r in recs:
            for g in split_ids(r.get('gse')):
                datasets_of.setdefault(f'GSE{g}', []).append(r['accession'])
        return []

    if os.path.exists(ds_map_path):
        with open(ds_map_path) as f:
            datasets_of = json.load(f)
    else:
        # small range (~30 requests): always walked from scratch so the map is complete
        if os.path.exists(os.path.join(args.state_dir, 'gds.state')):
            os.remove(os.path.join(args.state_dir, 'gds.state'))
        walk('gds', 0, 200, 5, args.workers, ds_rows, args.state_dir, part('gds.tmp'), ['x'])
        with open(ds_map_path, 'w') as f:
            json.dump(datasets_of, f)

    # 2. Series
    def series_rows(recs):
        for r in recs:
            sra = [x['targetobject'] for x in r.get('extrelations', []) if x.get('relationtype') == 'SRA']
            yield {'Accession': r['accession'], 'Title': r.get('title', ''),
                   'Series Type': r.get('gdstype', ''), 'Taxonomy': r.get('taxon', ''),
                   'Sample Count': r.get('n_samples', ''),
                   'Datasets': ','.join(sorted(datasets_of.get(r['accession'], []))),
                   'Supplementary Types': r.get('suppfile', ''),
                   'PubMed ID': ','.join(r.get('pubmedids', [])), 'SRA Accession': ','.join(sra),
                   'Release Date': geo_date(r.get('pdat'))}

    walk('series', 200000000, 200, 20, args.workers, lambda recs: list(series_rows(recs)),
         args.state_dir, part('geo_series.csv'), SERIES_COLS)

    # 3. Platforms (records are large, so small batches)
    def platform_rows(recs):
        return [{'Accession': r['accession'], 'Title': r.get('title', ''),
                 'Technology': r.get('ptechtype', ''), 'Taxonomy': r.get('taxon', ''),
                 'Samples Count': r.get('n_samples', ''), 'Series Count': len(split_ids(r.get('gse'))),
                 'Release Date': geo_date(r.get('pdat'))} for r in recs]

    walk('platforms', 100000000, 20, 50, args.workers, platform_rows,
         args.state_dir, part('geo_platforms.csv'), PLATFORMS_COLS)

    # 4. Samples: one row per sample (and per series, when a sample belongs to several)
    def sample_rows(recs):
        rows = []
        for r in recs:
            sra = ','.join(x['targetobject'] for x in r.get('extrelations', [])
                           if x.get('relationtype') == 'SRA')
            for g in split_ids(r.get('gse')) or ['']:
                rows.append({'Accession': r['accession'], 'Title': r.get('title', ''),
                             'Taxonomy': r.get('taxon', ''),
                             'Platform': f"GPL{split_ids(r.get('gpl'))[0]}" if split_ids(r.get('gpl')) else '',
                             'Series': f'GSE{g}' if g else '',
                             'Supplementary Types': r.get('suppfile', ''), 'SRA Accession': sra,
                             'Release Date': geo_date(r.get('pdat'))})
        return rows

    walk('samples', 300000000, 200, 50, args.workers, sample_rows,
         args.state_dir, part('geo_samples.csv'), SAMPLES_COLS)

    for name in ('geo_series.csv', 'geo_samples.csv', 'geo_platforms.csv'):
        shutil.copyfile(part(name), os.path.join(args.out_dir, name))
        print(f"Wrote {os.path.join(args.out_dir, name)}")


if __name__ == '__main__':
    main()
