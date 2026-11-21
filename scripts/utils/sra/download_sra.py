import csv
import re
import shlex
import subprocess
import os
import glob
import sys
import threading
import time
import requests
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

import argparse

_ap = argparse.ArgumentParser(description='Download SRA run metadata for every accession in a matrix.')
_ap.add_argument('--data-dir', default='../data', help='Persistent folder for per-accession downloads')
_ap.add_argument('--matrix', default=None,
                 help='pre_filter_matrix.csv (default: all CSVs in <data-dir>/pre_filter_matrices)')
ARGS = _ap.parse_args()

DATA_DIR = ARGS.data_dir

# NCBI SRA: Run/Study/Experiment/Sample for NCBI (SRR/SRP/SRX/SRS) and EBI (ERR/ERP/ERX/ERS)
# DRA*/ERA* (DDBJ/EBI archives) are routed to ENA API instead.
# SRA*/SRZ*/ERZ*/DRZ* (submission-level) are skipped — not queryable.
# PRJNA (BioProject) is queried via NCBI esearch to get its SRP/runs.
# GEO (GSE/GPL/GDS) are excluded — they don't have SRA runinfo.
NCBI_PATTERN = re.compile(r'^[SE]R[RPXS]\d+$')
PRJNA_PATTERN = re.compile(r'^PRJNA\d+$')
# DDBJ (DRR/DRP/DRX/DRS/DRA) and EBI archive-level ERA only: ERR/ERP/ERX/ERS go to NCBI above,
# so no accession is fetched twice.
ENA_PATTERN  = re.compile(r'^(?:DR[RPXSA]|ERA)\d+$')

interim_pfm_data = f'{DATA_DIR}/pre_filter_matrices/'
interim_scr_data = f'{DATA_DIR}/sra_complete_runs/'

os.makedirs(interim_scr_data, exist_ok=True)

# Accessions that were queried successfully but have no SRA runs. Remembered so that
# re-running the step does not query them again.
os.makedirs(DATA_DIR, exist_ok=True)
NO_RUNS_FILE = os.path.join(DATA_DIR, 'sra_no_runs.txt')
_no_runs_lock = threading.Lock()


def load_no_runs():
    if os.path.exists(NO_RUNS_FILE):
        with open(NO_RUNS_FILE) as f:
            return {line.strip() for line in f if line.strip()}
    return set()


def record_no_runs(value):
    with _no_runs_lock:
        with open(NO_RUNS_FILE, 'a') as f:
            f.write(value + '\n')

# NCBI rate limit: 3 req/s without API key, 10/s with key
NCBI_API_KEY = os.environ.get('NCBI_API_KEY', '')
NCBI_WORKERS = 10 if NCBI_API_KEY else 3
ENA_WORKERS  = 4  # ENA portal API recommended limit
RETRY_ROUNDS = 3

_counter_lock = threading.Lock()
counter = 0

# ENA fields → NCBI runinfo column names for a unified output schema
ENA_FIELDS = (
    'run_accession,study_accession,experiment_accession,sample_accession,'
    'scientific_name,tax_id,library_strategy,library_selection,library_source,'
    'library_layout,instrument_platform,instrument_model,first_public'
)
ENA_TO_NCBI = {
    'run_accession':      'Run',
    'study_accession':    'SRAStudy',
    'experiment_accession': 'Experiment',
    'sample_accession':   'Sample',
    'scientific_name':    'ScientificName',
    'tax_id':             'TaxID',
    'library_strategy':   'LibraryStrategy',
    'library_selection':  'LibrarySelection',
    'library_source':     'LibrarySource',
    'library_layout':     'LibraryLayout',
    'instrument_platform': 'Platform',
    'instrument_model':   'Model',
    'first_public':       'ReleaseDate',
}


def fetch_ncbi_accession(value):
    """Query NCBI esearch | efetch for one SRR/SRP/SRX/SRS/PRJNA accession.

    esearch 19.2+ reads NCBI_API_KEY from the environment.
    "No runs" is recorded only when esearch reports Count 0. An empty efetch
    result after a non-zero count (throttling, network) is a failure and is
    retried, so it never lands in sra_no_runs.txt by mistake.
    """
    out_path = os.path.join(interim_scr_data, f'output_{value}.csv')
    env = os.environ.copy()
    if NCBI_API_KEY:
        env['NCBI_API_KEY'] = NCBI_API_KEY
    search = subprocess.run(['esearch', '-db', 'sra', '-query', value],
                            capture_output=True, text=True, env=env)
    m = re.search(r'<Count>(\d+)</Count>', search.stdout)
    if search.returncode != 0 or not m:
        return value, False
    if int(m.group(1)) == 0:
        record_no_runs(value)
        return value, True
    fetch = subprocess.run(['efetch', '-format', 'runinfo'], input=search.stdout,
                           capture_output=True, text=True, env=env)
    lines = [l for l in fetch.stdout.splitlines() if l.strip()]
    if fetch.returncode != 0 or len(lines) < 2 or not lines[0].startswith('Run,'):
        return value, False
    tmp = out_path + '.part'
    with open(tmp, 'w') as f:
        f.write(fetch.stdout)
    os.replace(tmp, out_path)  # never leave a partial file that looks finished
    return value, True


def fetch_ena_accession(value):
    """Query ENA portal API for one DRA/ERA accession, save in NCBI-compatible CSV."""
    out_path = os.path.join(interim_scr_data, f'output_{value}.csv')
    url = (
        f'https://www.ebi.ac.uk/ena/portal/api/filereport'
        f'?accession={value}&result=read_run&fields={ENA_FIELDS}&format=tsv'
    )
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return value, False
        if not resp.text.strip():
            record_no_runs(value)
            return value, True

        df = pd.read_csv(StringIO(resp.text), sep='\t')
        if df.empty:
            record_no_runs(value)
            return value, True

        df = df.rename(columns=ENA_TO_NCBI)
        df.to_csv(out_path, index=False)
        return value, True
    except Exception:
        return value, False


def process_batch(values, fetch_fn, workers, desc):
    """Run fetch_fn over values with a thread pool, return failed list."""
    global counter
    failed = []
    with tqdm(total=len(values), desc=desc, unit='accession') as pbar:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_fn, v): v for v in values}
            for future in as_completed(futures):
                value, success = future.result()
                with _counter_lock:
                    counter += 1
                    global_count = counter
                if success:
                    already_runs.add(value)
                else:
                    tqdm.write(f"  WARN: failed accession {value}")
                    failed.append(value)
                pbar.update(1)
                pbar.set_postfix(total=global_count)
    return failed


# ── Main ──────────────────────────────────────────────────────────────────────

pfm_csv_files = [ARGS.matrix] if ARGS.matrix else glob.glob(os.path.join(interim_pfm_data, '*.csv'))
pfm_csv_files_sorted = sorted(pfm_csv_files, key=lambda x: os.path.getsize(x))

already_runs = {
    f[len('output_'):-len('.csv')]
    for f in os.listdir(interim_scr_data)
    if f.startswith('output_') and f.endswith('.csv')
} | load_no_runs()
counter = len(already_runs)
print(f"Already downloaded (total): {counter}")

all_failed = []
for csv_file in pfm_csv_files_sorted:
    print(f"\nProcessing CSV file: {csv_file}")

    values = []
    with open(csv_file, 'r') as file:
        for row in csv.reader(file):
            values.append(row[2])
    values = list(dict.fromkeys(values[1:]))  # dedupe, skip header

    ncbi_todo  = [v for v in values if NCBI_PATTERN.match(v)  and v not in already_runs]
    prjna_todo = [v for v in values if PRJNA_PATTERN.match(v) and v not in already_runs]
    ena_todo   = [v for v in values if ENA_PATTERN.match(v)   and v not in already_runs]
    skipped    = len(values) - len(ncbi_todo) - len(prjna_todo) - len(ena_todo) - \
                 sum(1 for v in values if v in already_runs)

    print(f"Unique: {len(values)} | NCBI: {len(ncbi_todo)} | PRJNA: {len(prjna_todo)} | "
          f"ENA: {len(ena_todo)} | Skip: {skipped} | Workers NCBI={NCBI_WORKERS} ENA={ENA_WORKERS}")

    if not ncbi_todo and not prjna_todo and not ena_todo:
        print(f"All accessions already downloaded for: {csv_file}")
        continue

    failed = []
    if ncbi_todo:
        failed += process_batch(ncbi_todo, fetch_ncbi_accession, NCBI_WORKERS, 'NCBI download')

    if prjna_todo:
        failed += process_batch(prjna_todo, fetch_ncbi_accession, NCBI_WORKERS, 'PRJNA download')

    if ena_todo:
        failed += process_batch(ena_todo, fetch_ena_accession, ENA_WORKERS, 'ENA download')

    # retry failures, one worker at a time and with a pause, before giving up
    for attempt in range(1, RETRY_ROUNDS + 1):
        if not failed:
            break
        time.sleep(30 * attempt)
        ena_f = [v for v in failed if ENA_PATTERN.match(v)]
        ncbi_f = [v for v in failed if not ENA_PATTERN.match(v)]
        failed = []
        if ncbi_f:
            failed += process_batch(ncbi_f, fetch_ncbi_accession, 1, f'NCBI retry {attempt}')
        if ena_f:
            failed += process_batch(ena_f, fetch_ena_accession, 1, f'ENA retry {attempt}')
    all_failed += failed

FAILED_FILE = os.path.join(DATA_DIR, 'sra_failed.txt')
with open(FAILED_FILE, 'w') as f:
    f.writelines(v + '\n' for v in all_failed)
print(f"\nStill failing after {RETRY_ROUNDS} retry rounds: {len(all_failed)} (listed in {FAILED_FILE})")
sys.exit(1 if all_failed else 0)
