import csv
import re
import subprocess
import os
import glob
import threading
import requests
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

DATA_DIR = '../data'

# NCBI SRA: Run/Study/Experiment/Sample for NCBI (SRR/SRP/SRX/SRS) and EBI (ERR/ERP/ERX/ERS)
# DRA*/ERA* (DDBJ/EBI archives) are routed to ENA API instead.
# SRA*/SRZ*/ERZ*/DRZ* (submission-level) are skipped — not queryable.
# PRJNA (BioProject) is queried via NCBI esearch to get its SRP/runs.
# GEO (GSE/GPL/GDS) are excluded — they don't have SRA runinfo.
NCBI_PATTERN = re.compile(r'^[SE]R[RPXS]\d+$')
PRJNA_PATTERN = re.compile(r'^PRJNA\d+$')
ENA_PATTERN  = re.compile(r'^[DE]R[RPXSA]\d+$')  # DRA/DRR/DRP/DRX/DRS + ERA/ERR...

interim_pfm_data = f'{DATA_DIR}/pre_filter_matrices/'
interim_scr_data = f'{DATA_DIR}/sra_complete_runs/'

os.makedirs(interim_scr_data, exist_ok=True)

# NCBI rate limit: 3 req/s without API key, 10/s with key
NCBI_API_KEY = os.environ.get('NCBI_API_KEY', '')
NCBI_WORKERS = 10 if NCBI_API_KEY else 3
ENA_WORKERS  = 4  # ENA portal API recommended limit

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

    esearch 19.2+ uses the NCBI_API_KEY environment variable automatically —
    the -api_key CLI flag is not supported and causes silent failures.
    """
    out_path = os.path.join(interim_scr_data, f'output_{value}.csv')
    env = os.environ.copy()
    if NCBI_API_KEY:
        env['NCBI_API_KEY'] = NCBI_API_KEY
    result = subprocess.run(
        f"esearch -db sra -query {value} | efetch -format runinfo > {out_path}",
        shell=True,
        capture_output=True,
        env=env,
    )
    # Empty or header-only file means no SRA runs for this accession
    if result.returncode == 0 and os.path.exists(out_path):
        if os.path.getsize(out_path) < 10:
            os.remove(out_path)
            return value, False
    return value, result.returncode == 0


def fetch_ena_accession(value):
    """Query ENA portal API for one DRA/ERA accession, save in NCBI-compatible CSV."""
    out_path = os.path.join(interim_scr_data, f'output_{value}.csv')
    url = (
        f'https://www.ebi.ac.uk/ena/portal/api/filereport'
        f'?accession={value}&result=read_run&fields={ENA_FIELDS}&format=tsv'
    )
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200 or not resp.text.strip():
            return value, False

        df = pd.read_csv(StringIO(resp.text), sep='\t')
        if df.empty:
            return value, False

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

pfm_csv_files = glob.glob(os.path.join(interim_pfm_data, '*.csv'))
pfm_csv_files_sorted = sorted(pfm_csv_files, key=lambda x: os.path.getsize(x))

already_runs = {
    f.replace('output_', '').replace('.csv', '')
    for f in os.listdir(interim_scr_data)
}
counter = len(already_runs)
print(f"Already downloaded (total): {counter}")

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

    if ncbi_todo:
        process_batch(ncbi_todo, fetch_ncbi_accession, NCBI_WORKERS, 'NCBI download')

    if prjna_todo:
        process_batch(prjna_todo, fetch_ncbi_accession, NCBI_WORKERS, 'PRJNA download')

    if ena_todo:
        process_batch(ena_todo, fetch_ena_accession, ENA_WORKERS, 'ENA download')
