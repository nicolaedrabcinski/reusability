"""
QC checks for each pipeline step.
Usage: python qc.py --step N [--data-dir ../data]
Exits with code 1 if any check fails, so run.sh stops the pipeline.
"""
import argparse
import os
import re
import sys
import csv
import glob
from pathlib import Path
from datetime import datetime

# ── Helpers ───────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
BOLD   = "\033[1m"

_failures = []
_warnings = []

def ok(msg):
    print(f"  {GREEN}✓{RESET}  {msg}")

def fail(msg):
    print(f"  {RED}✗{RESET}  {msg}")
    _failures.append(msg)

def warn(msg):
    print(f"  {YELLOW}!{RESET}  {msg}")
    _warnings.append(msg)

def header(step, label):
    print(f"\n{BOLD}=== QC Step {step}: {label} ==={RESET}")
    print(f"    {datetime.now().strftime('%H:%M:%S')}")

def finish(step):
    if _failures:
        print(f"\n{RED}{BOLD}FAILED{RESET} — {len(_failures)} check(s) failed, {len(_warnings)} warning(s)")
        for f in _failures:
            print(f"  {RED}✗{RESET} {f}")
        sys.exit(1)
    elif _warnings:
        print(f"\n{YELLOW}{BOLD}PASSED with warnings{RESET} — {len(_warnings)} warning(s)")
    else:
        print(f"\n{GREEN}{BOLD}PASSED{RESET} — all checks OK")

# ── Accession patterns (same as download_publications.py) ─────────────────────

ACCESSION_RE = re.compile(
    r'^([SDE]R[RPXSZA]\d+|PRJNA\d+|GDS\d+|GSE\d+|GPL\d+)$'
)

# ── Step QC functions ──────────────────────────────────────────────────────────

def qc_step1(data_dir):
    """Pre-filter matrices: one CSV per archive, non-empty, valid accessions."""
    header(1, "Download & parse PMC publications")
    import pandas as pd

    pfm_dir = Path(data_dir) / 'pre_filter_matrices'
    pub_dir = Path(data_dir) / 'publications'

    csv_files = list(pfm_dir.glob('*.csv'))
    arch_dirs = [d for d in pub_dir.iterdir() if d.is_dir()] if pub_dir.exists() else []

    ok(f"Found {len(csv_files)} pre-filter matrix CSVs")

    if len(csv_files) == 0:
        fail("No CSV files in pre_filter_matrices/")
        finish(1); return

    empty = [f.name for f in csv_files if f.stat().st_size == 0]
    if empty:
        fail(f"{len(empty)} empty CSV files: {empty[:5]}")
    else:
        ok("All CSV files are non-empty")

    # Sample 5 files for column + accession checks
    total_rows = 0
    bad_acc = 0
    for f in csv_files[:5]:
        df = pd.read_csv(f)
        total_rows += len(df)
        if not {'journal_name','pmc_id','accession'}.issubset(df.columns):
            fail(f"{f.name}: missing expected columns, got {list(df.columns)}")
        if df.isnull().any().any():
            warn(f"{f.name}: contains nulls")
        invalid = df['accession'].apply(lambda x: not bool(ACCESSION_RE.match(str(x)))).sum()
        bad_acc += invalid

    rate = bad_acc / max(total_rows, 1) * 100
    if rate > 5:
        warn(f"High invalid accession rate in sample: {rate:.1f}%")
    else:
        ok(f"Accession format OK (invalid rate in sample: {rate:.1f}%)")

    finish(1)


def qc_step2(data_dir):
    """Concatenated pre_filter_matrix.csv: row count, columns, nulls, dedup."""
    header(2, "Concat pre-filter matrices")
    import pandas as pd

    pfm_path = Path(data_dir) / 'pre_filter_matrix.csv'
    pfm_dir  = Path(data_dir) / 'pre_filter_matrices'

    if not pfm_path.exists():
        fail("pre_filter_matrix.csv does not exist"); finish(2); return

    df = pd.read_csv(pfm_path)
    ok(f"Rows: {len(df):,}")

    # Column check
    expected_cols = {'journal_name', 'pmc_id', 'accession'}
    if not expected_cols.issubset(df.columns):
        fail(f"Missing columns: {expected_cols - set(df.columns)}")
    else:
        ok("All expected columns present")

    # Null check
    null_counts = df.isnull().sum()
    if null_counts.any():
        fail(f"Nulls found: {null_counts[null_counts > 0].to_dict()}")
    else:
        ok("No null values")

    # Row count vs sum of individual files
    csv_files = list(pfm_dir.glob('*.csv'))
    if csv_files:
        sum_rows = sum(len(pd.read_csv(f)) for f in csv_files)
        if abs(len(df) - sum_rows) > 10:
            fail(f"Row count mismatch: combined={len(df):,} vs sum of files={sum_rows:,}")
        else:
            ok(f"Row count matches sum of individual files ({sum_rows:,})")

    # Duplicate (pmc_id, accession) check
    dupes = df.duplicated(subset=['pmc_id', 'accession']).sum()
    if dupes > 0:
        warn(f"{dupes:,} duplicate (pmc_id, accession) pairs")
    else:
        ok("No duplicate (pmc_id, accession) pairs")

    # Accession type distribution
    df['acc_type'] = df['accession'].str.extract(r'^([A-Z]+)')
    dist = df['acc_type'].value_counts()
    ok(f"Accession types: {dist.head(5).to_dict()}")

    finish(2)


def qc_step3(data_dir):
    """pmc_paths.txt: exists, non-empty, all paths exist on disk."""
    header(3, "Generate PMC paths list")

    path_file = Path(data_dir) / 'pmc_paths.txt'
    if not path_file.exists():
        fail("pmc_paths.txt does not exist"); finish(3); return

    with open(path_file) as f:
        paths = [p.strip() for p in f if p.strip()]

    ok(f"Total XML paths: {len(paths):,}")

    if len(paths) == 0:
        fail("pmc_paths.txt is empty"); finish(3); return

    # Check sample of 200 paths actually exist
    sample = paths[:100] + paths[-100:]
    missing = [p for p in sample if not os.path.exists(p)]
    if missing:
        fail(f"{len(missing)} sampled paths do not exist on disk: {missing[:3]}")
    else:
        ok(f"Sampled {len(sample)} paths — all exist on disk")

    # Non-.xml entries
    non_xml = [p for p in paths[:1000] if not p.endswith('.xml')]
    if non_xml:
        warn(f"{len(non_xml)} non-XML entries in sample: {non_xml[:3]}")
    else:
        ok("All sampled entries are .xml files")

    finish(3)


def qc_step4(data_dir):
    """pre_filter_dates.csv: row count, date format, coverage rate."""
    header(4, "Extract publication dates")
    import pandas as pd

    dates_path = Path(data_dir) / 'pre_filter_dates.csv'
    paths_file = Path(data_dir) / 'pmc_paths.txt'

    if not dates_path.exists():
        fail("pre_filter_dates.csv does not exist"); finish(4); return

    df = pd.read_csv(dates_path)
    ok(f"Rows with parsed dates: {len(df):,}")

    # Column check
    if not {'File Name', 'Date'}.issubset(df.columns):
        fail(f"Missing columns, got: {list(df.columns)}"); finish(4); return
    ok("Columns OK: File Name, Date")

    # Coverage vs total XML
    if paths_file.exists():
        with open(paths_file) as f:
            total_xml = sum(1 for l in f if l.strip())
        coverage = len(df) / total_xml * 100
        if coverage < 30:
            warn(f"Low date coverage: {coverage:.1f}% of XML files ({len(df):,}/{total_xml:,})")
        else:
            ok(f"Date coverage: {coverage:.1f}% ({len(df):,}/{total_xml:,} XML files)")

    # Date format check
    bad_fmt = re.compile(r'^\d{4}/\d{1,2}/\d{1,2}$')
    sample_dates = df['Date'].dropna().head(10000)
    invalid = sample_dates.apply(lambda x: not bool(bad_fmt.match(str(x)))).sum()
    if invalid > 0:
        warn(f"{invalid} dates in sample don't match YYYY/M/D format")
    else:
        ok("Date format YYYY/M/D OK")

    # Null dates
    null_dates = df['Date'].isnull().sum()
    if null_dates > 0:
        warn(f"{null_dates:,} null dates ({null_dates/len(df)*100:.1f}%)")
    else:
        ok("No null dates")

    finish(4)


def qc_step5(data_dir):
    """GEO reference files: exist, non-empty, expected columns, reasonable size."""
    header(5, "Download GEO reference metadata")
    import pandas as pd

    files = {
        'geo_samples.csv':   ('Accession', 'Release Date', 'SRA Accession'),
        'geo_series.csv':    ('Accession', 'Release Date', 'SRA Accession'),
        'geo_platforms.csv': ('Accession', 'Release Date'),
    }
    min_rows = {'geo_samples.csv': 10_000, 'geo_series.csv': 5_000, 'geo_platforms.csv': 100}

    for fname, req_cols in files.items():
        fpath = Path(data_dir) / fname
        if not fpath.exists():
            fail(f"{fname} does not exist"); continue

        df = pd.read_csv(fpath, nrows=5)
        actual_cols = set(df.columns)
        missing_cols = set(req_cols) - actual_cols
        if missing_cols:
            fail(f"{fname}: missing columns {missing_cols}")
        else:
            ok(f"{fname}: columns OK")

        # Count rows without loading full file
        with open(fpath) as f:
            nrows = sum(1 for _ in f) - 1  # subtract header
        if nrows < min_rows[fname]:
            fail(f"{fname}: only {nrows:,} rows (expected ≥ {min_rows[fname]:,})")
        else:
            ok(f"{fname}: {nrows:,} rows")

        # Null accession check (sample)
        df_sample = pd.read_csv(fpath, nrows=1000)
        null_acc = df_sample['Accession'].isnull().sum()
        if null_acc > 0:
            warn(f"{fname}: {null_acc} null accessions in first 1000 rows")

    finish(5)


def qc_step6(data_dir):
    """SRA run CSVs: coverage vs expected accessions, corrupt file rate."""
    header(6, "Download SRA run metadata")
    import pandas as pd

    sra_dir   = Path(data_dir) / 'sra_complete_runs'
    pfm_path  = Path(data_dir) / 'pre_filter_matrix.csv'

    if not sra_dir.exists():
        fail("sra_complete_runs/ directory does not exist"); finish(6); return

    sra_files = list(sra_dir.glob('output_*.csv'))
    ok(f"SRA output files: {len(sra_files):,}")

    if len(sra_files) == 0:
        fail("No SRA output files found"); finish(6); return

    # Expected accession count from pre_filter_matrix
    if pfm_path.exists():
        SRA_PAT = re.compile(r'^[SDE]R[RPXSZA]\d+$')
        df_pfm = pd.read_csv(pfm_path)
        expected = df_pfm['accession'].apply(lambda x: bool(SRA_PAT.match(str(x)))).sum()
        coverage = len(sra_files) / expected * 100 if expected else 0
        if coverage < 70:
            warn(f"Low SRA coverage: {len(sra_files):,}/{expected:,} ({coverage:.1f}%)")
        else:
            ok(f"SRA coverage: {len(sra_files):,}/{expected:,} ({coverage:.1f}%)")

    # Check for empty/corrupt files (sample 500)
    sample = sra_files[:500]
    empty = [f for f in sample if f.stat().st_size == 0]
    corrupt = []
    for f in sample[:200]:
        try:
            pd.read_csv(f, nrows=1)
        except Exception:
            corrupt.append(f.name)

    empty_rate = len(empty) / len(sample) * 100
    if empty_rate > 10:
        warn(f"High empty file rate in sample: {empty_rate:.1f}% ({len(empty)}/{len(sample)})")
    else:
        ok(f"Empty file rate in sample: {empty_rate:.1f}%")

    if corrupt:
        warn(f"{len(corrupt)} corrupt CSV files in sample: {corrupt[:3]}")
    else:
        ok("No corrupt CSV files in sample")

    finish(6)


def qc_step7(data_dir):
    """sra_complete_runs.csv: exists, non-empty, key columns, row count sanity."""
    header(7, "Concat SRA runs")
    import pandas as pd

    out_path = Path(data_dir) / 'sra_complete_runs.csv'
    sra_dir  = Path(data_dir) / 'sra_complete_runs'

    if not out_path.exists():
        fail("sra_complete_runs.csv does not exist"); finish(7); return

    df_head = pd.read_csv(out_path, nrows=5)
    ok(f"Columns ({len(df_head.columns)}): {list(df_head.columns[:8])}...")

    key_cols = {'Run', 'SRAStudy', 'LibraryStrategy'}
    missing = key_cols - set(df_head.columns)
    if missing:
        fail(f"Missing key columns: {missing}")
    else:
        ok(f"Key columns present: {key_cols}")

    # Row count
    with open(out_path) as f:
        nrows = sum(1 for _ in f) - 1
    ok(f"Total rows: {nrows:,}")

    if nrows < 1000:
        fail(f"Suspiciously low row count: {nrows:,}")

    # Compare with sum of individual files
    if sra_dir.exists():
        n_files = len(list(sra_dir.glob('output_*.csv')))
        if n_files > 0 and nrows < n_files:
            warn(f"Row count ({nrows:,}) < number of SRA files ({n_files:,}) — possible concat issue")

    finish(7)


# ── Main ──────────────────────────────────────────────────────────────────────

STEPS = {1: qc_step1, 2: qc_step2, 3: qc_step3,
         4: qc_step4, 5: qc_step5, 6: qc_step6, 7: qc_step7}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pipeline QC checks')
    parser.add_argument('--step', type=int, required=True, choices=range(1, 8))
    parser.add_argument('--data-dir', default='../data')
    args = parser.parse_args()

    fn = STEPS[args.step]
    fn(args.data_dir)
