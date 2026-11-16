"""Download one PMC .xml.gz archive, extract accessions and dates, delete the archive.

Writes <stem>.matrix.csv  (journal_name, pmc_id, accession)
   and <stem>.dates.csv   (File Name, Date)  into the current directory.
Exits non-zero on any failure so the caller (Nextflow) can retry.
"""
import argparse
import csv
import subprocess
import sys
from pathlib import Path

from pmc_stream import iter_articles

ap = argparse.ArgumentParser()
ap.add_argument('--name', required=True, help='archive file name, e.g. PMC10000002_PMC10010000.xml.gz')
ap.add_argument('--server', default='ftp.ebi.ac.uk')
ap.add_argument('--dir', default='/pub/databases/pmc/oa/')
args = ap.parse_args()

archive = Path(args.name)
stem = archive.name[:-len('.xml.gz')]
url = f"ftp://{args.server}/{args.dir.strip('/')}/{args.name}"

subprocess.run(['wget', '--quiet', '--continue', '-O', str(archive), url], check=True)
if not archive.is_file() or archive.stat().st_size == 0:
    sys.exit(f"Download failed or empty: {url}")

n_articles = 0
try:
    with open(f'{stem}.matrix.csv', 'w', newline='') as mf, open(f'{stem}.dates.csv', 'w', newline='') as df:
        matrix = csv.writer(mf, lineterminator='\n')
        dates = csv.writer(df, lineterminator='\n')
        matrix.writerow(['journal_name', 'pmc_id', 'accession'])
        dates.writerow(['File Name', 'Date'])
        for rec in iter_articles(archive):
            n_articles += 1
            if not (rec['accessions'] and rec['pmc_id'] and rec['journal']):
                continue
            for acc in rec['accessions']:
                matrix.writerow([rec['journal'], rec['pmc_id'], acc])
            if rec['accepted']:
                dates.writerow([f"{rec['pmc_id']}.xml", rec['accepted']])
except Exception as e:
    sys.exit(f"Failed to parse {args.name}: {e}")
finally:
    archive.unlink(missing_ok=True)  # free disk; also forces a clean re-download on retry

if n_articles == 0:
    sys.exit(f"No <article> elements found in {args.name}")
print(f"{args.name}: {n_articles} articles parsed")
