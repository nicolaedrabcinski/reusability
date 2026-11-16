"""Print the names of the .xml.gz files on the Europe PMC open-access FTP, one per line."""
import argparse
import ftplib
import sys

ap = argparse.ArgumentParser()
ap.add_argument('--server', default='ftp.ebi.ac.uk')
ap.add_argument('--dir', default='/pub/databases/pmc/oa/')
ap.add_argument('--limit', type=int, default=0, help='keep only the first N files (testing)')
args = ap.parse_args()

try:
    with ftplib.FTP(args.server, timeout=30) as ftp:
        ftp.login()
        ftp.cwd(args.dir)
        names = sorted(n for n in ftp.nlst() if n.endswith('.xml.gz'))
except ftplib.all_errors as e:
    sys.exit(f"Could not list {args.server}{args.dir}: {e}")

if not names:
    sys.exit(f"No .xml.gz files found in {args.server}{args.dir}")
if args.limit:
    names = names[:args.limit]
print('\n'.join(names))
