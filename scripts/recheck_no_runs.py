"""Re-query every accession in sra_no_runs.txt with esearch and drop the ones that do
have SRA records (false "no runs" from throttling). Rewrites the file in place;
the dropped accessions are then fetched by the next download_sra.py run.

Usage: python scripts/recheck_no_runs.py [--data-dir data]
"""
import argparse, os, re, subprocess, time

ap = argparse.ArgumentParser()
ap.add_argument('--data-dir', default='data')
args = ap.parse_args()
path = os.path.join(args.data_dir, 'sra_no_runs.txt')
values = list(dict.fromkeys(l.strip() for l in open(path) if l.strip()))
keep, false_neg, errors = [], [], []
pause = 0.12 if os.environ.get('NCBI_API_KEY') else 0.4
for i, v in enumerate(values, 1):
    r = subprocess.run(['esearch', '-db', 'sra', '-query', v], capture_output=True, text=True)
    m = re.search(r'<Count>(\d+)</Count>', r.stdout)
    if r.returncode != 0 or not m:
        errors.append(v); keep.append(v)   # unknown: leave as is
    elif int(m.group(1)) > 0:
        false_neg.append(v)
    else:
        keep.append(v)
    if i % 1000 == 0:
        print(f'{i}/{len(values)} checked, false no-runs so far: {len(false_neg)}', flush=True)
    time.sleep(pause)
with open(path + '.tmp', 'w') as f:
    f.writelines(v + '\n' for v in keep)
os.replace(path + '.tmp', path)
with open(os.path.join(args.data_dir, 'sra_no_runs_false.txt'), 'w') as f:
    f.writelines(v + '\n' for v in false_neg)
print(f'checked={len(values)} false_no_runs={len(false_neg)} errors={len(errors)} kept={len(keep)}')
