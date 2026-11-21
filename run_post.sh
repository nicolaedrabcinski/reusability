#!/usr/bin/env bash
# Waits for run_full.sh to finish, then runs create -> analyze -> visualize notebooks and builds RUN_REPORT.md.
# Executed notebooks are written to data/executed/ (the tracked notebooks are not modified).
# Usage: nohup bash run_post.sh > logs/run_post_$(date +%Y%m%d).log 2>&1 &
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
PY="$PWD/.venv/bin/python"
LOG="$PWD/RUN_LOG.md"
note() { echo "- $(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"; echo "[$(date '+%T')] $*"; }

until grep -q "ALL NEXTFLOW STAGES DONE\|FAILED after 5 attempts" "$LOG"; do sleep 300; done
if grep -q "FAILED after 5 attempts" "$LOG"; then
  note "post: a Nextflow stage FAILED, notebooks skipped; building partial report"
  "$PY" scripts/make_run_report.py; exit 1
fi

mkdir -p data/executed
run_nb() {   # run_nb NAME critical(1/0)
  note "NOTEBOOK $1: start"
  ( cd notebooks && "$PY" -m jupyter nbconvert --to notebook --execute "$1.ipynb" \
      --output-dir ../data/executed --ExecutePreprocessor.kernel_name=reusability-venv \
      --ExecutePreprocessor.timeout=-1 ) > "logs/notebooks_$1.log" 2>&1
  rc=$?
  if [ $rc -eq 0 ]; then note "NOTEBOOK $1: OK"; else note "NOTEBOOK $1: FAILED rc=$rc (see logs/notebooks_$1.log)"; fi
  return $rc
}
run_nb create  && run_nb analyze && run_nb visualize
"$PY" scripts/make_run_report.py && note "RUN_REPORT.md written"
