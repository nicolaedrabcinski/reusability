#!/usr/bin/env bash
# Replaces run_post.sh. Waits for run_full.sh, then:
# 1) re-checks sra_no_runs.txt, 2) re-fetches failed/false-negative accessions,
# 3) rebuilds sra_complete_runs.csv, 4) runs notebooks, 5) writes RUN_REPORT.md.
# Usage: nohup bash run_finish.sh > logs/run_finish_$(date +%Y%m%d).log 2>&1 &
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
PY="$PWD/.venv/bin/python"
LOG="$PWD/RUN_LOG.md"
note() { echo "- $(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"; echo "[$(date '+%T')] $*"; }

until grep -q "ALL NEXTFLOW STAGES DONE\|FAILED after 5 attempts" "$LOG"; do sleep 300; done
if grep -q "FAILED after 5 attempts" "$LOG"; then
  note "finish: a Nextflow stage FAILED, notebooks skipped; building partial report"
  "$PY" scripts/make_run_report.py; exit 1
fi

note "finish: re-checking sra_no_runs.txt with esearch"
"$PY" scripts/recheck_no_runs.py --data-dir data 2>&1 | tail -1 | while read -r l; do note "finish: $l"; done

note "finish: re-fetching failed and false no-runs accessions"
"$PY" scripts/utils/sra/download_sra.py --data-dir data --matrix data/pre_filter_matrix.csv > logs/sra_refetch.log 2>&1
note "finish: refetch rc=$? still failing: $(wc -l < data/sra_failed.txt) (data/sra_failed.txt)"

"$PY" scripts/concat_sra_runs.py data data/sra_complete_runs.csv > logs/sra_concat.log 2>&1 \
  && note "finish: $(tail -1 logs/sra_concat.log)" || { note "finish: concat FAILED (logs/sra_concat.log)"; exit 1; }

mkdir -p data/executed
run_nb() {
  note "NOTEBOOK $1: start"
  ( cd notebooks && "$PY" -m jupyter nbconvert --to notebook --execute "$1.ipynb" \
      --output-dir ../data/executed --ExecutePreprocessor.kernel_name=reusability-venv \
      --ExecutePreprocessor.timeout=-1 ) > "logs/notebooks_$1.log" 2>&1
  rc=$?
  if [ $rc -eq 0 ]; then note "NOTEBOOK $1: OK"; else note "NOTEBOOK $1: FAILED rc=$rc (see logs/notebooks_$1.log)"; fi
  return $rc
}
run_nb create && run_nb analyze && run_nb visualize
"$PY" scripts/make_run_report.py && note "RUN_REPORT.md written"
