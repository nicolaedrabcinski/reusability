#!/usr/bin/env bash
# Full run supervisor: stages run one after another (so NCBI is never hit by two stages at once),
# every Nextflow stage is restarted with -resume up to 5 times. Never prints NCBI_API_KEY.
# Usage: nohup bash run_full.sh > logs/run_$(date +%Y%m%d).log 2>&1 &
set -u
cd "$(dirname "${BASH_SOURCE[0]}")"
PY="$PWD/.venv/bin/python"
LOG="$PWD/RUN_LOG.md"
STAMP() { date '+%Y-%m-%d %H:%M:%S'; }
note() { echo "- $(STAMP) $*" >> "$LOG"; echo "[$(STAMP)] $*"; }

# progress line every 30 min
( while true; do
    sleep 1800
    n=$(find work -name '*.matrix.csv' 2>/dev/null | wc -l)
    sz=$(du -sh data 2>/dev/null | cut -f1)
    err=$(grep -ciE 'error|retry|failed' "$PWD/logs/run_$(date +%Y%m%d).log" 2>/dev/null)
    echo "- $(STAMP) PROGRESS: archives parsed (matrix files in work/)=$n, data/=$sz, error/retry lines in log=$err" >> "$LOG"
  done ) &
PROG=$!
trap 'kill $PROG 2>/dev/null' EXIT

stage() {   # stage NAME flags...
  local name="$1"; shift
  note "STAGE $name: start"
  for attempt in 1 2 3 4 5; do
    nextflow run main.nf --parallel_downloads 4 "$@" -resume -ansi-log false
    rc=$?
    if [ $rc -eq 0 ]; then note "STAGE $name: done (attempt $attempt)"; return 0; fi
    note "STAGE $name: exit $rc on attempt $attempt, restarting with -resume in 60 s"
    sleep 60
  done
  note "STAGE $name: FAILED after 5 attempts. Continue with: nextflow run main.nf --parallel_downloads 4 $* -resume"
  return 1
}

stage "a-publications" --skip_geo --skip_sra --skip_gold || exit 1
stage "b-geo"          --skip_sra --skip_gold            || exit 1
stage "c-sra"          --skip_geo --skip_gold            || exit 1
stage "d-gold"         --skip_geo --skip_sra             || exit 1
note "ALL NEXTFLOW STAGES DONE"
