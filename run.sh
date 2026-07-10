#!/usr/bin/env bash
# run.sh — full pipeline for the reusability project
# Run from the project root: bash run.sh
# Optional: bash run.sh --limit 1   (test on 1 archive per FTP server)
#           bash run.sh --from 3    (resume from step 3)
#           bash run.sh --force     (ignore checkpoints, re-run everything)

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$PROJECT_ROOT/scripts"
DATA_DIR="$PROJECT_ROOT/data"
PYTHON="$PROJECT_ROOT/.venv/bin/python"

export NCBI_API_KEY="3b793f2849d9b3519ece0c8312b606b36308"

LIMIT_ARG=""
FROM_STEP=1
FORCE=0

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --limit) LIMIT_ARG="--limit $2"; shift 2 ;;
        --from)  FROM_STEP="$2"; shift 2 ;;
        --force) FORCE=1; shift ;;
        *) shift ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── Checkpoint helpers ────────────────────────────────────────────────────────
STATE_DIR="$DATA_DIR/state"
mkdir -p "$STATE_DIR"

step_done() {
    [[ "$FORCE" -eq 0 ]] && [[ "$1" -lt "$FROM_STEP" || -f "$STATE_DIR/step_$1.done" ]]
}
mark_done() { touch "$STATE_DIR/step_$1.done"; }

run_step() {
    local n="$1" label="$2"; shift 2
    if step_done "$n"; then
        log "Step $n/7 — $label (already done, skipping)"
        return
    fi
    log "Step $n/7 — $label"
    "$@"
    log "Step $n/7 — QC"
    "$PYTHON" qc.py --step "$n" --data-dir "$DATA_DIR"
    mark_done "$n"
}
# ─────────────────────────────────────────────────────────────────────────────

log "=== Pipeline start ==="
log "Project root : $PROJECT_ROOT"
log "Data dir     : $DATA_DIR"
[[ "$FORCE" -eq 1 ]] && log "Mode: --force (ignoring checkpoints)"
[[ "$FROM_STEP" -gt 1 ]] && log "Mode: --from $FROM_STEP"

mkdir -p "$DATA_DIR"
cd "$SCRIPTS_DIR"

# ── Step 1: Download & parse PMC publications ─────────────────────────────────
run_step 1 "Download & parse PMC publications" \
    "$PYTHON" download_publications.py --data-dir "$DATA_DIR" $LIMIT_ARG

# ── Step 2: Concatenate per-archive matrices into one CSV ─────────────────────
run_step 2 "Concat pre-filter matrices" \
    "$PYTHON" concat_pre_filter_matrices.py

# ── Step 3: Generate list of XML paths ───────────────────────────────────────
run_step 3 "Generate PMC paths list" \
    "$PYTHON" generate_pmc_paths.py

# ── Step 4: Extract publication dates from XML ────────────────────────────────
run_step 4 "Extract publication dates" \
    "$PYTHON" extract_dates.py

# ── Step 5: Download GEO metadata (samples / series / platforms) ──────────────
run_step 5 "Download GEO reference metadata" \
    "$PYTHON" utils/refs/download_refs.py

# ── Step 6: Download SRA run metadata per accession ──────────────────────────
run_step 6 "Download SRA run metadata" \
    "$PYTHON" utils/sra/download_sra.py

# ── Step 7: Concatenate SRA run CSVs ─────────────────────────────────────────
run_step 7 "Concat SRA runs" \
    "$PYTHON" concat_sra_runs.py

log ""
log "=== Scripts done ==="
log "Now open notebooks/ and run in order:"
log "  1. create.ipynb    →  data/metadata_matrix_raw.csv"
log "  2. analyze.ipynb   →  data/metadata_matrix_filtered.csv + papers.csv"
log "  3. visualize.ipynb →  figures/"
