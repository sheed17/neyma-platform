#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
MARKET_FILE="$ROOT_DIR/config/markets/priority_markets.txt"
LOG_DIR="$ROOT_DIR/output/launchd"
LOG_FILE="$LOG_DIR/market_batch_$(date '+%Y%m%d_%H%M%S').log"
NOTIFY_SCRIPT="$ROOT_DIR/scripts/macos_notify.py"

mkdir -p "$LOG_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python interpreter at $PYTHON_BIN" >&2
  exit 1
fi

if [[ ! -f "$MARKET_FILE" ]]; then
  echo "Missing market file at $MARKET_FILE" >&2
  exit 1
fi

batch_status=0

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting scheduled market batch"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Repo: $ROOT_DIR"
  cd "$ROOT_DIR"
  "$PYTHON_BIN" scripts/run_market_training_batch.py \
    --market-file "$MARKET_FILE" \
    --scan-limit 20 \
    --diagnostics-per-market 10 \
    --selection-strategy stratified_rank \
    --group-by market \
    --holdout-market 'Phoenix, AZ'
} >>"$LOG_FILE" 2>&1 || batch_status=$?

if [[ -x "$PYTHON_BIN" && -f "$NOTIFY_SCRIPT" ]]; then
  if [[ "$batch_status" -eq 0 ]]; then
    "$PYTHON_BIN" "$NOTIFY_SCRIPT" \
      --title "Neyma batch complete" \
      --message "Scheduled market batch finished. Check output/training_batches for the latest run."
  else
    "$PYTHON_BIN" "$NOTIFY_SCRIPT" \
      --title "Neyma batch failed" \
      --message "Scheduled market batch exited with code $batch_status. Check $(basename "$LOG_FILE")."
  fi
fi

exit "$batch_status"
