#!/usr/bin/env bash
# run_profiling.sh — Stand-alone column-profiling pipeline.
# Can be run manually after the monthly detection pipeline completes,
# or scheduled independently (e.g. on the 3rd of each month at 06:00).
#
# Usage:
#   ./run_profiling.sh                   # profile all institutions for today
#   ./run_profiling.sh --date 2026-07-01 # backfill a specific run date
#   ./run_profiling.sh --le-book 010 015 # single-institution test

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$DIR/logs"
LOG="$LOG_DIR/profiling_$(date +%F).log"
PYTHON=/usr/bin/python3

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG"
echo "Profiling pipeline started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "========================================" >> "$LOG"

cd "$DIR"

$PYTHON -m jobs.run_profiling --schema dqp "$@" >> "$LOG" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Profiling pipeline finished: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
else
    echo "Profiling pipeline FAILED (exit $EXIT_CODE): $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
fi

echo "========================================" >> "$LOG"

# Rotate: delete profiling logs older than 90 days
find "$LOG_DIR" -name "profiling_*.log" -mtime +90 -delete

exit $EXIT_CODE
