#!/usr/bin/env bash
# run_pipeline.sh — Daily DQ pipeline runner.
# Invoked by cron. Logs to logs/pipeline_YYYY-MM-DD.log and keeps 30 days.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$DIR/logs"
LOG="$LOG_DIR/pipeline_$(date +%F).log"

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG"
echo "Pipeline started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "========================================" >> "$LOG"

cd "$DIR"
/usr/bin/python3 dq_pipeline_2m.py --load >> "$LOG" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Pipeline finished successfully: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
else
    echo "Pipeline FAILED (exit $EXIT_CODE): $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
fi

# Rotate: delete log files older than 30 days
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete

exit $EXIT_CODE
