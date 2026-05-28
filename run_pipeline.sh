#!/usr/bin/env bash
# run_pipeline.sh — Daily DQ pipeline runner.
# Invoked by cron. Logs to logs/pipeline_YYYY-MM-DD.log and keeps 30 days.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$DIR/logs"
LOG="$LOG_DIR/pipeline_$(date +%F).log"
STATUS_FILE="$DIR/pipeline_status.json"

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG"
echo "Pipeline started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "========================================" >> "$LOG"

# Mark as running so the dashboard header shows yellow "Running"
/usr/bin/python3 - << PYEOF
import json, datetime
json.dump({
    "status":     "running",
    "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "finished_at": None,
    "exit_code":  None,
}, open("$STATUS_FILE", "w"), indent=2)
PYEOF

cd "$DIR"

# ── Stage 1: SQL engines + issue tracker + history (low memory, always runs) ──
echo "Stage 1 (--load): SQL engines + issue tracker + history …" >> "$LOG"
/usr/bin/python3 dq_pipeline_2m.py --load >> "$LOG" 2>&1
EXIT_CODE=$?

FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Stage 1 finished: $FINISHED_AT" >> "$LOG"

    # Restart dashboard immediately after Stage 1 so users see fresh scores
    bash "$DIR/stop_dashboard.sh" >> "$LOG" 2>&1
    sleep 2
    bash "$DIR/start_dashboard.sh" >> "$LOG" 2>&1
    echo "Dashboard restarted after Stage 1: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"

    # ── Stage 2: load frames + RI + XLSX reports (heavy, runs after Stage 1) ──
    echo "Stage 2 (--reports): RI checks + per-institution XLSX …" >> "$LOG"
    /usr/bin/python3 dq_pipeline_2m.py --reports >> "$LOG" 2>&1
    REPORTS_CODE=$?

    if [ $REPORTS_CODE -eq 0 ]; then
        echo "Stage 2 finished: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
        STATUS="success"
    else
        echo "Stage 2 FAILED (exit $REPORTS_CODE) — scores/tracker unaffected." >> "$LOG"
        STATUS="success"   # Stage 1 succeeded; reports failure doesn't break monitoring
    fi
else
    STATUS="failed"
    echo "Stage 1 FAILED (exit $EXIT_CODE): $FINISHED_AT" >> "$LOG"
fi

/usr/bin/python3 - << PYEOF
import json
with open("$STATUS_FILE") as f:
    data = json.load(f)
data["status"]      = "$STATUS"
data["finished_at"] = "$(date '+%Y-%m-%d %H:%M:%S')"
data["exit_code"]   = $EXIT_CODE
json.dump(data, open("$STATUS_FILE", "w"), indent=2)
PYEOF

if [ $EXIT_CODE -ne 0 ]; then
    echo "Dashboard NOT restarted — keeping current data visible." >> "$LOG"
fi

# Rotate: delete log files older than 30 days
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete

exit $EXIT_CODE
