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
/usr/bin/python3 dq_pipeline_2m.py --load >> "$LOG" 2>&1
EXIT_CODE=$?

# Write final status (success / failed)
FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
if [ $EXIT_CODE -eq 0 ]; then
    STATUS="success"
    echo "Pipeline finished successfully: $FINISHED_AT" >> "$LOG"
else
    STATUS="failed"
    echo "Pipeline FAILED (exit $EXIT_CODE): $FINISHED_AT" >> "$LOG"
fi

/usr/bin/python3 - << PYEOF
import json
with open("$STATUS_FILE") as f:
    data = json.load(f)
data["status"]      = "$STATUS"
data["finished_at"] = "$FINISHED_AT"
data["exit_code"]   = $EXIT_CODE
json.dump(data, open("$STATUS_FILE", "w"), indent=2)
PYEOF

# Restart dashboard only on success so users always see latest data
if [ $EXIT_CODE -eq 0 ]; then
    echo "Restarting dashboard to load fresh data …" >> "$LOG"
    bash "$DIR/stop_dashboard.sh" >> "$LOG" 2>&1
    sleep 2
    bash "$DIR/start_dashboard.sh" >> "$LOG" 2>&1
    echo "Dashboard restarted: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
else
    echo "Dashboard NOT restarted — keeping current data visible." >> "$LOG"
fi

# Rotate: delete log files older than 30 days
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete

exit $EXIT_CODE
