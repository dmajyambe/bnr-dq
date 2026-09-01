#!/usr/bin/env bash
# run_monthly.sh — Monthly DQ detection + immediate resolution run.
#Invoced by a cronjob on the server.  Logs to logs/monthly_YYYY-MM-DD.log.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$DIR/logs"
LOG="$LOG_DIR/monthly_$(date +%F).log"
PYTHON=/usr/bin/python3

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG"
echo "Monthly pipeline started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "========================================" >> "$LOG"

cd "$DIR"

# ── Stage 1: full-table detection (all dimensions, all tables) ────────────────
echo "Stage 1 (detection): full-table scan …" >> "$LOG"
$PYTHON -m jobs.monthly_detection --schema dqp >> "$LOG" 2>&1
DETECT_CODE=$?

if [ $DETECT_CODE -ne 0 ]; then
    echo "Stage 1 FAILED (exit $DETECT_CODE): $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
    echo "Monthly pipeline aborted — resolution not run." >> "$LOG"
    exit $DETECT_CODE
fi

echo "Stage 1 finished: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"

# Restart dashboard so users see fresh issue counts immediately
bash "$DIR/stop_dashboard.sh"  >> "$LOG" 2>&1
sleep 2
bash "$DIR/start_dashboard.sh" >> "$LOG" 2>&1
echo "Dashboard restarted after detection: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"

# ── Stage 2: resolution scan (re-checks all open issues, pre-computes ZIPs) ──
echo "Stage 2 (resolution): daily scanner run …" >> "$LOG"
$PYTHON -m jobs.resolution_scan --schema dqp >> "$LOG" 2>&1
RESOLUTION_CODE=$?

if [ $RESOLUTION_CODE -eq 0 ]; then
    echo "Stage 2 finished: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
else
    echo "Stage 2 FAILED (exit $RESOLUTION_CODE) — open issues left unscanned." >> "$LOG"
fi

echo "========================================" >> "$LOG"
echo "Monthly pipeline done: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "========================================" >> "$LOG"

# Rotate: delete monthly logs older than 90 days
find "$LOG_DIR" -name "monthly_*.log" -mtime +90 -delete

exit 0
