#!/usr/bin/env bash
# start_dashboard.sh — Start the BNR DQ dashboard via gunicorn if not running.
# Called at @reboot and every 5 minutes by cron to self-heal after crashes.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$DIR/logs/dashboard.log"
PIDFILE="$DIR/logs/dashboard.pid"

mkdir -p "$DIR/logs"

# Primary guard: check by process name to catch processes not tracked by pidfile
if pgrep -f "gunicorn.*dq_dashboard_dash" > /dev/null 2>&1; then
    exit 0   # already running
fi

rm -f "$PIDFILE"
cd "$DIR"

# 1 worker keeps in-memory state (history, gen_procs) consistent across requests.
# 4 threads handle concurrent browser connections without spawning extra processes.
nohup gunicorn dq_dashboard_dash:server \
    --bind 0.0.0.0:8050 \
    --workers 1 \
    --threads 4 \
    --timeout 120 \
    --access-logfile "$DIR/logs/dashboard_access.log" \
    --error-logfile  "$DIR/logs/dashboard.log" \
    --capture-output \
    --daemon \
    --pid "$PIDFILE" \
    >> "$DIR/logs/dashboard.log" 2>&1

echo "Dashboard started at $(date '+%Y-%m-%d %H:%M:%S') — PID $(cat $PIDFILE 2>/dev/null)" \
    >> "$DIR/logs/dashboard.log"
