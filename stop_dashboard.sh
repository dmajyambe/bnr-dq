#!/usr/bin/env bash
# stop_dashboard.sh — Gracefully stop the gunicorn dashboard process.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$DIR/logs/dashboard.pid"

if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "Dashboard stopped (PID $PID)."
        rm -f "$PIDFILE"
    else
        echo "Stale pidfile — process $PID not running."
        rm -f "$PIDFILE"
    fi
else
    # Fall back to pgrep if pidfile missing
    PIDS=$(pgrep -f "gunicorn.*dq_dashboard_dash" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo "$PIDS" | xargs kill
        echo "Dashboard stopped (PIDs: $PIDS)."
    else
        echo "Dashboard is not running."
    fi
fi
