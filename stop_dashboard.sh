#!/usr/bin/env bash
# stop_dashboard.sh — Gracefully stop the gunicorn dashboard process.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$DIR/logs/dashboard.pid"
STOPPED=false

if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "Dashboard stopped (PID $PID)."
        STOPPED=true
    else
        echo "Stale pidfile — process $PID not running."
    fi
    rm -f "$PIDFILE"
fi

# Always also check pgrep — a stale/missing pidfile must not mask a dashboard
# that's actually still running under a different PID (matches both the
# current target and the legacy one, in case a process from before the
# dashboard.app cutover is still around).
PIDS=$(pgrep -f "gunicorn.*(dashboard\.app|dq_dashboard_dash)" 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo "$PIDS" | xargs kill
    echo "Dashboard stopped (PIDs: $PIDS)."
    STOPPED=true
fi

if [ "$STOPPED" = false ]; then
    echo "Dashboard is not running."
fi
