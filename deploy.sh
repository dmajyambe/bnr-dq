#!/usr/bin/env bash
# deploy.sh — Pull latest code and restart the dashboard.
# Usage:
#   bash deploy.sh           # pull from git + restart
#   bash deploy.sh --no-git  # restart only (skip git pull)

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

NO_GIT=false
for arg in "$@"; do
    [ "$arg" = "--no-git" ] && NO_GIT=true
done

echo "=== BNR DQ Deploy — $(date '+%Y-%m-%d %H:%M:%S') ==="

# pull latest code
if [ "$NO_GIT" = false ]; then
    if git -C "$DIR" rev-parse --is-inside-work-tree > /dev/null 2>&1; then
        echo "Pulling latest code..."
        git -C "$DIR" pull --ff-only
    else
        echo "No git repo found — skipping pull. Use 'git init' to set one up."
    fi
fi

#install/update dependencies
echo "Checking dependencies..."
pip3 install -q -r requirements.txt

#restart the dashboard
echo "Restarting dashboard..."
bash "$DIR/stop_dashboard.sh"
sleep 2
bash "$DIR/start_dashboard.sh"
sleep 3

# health check
if pgrep -f "gunicorn.*dashboard.app" > /dev/null 2>&1; then
    echo "Dashboard is up — http://$(hostname -I | awk '{print $1}'):8050"
else
    echo "ERROR: Dashboard did not start. Check logs/dashboard.log"
    exit 1
fi

echo "=== Deploy complete ==="
