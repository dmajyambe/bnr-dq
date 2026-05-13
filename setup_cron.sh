#!/usr/bin/env bash
# setup_cron.sh — Install BNR DQ cron jobs for the current user.
# Safe to re-run: existing BNR DQ entries are replaced, everything else is kept.
# Usage:
#   bash setup_cron.sh

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BNR_BLOCK="# ── BNR DQ Pipeline ───────────────────────────────────────────────────────────
# Run daily at 10:00
0 10 * * * $DIR/run_pipeline.sh

# ── BNR DQ Dashboard ──────────────────────────────────────────────────────────
# Start on reboot
@reboot $DIR/start_dashboard.sh

# Health-check every 5 minutes: restarts the dashboard if it has crashed
*/5 * * * * $DIR/start_dashboard.sh"

# Remove all existing BNR DQ lines (commands + their comments) then append fresh block.
EXISTING=$(crontab -l 2>/dev/null | grep -v "run_pipeline.sh" \
                                   | grep -v "start_dashboard.sh" \
                                   | grep -v "# ── BNR DQ" \
                                   | grep -v "# Run daily at" \
                                   | grep -v "# Start on reboot" \
                                   | grep -v "# Health-check every") || true

# Collapse multiple blank lines, strip leading/trailing blank lines
EXISTING=$(echo "$EXISTING" | sed '/^[[:space:]]*$/d')

if [ -n "$EXISTING" ]; then
    printf '%s\n\n%s\n' "$EXISTING" "$BNR_BLOCK" | crontab -
else
    printf '%s\n' "$BNR_BLOCK" | crontab -
fi

echo "Cron jobs installed for user: $(whoami)"
echo ""
crontab -l
