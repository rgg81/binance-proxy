#!/usr/bin/env bash
# Cron entry point: runs the monitor-binance-proxy skill headlessly via the
# Claude Code CLI. Detect-and-alert only — see the skill's Scope section for
# why this deliberately never lets the scheduled run modify code.
#
# cron runs with a minimal PATH (no ~/.local/bin), unlike an interactive
# shell — the same reason binance_proxy.sh (the uptime watchdog) exports
# PATH explicitly. Without this, `claude` silently fails as "command not
# found" and the entire monitor run is a no-op: no report, no alert, ever
# — which is exactly what happened here for three days straight before
# this fix (see git history). Always test cron scripts with `env -i` or
# equivalent, not just an interactive shell, where PATH hides this class
# of bug completely.
set -euo pipefail
export PATH="$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LOG_DIR="monitoring/cron-logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date -u +%Y%m%dT%H%M%SZ).log"

claude -p \
  "Run the monitor-binance-proxy skill: execute the health/correctness check on binance-proxy and follow its instructions for interpreting the results and alerting if needed. Do not modify any code or files, and do not restart the proxy — detect and alert only, per the skill's Scope section." \
  --allowedTools "Bash" "Read" "PushNotification" \
  > "$LOG_FILE" 2>&1

echo "Monitor run complete, log: $LOG_FILE"
