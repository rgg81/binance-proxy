#!/usr/bin/env bash
# Cron entry point: runs the monitor-binance-proxy skill headlessly via the
# Claude Code CLI. Detect-and-alert only — see the skill's Scope section for
# why this deliberately never lets the scheduled run modify code.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LOG_DIR="monitoring/cron-logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date -u +%Y%m%dT%H%M%SZ).log"

claude -p \
  "Run the monitor-binance-proxy skill: execute the health/correctness check on binance-proxy and follow its instructions for interpreting the results and alerting if needed. Do not modify any code or files, and do not restart the proxy — detect and alert only, per the skill's Scope section." \
  --allowedTools "Bash" "Read" "PushNotification" \
  > "$LOG_FILE" 2>&1

echo "Monitor run complete, log: $LOG_FILE"
