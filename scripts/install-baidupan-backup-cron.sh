#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRON_TIME="${1:-${BAIDUPAN_CRON_SCHEDULE:-35 3 * * *}}"
LOG_FILE="${2:-$ROOT_DIR/backup/logs/baidupan-cron.log}"
COMMAND="cd $ROOT_DIR && ./scripts/backup-to-baidupan.sh >> $LOG_FILE 2>&1"
ENTRY="$CRON_TIME $COMMAND"

mkdir -p "$(dirname "$LOG_FILE")"

tmp="$(mktemp)"
crontab -l 2>/dev/null | grep -vF "./scripts/backup-to-baidupan.sh" >"$tmp" || true
printf "%s\n" "$ENTRY" >>"$tmp"
crontab "$tmp"
rm -f "$tmp"

echo "Installed cron:"
echo "$ENTRY"
