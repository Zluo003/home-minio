#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo ".env not found. Scheduler waiting."
fi

legacy_schedule_values() {
  local schedule="${BAIDUPAN_CRON_SCHEDULE:-35 3 * * *}"
  local minute hour day_of_month month day_of_week
  read -r minute hour day_of_month month day_of_week <<<"$schedule"
  if [[ "$minute" =~ ^[0-9]{1,2}$ && "$hour" =~ ^[0-9]{1,2}$ && "$minute" -ge 0 && "$minute" -le 59 && "$hour" -ge 0 && "$hour" -le 23 ]]; then
    LEGACY_BACKUP_TIME="$(printf "%02d:%02d" "$hour" "$minute")"
  else
    LEGACY_BACKUP_TIME="03:35"
  fi
  if [[ "$day_of_month" == "*" && "$month" == "*" && "$day_of_week" =~ ^[0-7]$ ]]; then
    [[ "$day_of_week" == "7" ]] && day_of_week="0"
    LEGACY_BACKUP_FREQUENCY="weekly:$day_of_week"
  else
    LEGACY_BACKUP_FREQUENCY="daily"
  fi
}

while true; do
  set -a
  source .env 2>/dev/null || true
  set +a

  legacy_schedule_values
  run_time="${BAIDUPAN_BACKUP_TIME:-$LEGACY_BACKUP_TIME}"
  run_frequency="${BAIDUPAN_BACKUP_FREQUENCY:-$LEGACY_BACKUP_FREQUENCY}"
  run_time_zone="${BAIDUPAN_TIME_ZONE:-${TZ:-UTC}}"
  wait_seconds="$(node ./scripts/next-backup-run-delay.mjs "$run_time" "$run_time_zone" "$run_frequency")"
  echo "Next Baidu Netdisk backup: frequency=$run_frequency time=$run_time timezone=$run_time_zone waiting=${wait_seconds}s."
  sleep "$wait_seconds"

  set -a
  source .env 2>/dev/null || true
  set +a

  if [[ "${BAIDUPAN_BACKUP_ENABLED:-false}" == "true" ]]; then
    echo "Starting scheduled Baidu Netdisk backup."
    ./scripts/backup-to-baidupan.sh || true
  else
    echo "BAIDUPAN_BACKUP_ENABLED is not true, skipping scheduled backup."
  fi
done
