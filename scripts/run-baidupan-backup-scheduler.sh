#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo ".env not found. Scheduler waiting."
fi

parse_daily_time() {
  local schedule="${BAIDUPAN_CRON_SCHEDULE:-35 3 * * *}"
  local minute hour rest
  read -r minute hour rest <<<"$schedule"
  if [[ "$minute" =~ ^[0-9]{1,2}$ && "$hour" =~ ^[0-9]{1,2}$ && "$minute" -ge 0 && "$minute" -le 59 && "$hour" -ge 0 && "$hour" -le 23 ]]; then
    printf "%02d:%02d" "$hour" "$minute"
    return
  fi
  echo "03:35"
}

seconds_until_next_run() {
  local hhmm="$1"
  node ./scripts/next-daily-run-delay.mjs "$hhmm"
}

while true; do
  set -a
  source .env 2>/dev/null || true
  set +a

  run_time="$(parse_daily_time)"
  wait_seconds="$(seconds_until_next_run "$run_time")"
  echo "Next Baidu Netdisk backup at $run_time, waiting ${wait_seconds}s."
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
