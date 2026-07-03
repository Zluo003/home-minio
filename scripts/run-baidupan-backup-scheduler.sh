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
  local hour="${hhmm%:*}"
  local minute="${hhmm#*:}"
  local now target
  now="$(date +%s)"
  target="$(date -d "today ${hour}:${minute}:00" +%s 2>/dev/null || date -v "${hour}H" -v "${minute}M" -v 0S +%s)"
  if [[ "$target" -le "$now" ]]; then
    target="$(date -d "tomorrow ${hour}:${minute}:00" +%s 2>/dev/null || date -v+1d -v "${hour}H" -v "${minute}M" -v 0S +%s)"
  fi
  echo $((target - now))
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
