#!/usr/bin/env bash
set -euo pipefail

experiment="${1:-normal}"
case "$experiment" in
  normal)
    export LOCK_ENABLED=true BARRIER_ENABLED=true
    export CLIENT_DELAY_MIN_MS=1000 CLIENT_DELAY_MAX_MS=5000
    export CLIENT_1_DELAY_MIN_MS=1000 CLIENT_1_DELAY_MAX_MS=5000
    ;;
  contention)
    export LOCK_ENABLED=true BARRIER_ENABLED=true
    export CLIENT_DELAY_MIN_MS=0 CLIENT_DELAY_MAX_MS=0
    export CLIENT_1_DELAY_MIN_MS=0 CLIENT_1_DELAY_MAX_MS=0
    ;;
  unequal)
    export LOCK_ENABLED=true BARRIER_ENABLED=true
    export CLIENT_DELAY_MIN_MS=3000 CLIENT_DELAY_MAX_MS=5000
    export CLIENT_1_DELAY_MIN_MS=0 CLIENT_1_DELAY_MAX_MS=250
    ;;
  unprotected)
    export LOCK_ENABLED=false BARRIER_ENABLED=true
    export CLIENT_DELAY_MIN_MS=0 CLIENT_DELAY_MAX_MS=0
    export CLIENT_1_DELAY_MIN_MS=0 CLIENT_1_DELAY_MAX_MS=0
    ;;
  *)
    echo "experimento inválido: $experiment" >&2
    echo "use: normal, contention, unequal ou unprotected" >&2
    exit 2
    ;;
esac

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
result_dir="results/${experiment}-${run_id}"
mkdir -p "$result_dir"

cleanup() {
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up --build --detach

status=0
for client in client-1 client-2 client-3 client-4 client-5; do
  container_id="$(docker compose ps --all --quiet "$client")"
  client_status="$(docker wait "$container_id")"
  if [[ "$client_status" -ne 0 ]]; then
    status="$client_status"
  fi
done

docker compose stop observer cluster-1 cluster-2 cluster-3 cluster-4 cluster-5 >/dev/null
docker compose logs --no-color >"$result_dir/all.log"
if ! python3 scripts/summarize.py "$result_dir/all.log" >"$result_dir/summary.json"; then
  cat "$result_dir/summary.json"
  echo "a verificação automática reprovou a execução" >&2
  exit 1
fi
cat "$result_dir/summary.json"

if [[ "$status" -ne 0 ]]; then
  echo "um ou mais clientes terminaram com erro; consulte $result_dir/all.log" >&2
  exit "$status"
fi

echo "evidências salvas em $result_dir"

