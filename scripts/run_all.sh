#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
failed=0; passed=0
run() {
 if bash scripts/run.sh "$@"; then passed=$((passed+1)); else
  rc=$?; failed=$((failed+1)); echo "FALHOU: cenário=$1 semente=$2 N=${N:-5} rc=$rc"
 fi
}
for s in 1 2 3; do
 for c in nosync normal contention unequal barrier; do run "$c" "$s"; done
done
for s in 1 2 3; do N=10 run contention "$s"; done
echo "Bateria: aprovadas=$passed falhas=$failed. Resultados em results/summary.csv"
((failed==0))
