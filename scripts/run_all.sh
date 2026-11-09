#!/usr/bin/env bash
# Bateria completa: 3 repetições (sementes 1..3) de cada cenário, com 5 nós; escala com 10 nós.
cd "$(dirname "$0")/.."
for s in 1 2 3; do
  for c in nosync normal contention unequal barrier; do
    scripts/run.sh "$c" "$s"
  done
done
# Comparação 5 vs 10 processos (mesma carga e mesma duração de seção crítica)
for s in 1 2 3; do N=10 scripts/run.sh contention "$s"; done
echo "Resultados em results/summary.csv"
