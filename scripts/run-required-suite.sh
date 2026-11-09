#!/usr/bin/env bash
set -euo pipefail

for repetition in 1 2 3; do
  echo "carga normal: repetição $repetition/3"
  scripts/run-experiment.sh normal
done

for repetition in 1 2 3; do
  echo "alta contenção: repetição $repetition/3"
  scripts/run-experiment.sh contention
done

echo "execução de controle sem mutex"
scripts/run-experiment.sh unprotected

