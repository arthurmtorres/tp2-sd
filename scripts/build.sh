#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${USE_DOCKER:-0}" == "1" ]]; then
  docker run --rm -u "$(id -u):$(id -g)" \
    -v "$PWD:/app" -w /app -e MAVEN_CONFIG=/app/.m2 \
    maven:3.9.9-eclipse-temurin-17 \
    mvn -Dmaven.repo.local=/app/.m2/repository clean package
else
  mvn clean package
fi
python3 scripts/test_scripts.py
