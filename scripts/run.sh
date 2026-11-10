#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
SCEN=${1:-normal}; SEED=${2:-1}; N=${N:-5}; REQ=${REQ:-10}; ZK=${ZK:-localhost:2181}
TIMEOUT=${TIMEOUT:-1800}
for value in "$N" "$REQ" "$SEED" "$TIMEOUT"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo 'Parâmetro inteiro positivo inválido'; exit 1; }
done
(( N >= 5 && N <= 100 && REQ >= 10 && REQ <= 50 )) || { echo 'Use N entre 5 e 100 e REQ entre 10 e 50'; exit 1; }
if [[ ${TP2_TIMED:-0} != 1 ]]; then
  export TP2_TIMED=1
  timeout --foreground --kill-after=10s "$TIMEOUT" bash "$0" "$SCEN" "$SEED"
  rc=$?
  (( rc != 124 && rc != 137 )) || echo 'TIMEOUT da execução completa'
  exit "$rc"
fi
UNEQ=${UNEQUAL_REQ:-$((REQ*4))}; (( UNEQ <= 50 )) || UNEQ=50
[[ "$UNEQ" =~ ^[1-9][0-9]*$ ]] && ((UNEQ>=10 && UNEQ<=50)) || { echo 'UNEQUAL_REQ deve estar entre 10 e 50'; exit 1; }
FLAGS=(); OBS_FLAGS=(); TMIN=1000; TMAX=5000; EXPECTED=$((N*REQ))
case "$SCEN" in
 nosync) FLAGS+=(--no-sync); TMIN=0; TMAX=0 ;;
 normal) ;;
 contention) TMIN=0; TMAX=0 ;;
 unequal) EXPECTED=$(((N-1)*REQ+UNEQ)) ;;
 barrier) FLAGS+=(--barrier); OBS_FLAGS+=(--barrier) ;;
 *) echo 'Cenário inválido'; exit 1 ;;
esac
RUN="${SCEN}-n${N}-s${SEED}-$(date +%s)-$$"
LOGDIR="logs/$RUN"; mkdir -p "$LOGDIR" results
JAR=target/tp2-sync.jar
CSV_HEADER='run_id,scenario,seed,nodes,expected,completed,total_s,median_wait_ms,p95_wait_ms,throughput_per_s,max_active,overlaps,barrier_violations,protocol_violations,status,per_client'
if [[ -f results/summary.csv && $(head -1 results/summary.csv) != "$CSV_HEADER" ]]; then
 echo 'CSV antigo incompatível: arquive results/summary.csv antes de executar esta versão'; exit 1
fi
[[ -f "$JAR" ]] || { echo 'Compile com mvn clean package'; exit 1; }
JAVA=(java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$JAR")
PIDS=(); CONTAINERS=()
cleanup() {
 trap - EXIT TERM INT
 for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
 for c in "${CONTAINERS[@]}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
 for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
launch() {
 local name=$1 log=$2; shift 2
 if [[ ${USE_DOCKER:-0} == 1 ]]; then
  name="tp2-$RUN-$name"; CONTAINERS+=("$name")
  docker run --rm --name "$name" --network host -u "$(id -u):$(id -g)" -v "$PWD:/app" -w /app eclipse-temurin:17-jre "${JAVA[@]}" "$@" > "$log" 2>&1 &
 else
  "${JAVA[@]}" "$@" > "$log" 2>&1 &
 fi
 PID=$!; PIDS+=("$PID")
}
{
 printf 'run_id=%s\nscenario=%s\nseed=%s\nnodes=%s\nclients=%s\nrequests=%s\nunequal_requests=%s\nexpected=%s\nthink_min_ms=%s\nthink_max_ms=%s\ncritical_min_ms=200\ncritical_max_ms=1000\nzk=%s\nnamespace=%s\ntimeout_s=%s\nuse_docker=%s\n' "$RUN" "$SCEN" "$SEED" "$N" "$N" "$REQ" "$UNEQ" "$EXPECTED" "$TMIN" "$TMAX" "$ZK" "$RUN" "$TIMEOUT" "${USE_DOCKER:-0}"
 printf 'unequal_client_think_ms=0\ncurator=5.7.1\nzookeeper_image=zookeeper:3.9.3\n'
 if [[ ${USE_DOCKER:-0} == 1 ]]; then docker run --rm eclipse-temurin:17-jre java -version 2>&1; else java -version 2>&1; fi
} > "$LOGDIR/manifest.txt"
echo ">> Execução $RUN esperado=$EXPECTED"
launch observer "$LOGDIR/observer.log" tp2.Observer --port 9000 --nodes "$N" --expected "$EXPECTED" --label "$SCEN" --seed "$SEED" --run-id "$RUN" --csv "$LOGDIR/summary.csv" "${OBS_FLAGS[@]}"
OBS_PID=$PID
sleep 1
NODE_PIDS=()
for i in $(seq 1 "$N"); do
 launch "node-$i" "$LOGDIR/node-$i.log" tp2.SyncNode --id "n$i" --port "$((7000+i))" --zk "$ZK" --ns "$RUN" --observer localhost:9000 --participants "$N" --seed "$SEED" "${FLAGS[@]}"
 NODE_PIDS+=("$PID")
done
sleep 4
START=$(($(date +%s%3N)+3000)); CLIENT_PIDS=()
for i in $(seq 1 "$N"); do
 R=$REQ; LO=$TMIN; HI=$TMAX
 if [[ "$SCEN" == unequal && "$i" == 1 ]]; then R=$UNEQ; LO=0; HI=0; fi
 launch "client-$i" "$LOGDIR/client-$i.log" tp2.Client --id "c$i" --node "localhost:$((7000+i))" --requests "$R" --think-min "$LO" --think-max "$HI" --seed "$SEED" --start-at "$START"
 CLIENT_PIDS+=("$PID")
done
# Falhas de qualquer processo abortam a execução, sem aguardar um observador incompleto.
for p in "${CLIENT_PIDS[@]}"; do
 wait "$p"; rc=$?; ((rc==0)) || { echo "Falha de cliente pid=$p rc=$rc"; exit 4; }
done
for p in "${NODE_PIDS[@]}"; do
 wait "$p"; rc=$?; ((rc==0)) || { echo "Falha de nó pid=$p rc=$rc"; exit 4; }
done
wait "$OBS_PID"; RC=$?
SUMMARY=$(grep ' OBS SUMMARY ' "$LOGDIR/observer.log" | tail -1)
echo "$SUMMARY"
[[ -f "$LOGDIR/summary.csv" ]] || { echo 'Resumo ausente'; exit 4; }
if [[ ! -f results/summary.csv ]]; then head -1 "$LOGDIR/summary.csv" > results/summary.csv; fi
tail -1 "$LOGDIR/summary.csv" >> results/summary.csv
OK=0
if [[ "$SCEN" == nosync ]]; then
 [[ $RC == 2 && "$SUMMARY" =~ overlaps=([1-9][0-9]*) && "$SUMMARY" == *'barrier_violations=0 protocol_violations=0'* && "$SUMMARY" == *"completed=$EXPECTED expected=$EXPECTED "* ]] && OK=1
else
 [[ $RC == 0 && "$SUMMARY" == *'status=OK '* && "$SUMMARY" == *"completed=$EXPECTED expected=$EXPECTED "* ]] && OK=1
fi
((OK==1)) || { echo "FALHOU: resultado inesperado rc=$RC; logs=$LOGDIR"; exit 4; }
echo "PASSOU: $RUN logs=$LOGDIR"
