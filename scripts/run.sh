#!/usr/bin/env bash
# Uso: [N=5] [REQ=10] [ZK=localhost:2181] scripts/run.sh <cenario> <semente>
# Cenários: nosync | normal | contention | unequal | barrier
set -uo pipefail
cd "$(dirname "$0")/.."

SCEN=${1:-normal}; SEED=${2:-1}
N=${N:-5}; REQ=${REQ:-10}; ZK=${ZK:-localhost:2181}
JAR=target/tp2-sync.jar
OBS_PORT=9000; BASE=7000
RUN="${SCEN}-n${N}-s${SEED}-$(date +%s)"
LOGDIR="logs/${RUN}"; mkdir -p "$LOGDIR" results
JAVA="java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp $JAR"

SYNC_FLAG=""; BARRIER_FLAG=""; TMIN=1000; TMAX=5000; UNEQ=$((REQ*4))
EXPECTED=$((N*REQ))
case "$SCEN" in
  nosync)     SYNC_FLAG="--no-sync"; TMIN=0; TMAX=0 ;;   # prova que a instrumentação detecta sobreposição
  normal)     ;;
  contention) TMIN=0; TMAX=0 ;;                            # sem espera entre pedidos
  unequal)    EXPECTED=$(( (N-1)*REQ + UNEQ )) ;;          # c1 pede mais e sem pausa
  barrier)    BARRIER_FLAG="--barrier" ;;                  # mutex + barreira dupla por rodada
  *) echo "Cenário inválido: $SCEN"; exit 1 ;;
esac

echo ">> Execução $RUN (N=$N, REQ=$REQ, seed=$SEED, esperado=$EXPECTED)"

timeout "${TIMEOUT:-1800}" $JAVA tp2.Observer --port $OBS_PORT --nodes $N --expected $EXPECTED \
  --label "$SCEN" --seed "$SEED" --csv results/summary.csv $BARRIER_FLAG > "$LOGDIR/observer.log" 2>&1 &
OBS_PID=$!
sleep 1

NODE_PIDS=()
for i in $(seq 1 $N); do
  $JAVA tp2.SyncNode --id n$i --port $((BASE+i)) --zk "$ZK" --ns "$RUN" \
    --observer localhost:$OBS_PORT --participants $N --seed "$SEED" $SYNC_FLAG $BARRIER_FLAG \
    > "$LOGDIR/node-$i.log" 2>&1 &
  NODE_PIDS+=($!)
done
sleep 4

START=$(( $(date +%s%3N) + 3000 ))   # todos os clientes disparam juntos
CLIENT_PIDS=()
for i in $(seq 1 $N); do
  R=$REQ; TMIN_I=$TMIN; TMAX_I=$TMAX
  if [ "$SCEN" = "unequal" ] && [ "$i" = "1" ]; then R=$UNEQ; TMIN_I=0; TMAX_I=0; fi
  $JAVA tp2.Client --id c$i --node localhost:$((BASE+i)) --requests $R \
    --think-min $TMIN_I --think-max $TMAX_I --seed "$SEED" --start-at $START \
    > "$LOGDIR/client-$i.log" 2>&1 &
  CLIENT_PIDS+=($!)
done

for p in "${CLIENT_PIDS[@]}"; do wait "$p"; done
wait $OBS_PID; RC=$?
for p in "${NODE_PIDS[@]}"; do wait "$p" 2>/dev/null; done

grep SUMMARY "$LOGDIR/observer.log" || echo "Observador não gerou resumo (rc=$RC); veja $LOGDIR"
echo ">> Logs em $LOGDIR | resumo acumulado em results/summary.csv | rc=$RC (0 ok, 2 violação, 3 incompleto)"
exit $RC
