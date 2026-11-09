# TP2 — Sincronização distribuída com ZooKeeper + Apache Curator (B2)

BCC362 — Sistemas Distribuídos. Opção **B2**: `InterProcessMutex` sobre o recurso R, mais a extensão **`DistributedDoubleBarrier`**.

## Versões fixadas
Java 17 · Apache Curator 5.7.1 · ZooKeeper 3.9.3 (Docker) · Maven 3.9+ · slf4j-simple 2.0.13

## Arquitetura

```
 c1..c5 (Client) --TCP--> n1..n5 (SyncNode) --Curator--> ZooKeeper (znodes)
                               |
                               +--TCP síncrono--> Observer (BEGIN/END/ARRIVE/PASS)
```
- **Client**: conhece apenas um nó. Envia `REQUEST <request_id>` (`request_id = cN-seq`), espera `COMMITTED`, dorme 1–5 s.
- **SyncNode** (Cluster Sync): fila FIFO local de pedidos → `acquire` → seção crítica simulada (sleep 0,2–1 s) → `release` → `COMMITTED`. A sincronização (`InterProcessMutex`) fica separada da seção crítica.
- **Observer**: recebe BEGIN/END de todos os nós e mantém o contador de acessos ativos a R. Só registra e acusa; **não autoriza nem bloqueia** (responde `OK` logo após registrar). Também verifica a barreira dupla.

Sequência por pedido com barreira: `barrier.enter → mutex.acquire → R → mutex.release → COMMITTED → barrier.leave`. O mutex nunca é segurado enquanto se espera na barreira (evita deadlock). Cada rodada usa um caminho próprio (`/barriers/round-<r>`), identificando a fase.

## Como executar
```bash
docker compose up -d                  # ZooKeeper
mvn -q clean package                  # gera target/tp2-sync.jar
scripts/run.sh normal 1               # um cenário, semente 1
scripts/run_all.sh                    # bateria completa (3 repetições + 10 nós)
```
Variáveis: `N` (nós = clientes, padrão 5), `REQ` (acessos por cliente, padrão 10; o enunciado pede 10–50), `ZK`, `TIMEOUT`.

| Cenário | O que faz |
|---|---|
| `nosync` | mutex **desabilitado**, sem pausa: o observador deve acusar `VIOLATION_OVERLAP` |
| `normal` | mutex habilitado, pausas de 1–5 s |
| `contention` | mutex habilitado, sem pausa entre pedidos, disparo conjunto (`--start-at`) |
| `unequal` | c1 faz 4×REQ pedidos sem pausa; mostra distribuição e espera dos demais |
| `barrier` | mutex + barreira dupla por rodada (todos os clientes com o mesmo nº de pedidos) |
| `N=10 … contention` | comparação 5 vs 10 processos |

A barreira exige que todos os participantes cheguem em cada rodada; por isso só é usada com cargas iguais.

## Verificações automáticas (Observer)
- **Exclusão mútua**: `max_active` deve ser 1 e `overlaps = 0` (no `nosync` deve ser > 1).
- **Barreira de entrada**: ninguém passa/inicia a rodada *r* antes de todos terem chegado.
- **Barreira de saída**: ninguém chega à rodada *r+1* antes de todos terminarem a rodada *r*.
- **Conclusão**: `completed == expected`.
- Códigos de saída: `0` ok · `2` violação · `3` incompleto.

As mensagens do observador são síncronas (o nó espera o `OK`), então a ordem causal dos eventos é preservada sem comparar relógios físicos. A espera de aquisição é medida no próprio nó com `System.nanoTime()`.

Saída: `logs/<execução>/` (logs de clientes, nós e observador; os nós registram REQUEST, ACQUIRE, ENTER, EXIT, RELEASE, COMMITTED com `request_id`, `holder_znode` e `queue_size`) e `results/summary.csv` (pedidos concluídos, tempo total, espera mediana/p95, vazão, acessos por cliente, semente).

## O que está por trás do `acquire()` (para a apresentação)
1. O nó cria um znode **efêmero sequencial** em `/locks/R` (`_c_<uuid>-lock-000000000N`).
2. Lista os filhos; se o seu é o de menor sequência, adquire.
3. Caso contrário, coloca um *watch* **apenas no predecessor** (evita o efeito "herd": só um nó é acordado a cada liberação).
4. Ao ser notificado, **reavalia** a condição (relista os filhos) antes de entrar.
5. `release()` remove o znode, o que notifica o sucessor.
O `InterProcessMutex` é reentrante por thread e cooperativo: só vale se todos usarem o mesmo caminho. A barreira dupla (`DistributedDoubleBarrier`) usa um znode efêmero por participante: `enter()` libera quando `memberQty` participantes chegaram; `leave()` libera quando todos removeram seus znodes.

## Limitações assumidas (conforme o enunciado e a análise do grupo)
- Sem falhas, quedas ou perda de mensagens; ZooKeeper com 1 servidor. Não tratamos perda de sessão nem *fencing tokens* (apenas logamos `ZK_STATE`).
- Ordem de atendimento = ordem de criação dos znodes no ZooKeeper, **não** a ordem de Lamport dos clientes.
- Timeouts de 120 s no mutex e na barreira; se um participante não chegar, a execução aborta.
- Mensagens por entrada **não** são instrumentadas (uma chamada Curator gera várias requisições ao ZooKeeper); o custo é discutido teoricamente e comparado via tempo/vazão. Mensagens do observador devem ser contadas à parte.
- Ausência de starvation em execução curta é observação, não prova.
