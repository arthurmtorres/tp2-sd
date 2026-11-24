# TP2 — Sincronização distribuída com ZooKeeper + Apache Curator (B2)

BCC362 — Sistemas Distribuídos. Opção **B2**: `InterProcessMutex` sobre o recurso R, mais a extensão **`DistributedDoubleBarrier`**.

## Versões fixadas
Java 17 · Apache Curator 5.7.1 · ZooKeeper 3.9.3 (Docker, **ensemble de 3 servidores**) · Maven 3.9+ · slf4j-simple 2.0.13

## Arquitetura

```
 c1..c5 (Client) --TCP--> n1..n5 (SyncNode) --Curator--> Ensemble ZooKeeper (znodes)
                               |                              zoo1:2181  zoo2:2182  zoo3:2183
                               |                              (1 líder + 2 seguidores, quórum = 2)
                               +--TCP síncrono--> Observer (BEGIN/END/ARRIVE/PASS)
                                                     |
                                                     +--HTTP--> Painel web (localhost:8080)
```
- **Client**: conhece apenas um nó. Envia `REQUEST <request_id>` (`request_id = cN-seq`), espera `COMMITTED`, dorme 1–5 s.
- **SyncNode** (Cluster Sync): fila FIFO local de pedidos → `acquire` → seção crítica simulada (sleep 0,2–1 s) → `release` → `COMMITTED`. A sincronização (`InterProcessMutex`) fica separada da seção crítica.
- **Observer**: recebe BEGIN/END de todos os nós e mantém o contador de acessos ativos a R. Só registra e acusa; **não autoriza nem bloqueia** (responde `OK` logo após registrar). Também verifica a barreira dupla e serve o painel web (somente leitura).

Sequência por pedido com barreira: `barrier.enter → mutex.acquire → R → mutex.release → COMMITTED → barrier.leave`. O mutex nunca é segurado enquanto se espera na barreira (evita deadlock). Cada rodada usa um caminho próprio (`/barriers/round-<r>`), identificando a fase.

## Como executar
```bash
docker compose up -d                 
mvn -q clean package                 
export ZK=localhost:2181,localhost:2182,localhost:2183
bash scripts/run.sh normal 1          # um cenário, semente 1
bash scripts/run_all.sh               # bateria completa (3 repetições + 10 nós)
```
Os servidores levam cerca de 15–20 s para eleger o líder. Antes de rodar, confira (deve haver 1 `leader` e 2 `follower`):
```bash
for p in 2181 2182 2183; do echo mntr | nc localhost $p | grep zk_server_state; done
```
Com JDK 21 e Maven antigo (erro `Source option 5`), compile com `mvn -q clean package -Dmaven.compiler.source=17 -Dmaven.compiler.target=17`.
Sem JDK 17 no sistema? Use Docker para tudo:
```bash
docker run --rm -u $(id -u):$(id -g) -v "$PWD":/app -w /app -e MAVEN_CONFIG=/app/.m2 maven:3.9-eclipse-temurin-17 mvn -q -Dmaven.repo.local=/app/.m2/repository clean package
USE_DOCKER=1 scripts/run.sh normal 1
```
Variáveis: `N` (nós = clientes, padrão 5), `REQ` (acessos por cliente, padrão 10; o enunciado pede 10–50), `ZK` (lista de servidores do ensemble), `TIMEOUT`, `OBS_WEB_PORT` (porta do painel, padrão 8080; `0` desliga) e `OBS_HOLD_S` (segundos que o painel continua no ar após o fim; padrão 0).

| Cenário | O que faz |
|---|---|
| `nosync` | mutex **desabilitado**, sem pausa: o observador deve acusar `VIOLATION_OVERLAP` |
| `normal` | mutex habilitado, pausas de 1–5 s |
| `contention` | mutex habilitado, sem pausa entre pedidos, disparo conjunto (`--start-at`) |
| `unequal` | c1 faz 4×REQ pedidos sem pausa; mostra distribuição e espera dos demais |
| `barrier` | mutex + barreira dupla por rodada (todos os clientes com o mesmo nº de pedidos) |
| `N=10 … contention` | comparação 5 vs 10 processos |

A barreira exige que todos os participantes cheguem em cada rodada; por isso só é usada com cargas iguais.

## Ensemble ZooKeeper (multi-servidor)
`docker-compose.yml` sobe 3 servidores (`tp2-zk1`, `tp2-zk2`, `tp2-zk3`), acessíveis do host em `localhost:2181`, `2182` e `2183`. Entre si usam a rede do compose (`zoo1`, `zoo2`, `zoo3`; portas 2888 para seguidor→líder e 3888 para eleição). Um servidor é eleito **líder** e os outros são **seguidores**; o serviço funciona enquanto a maioria (**quórum = 2 de 3**) estiver ativa. Com 3 servidores tolera-se a queda de 1; com 5, a de 2. Número ímpar evita desperdício: 4 servidores também toleram só 1 falha.

Todos os nós Curator recebem a lista completa em `ZK` e, se o servidor ao qual estão ligados cair, reconectam a outro mantendo a sessão. Dois cuidados:
- `SyncNode` usa `.ensembleTracker(false)`. Sem isso, o Curator substitui a lista informada pela que os servidores anunciam (`zoo1`, `zoo2`, `zoo3`), nomes que só existem dentro da rede do Docker; o host não os resolve e os nós não reconectam.
- Os 3 servidores rodam como containers **na mesma máquina**. Isso demonstra o protocolo de replicação e eleição, não tolerância a falha de hardware.

**Teste de falha (manual).** Com um teste em andamento (ex.: `N=3 REQ=20 OBS_HOLD_S=30 bash scripts/run.sh normal 1`), pare o servidor líder indicado no painel (`docker stop tp2-zk3`, por exemplo) e religue-o depois (`docker start tp2-zk3`). Esperado: o painel marca o servidor como "fora do ar", outro vira líder, os nós registram `ZK_STATE SUSPENDED` e `RECONNECTED` (`grep -h ZK_STATE logs/<execução>/node-*.log`) e o teste termina com `status=OK`, `overlaps=0` e todos os acessos concluídos. Não pare dois servidores ao mesmo tempo: sem quórum o serviço deixa de aceitar operações.

## Painel web (Observer)
Ao rodar um cenário, o Observer serve em `http://localhost:8080` (porta alterável com `OBS_WEB_PORT`) uma página somente leitura, atualizada a cada ~0,7 s, com:
- os servidores do ensemble (líder/seguidor/fora do ar, nº de znodes e conexões), consultados a cada 1 s pelo comando `mntr`, e se há quórum;
- o estado do recurso R (livre, em uso por qual nó/pedido, ou sobreposição);
- acessos concluídos, `max_active`, sobreposições e violações da barreira;
- o estado de cada nó, os acessos por cliente e o log dos eventos recebidos.

O painel só existe enquanto o Observer roda. Use `OBS_HOLD_S=N` para mantê-lo no ar por N segundos após o fim. **Não interrompa com Ctrl+C durante esse tempo:** o Observer pode continuar vivo, ocupando as portas 9000 e 8080, e a execução seguinte falha (`rc=1`, "Observador não gerou resumo"). Se acontecer, rode `pkill -f tp2-sync.jar`. Se a porta 8080 estiver ocupada, o teste roda normalmente sem painel (`DASHBOARD_ERROR` no log do observador). O painel mostra o que o Observer recebe; a fila de znodes do mutex continua nos logs dos nós.

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
- A avaliação principal assume execução sem falhas, quedas ou perda de mensagens. O ZooKeeper é um ensemble de 3 servidores em containers na mesma máquina e tolera a queda de 1 por tempo curto. Não tratamos **perda de sessão** (se um nó ficar sem servidor além do tempo da sessão, em torno de 60 s, o estado vai a `LOST` e a execução trava) nem *fencing tokens* (apenas logamos `ZK_STATE`).
- Ordem de atendimento = ordem de criação dos znodes no ZooKeeper, **não** a ordem de Lamport dos clientes.
- Timeouts de 120 s no mutex e na barreira; se um participante não chegar, a execução aborta.
- Mensagens por entrada **não** são instrumentadas (uma chamada Curator gera várias requisições ao ZooKeeper); o custo é discutido teoricamente e comparado via tempo/vazão. Mensagens do observador devem ser contadas à parte.
- Ausência de starvation em execução curta é observação, não prova.