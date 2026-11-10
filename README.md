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
Sem JDK 17 no sistema? Use Docker para tudo:
```bash
docker run --rm -u $(id -u):$(id -g) -v "$PWD":/app -w /app -e MAVEN_CONFIG=/app/.m2 maven:3.9-eclipse-temurin-17 mvn -q -Dmaven.repo.local=/app/.m2/repository clean package
USE_DOCKER=1 scripts/run.sh normal 1
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
- **Conclusão**: `completed == expected`, com pedidos únicos e nenhum pedido ativo.
- Códigos do observador: `0` ok · `2` violação · `3` incompleto · `4` erro de CSV. O script retorna `0` quando o resultado corresponde ao cenário, inclusive `nosync`, e falha para resultados inesperados.

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
- Mensagens por entrada **não** são instrumentadas (uma chamada Curator gera várias requisições ao ZooKeeper); o custo é discutido teoricamente e comparado sem usar tempo ou vazão como substitutos de contagem de mensagens. Mensagens do observador devem ser contadas à parte.
- Ausência de starvation em execução curta é observação, não prova.


## Correções desta versão

- O observador valida IDs de nós (`n1` a `nN`), pedidos ativos e concluídos e a sequência das rodadas. Eventos duplicados ou inconsistentes recebem `ERROR` e entram no total de violações de protocolo. `OK` confirma somente o registro, sem autorizar acesso.
- A barreira registra `LEAVE_READY` antes de `leave()` e `LEFT` depois. O observador exige que todos tenham terminado antes de qualquer `LEFT`, inclusive na última rodada.
- Mediana para número par de amostras usa a média dos valores centrais. O CSV usa ponto decimal independentemente da localidade.
- Cada execução salva `manifest.txt` e `summary.csv` em sua pasta de logs. O resumo acumulado inclui `run_id`, que identifica o manifesto.
- O script valida pedidos entre 10 e 50. `UNEQUAL_REQ` configura o cliente mais ativo, limitado a 50. O padrão continua sendo quatro vezes `REQ`, limitado a 50.
- `TIMEOUT` envolve toda a execução. Falhas de clientes e nós são consideradas. A limpeza encerra os processos iniciados. A bateria retorna falha se algum cenário não corresponder ao esperado.
- O teste `nosync` passa somente com pedidos concluídos, sobreposição detectada e ausência de violações de barreira ou protocolo.

### Regressões do observador

Depois de compilar, execute `python3 tests/test_observer.py` (Python 3 e Java no PATH). O teste exercita TCP real e cobre mediana, localidade portuguesa, END sem BEGIN, duplicações, sobreposição e barreira dupla, incluindo saída prematura na última rodada.

### Execução dos scripts

Use `bash scripts/run.sh normal 1` ou `bash scripts/run_all.sh`. Em Linux com os scripts executáveis, a chamada direta também funciona. A opção `USE_DOCKER=1` exige suporte a rede host no Docker. As validações desta versão executaram JVMs independentes dentro de um contêiner de teste, conectadas por TCP a um ZooKeeper separado.

Não use resultados de versões anteriores no mesmo CSV acumulado, pois o cabeçalho mudou. Arquive o CSV antigo antes de iniciar uma nova bateria.
