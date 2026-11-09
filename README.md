# TP2 — Cluster Sync com ZooKeeper e Apache Curator

Implementação do tema **B2 — ZooKeeper com Apache Curator** para a disciplina BCC362 — Sistemas
Distribuídos. Cinco clientes enviam pedidos pela rede para cinco processos independentes do Cluster
Sync. Os processos disputam o recurso hipotético `R` por meio de um `InterProcessMutex` e usam uma
`DistributedDoubleBarrier` para sincronizar o início e o fim de cada rodada.

## Arquitetura

```text
client-1 ──TCP──> cluster-1 ─┐
client-2 ──TCP──> cluster-2 ─┤
client-3 ──TCP──> cluster-3 ─┼──> R (ENTER → sleep → EXIT)
client-4 ──TCP──> cluster-4 ─┤
client-5 ──TCP──> cluster-5 ─┘
                    │     │
                    │     └── BEGIN/END por TCP ──> observer
                    └──────── mutex + barreira ───> ZooKeeper
```

Cada cliente conhece somente seu processo do Cluster Sync. Cada pedido possui o identificador único
`client_id-sequência`, por exemplo `client-3-7`. Dentro de cada processo, uma fila de execução com uma
única thread preserva a ordem local dos pedidos recebidos. Essa fila local não substitui o mutex
distribuído: processos diferentes continuam concorrendo pelo mesmo caminho no ZooKeeper.

O observador recebe `BEGIN` antes do `sleep` da seção crítica e confirma seu registro. Depois do
`sleep`, recebe `END`. Ele apenas mede a quantidade de acessos ativos e acusa sobreposições; nunca
concede ou bloqueia acesso a `R`.

`R` não é o ZooKeeper nem o observador. Ele é o recurso hipotético pedido no enunciado e aparece no
código como a classe interna `SimulatedResource` de `ClusterNode`. Cada processo possui acesso lógico
ao mesmo recurso `R`, mas só pode
chamar sua operação de escrita depois de obter o mutex distribuído. Em um TP posterior, essa classe
pode ser substituída por uma chamada de rede ao recurso real sem alterar o mecanismo de coordenação.

## Sequência de um pedido

1. O cliente envia `REQUEST` ao processo que conhece.
2. O processo entra na barreira `/tp2/barriers/round-N`.
3. O processo solicita o mutex em `/tp2/resources/R/lock`.
4. Ao obter o lock, registra `ACQUIRE` e envia `BEGIN` ao observador.
5. A seção crítica registra `ENTER`, espera entre 200 ms e 1 s e registra `EXIT`.
6. O processo envia `END`, libera o mutex e registra `RELEASE`.
7. Todos saem da barreira dupla somente depois de terminar a atividade da rodada.
8. O processo responde `COMMITTED` ao cliente.

O `InterProcessMutex` implementa a receita de lock do ZooKeeper com nós efêmeros sequenciais. Cada
concorrente cria um nó ordenado; o menor adquire o lock e os demais observam o predecessor. Quando o
predecessor desaparece, o próximo concorrente reavalia a fila. Observar somente o predecessor evita
acordar todos os participantes a cada liberação.

A barreira inclui o número da rodada no caminho. Assim, participantes atrasados da rodada `N` não são
misturados aos da rodada `N+1`. Todos os clientes devem executar a mesma quantidade de pedidos quando
a barreira estiver habilitada.

## Tecnologias e versões

- Java 17;
- Apache Curator 5.9.0;
- cliente Apache ZooKeeper 3.9.7 e servidor Docker 3.9.6;
- Maven 3.9 ou superior;
- Docker Engine e Docker Compose v2.

As versões estão fixadas no `pom.xml`, no `Dockerfile` e no `compose.yaml`. A biblioteca cliente usa
3.9.7; o servidor usa `zookeeper:3.9.6-jre-17`, que é a versão mais recente atualmente publicada como
imagem oficial. Clientes da linha 3.9 são compatíveis com esse servidor. Consulte a documentação do
[Apache Curator](https://curator.apache.org/docs/recipes/) e os
[releases do ZooKeeper](https://zookeeper.apache.org/releases/).

## Estrutura do código

O código Java foi mantido em um único pacote, com uma classe por responsabilidade principal:

```text
ClusterNode.java      processo intermediário e seção crítica de R
LoadClient.java       geração dos pedidos dos clientes
CuratorSync.java      mutex e barreira dupla do Curator
ObserverServer.java   auditoria de BEGIN/END
WireProtocol.java     mensagens e protocolo TCP
Environment.java      leitura das configurações
EventLog.java         formato comum dos logs
```

Os records `AccessRequest`, `AccessResponse` e `AuditEvent` ficam dentro de `WireProtocol`, pois são
usados somente na comunicação entre esses processos.

## Compilação e testes

Para executar os testes unitários e os testes das receitas com um ZooKeeper embutido:

```bash
mvn clean test
```

Para gerar o JAR com todas as dependências:

```bash
mvn clean package
```

Os testes verificam:

- serialização do protocolo de rede;
- detecção automática de sobreposição pelo observador;
- exclusão mútua com cinco clientes Curator concorrentes;
- sincronização de início e fim com a barreira dupla.

## Execução com Docker

O script aceita quatro experimentos:

```bash
scripts/run-experiment.sh normal
scripts/run-experiment.sh contention
scripts/run-experiment.sh unequal
scripts/run-experiment.sh unprotected
```

- `normal`: mutex ativo e espera de 1 a 5 segundos entre pedidos;
- `contention`: mutex ativo e sem espera entre pedidos;
- `unequal`: o primeiro cliente solicita com mais frequência;
- `unprotected`: mutex desativado, usado para comprovar que a auditoria detecta sobreposições.

Cada execução cria `results/<experimento>-<data>/all.log` e `summary.json`. O resumo contém pedidos
concluídos, tempo total, vazão, espera mediana, percentil 95, acessos por cliente, máximo de acessos
simultâneos e quantidade de sobreposições. A execução só é aprovada se o verificador confirmar
progresso, ciclo completo de cada pedido, pares `BEGIN`/`END`, propriedade esperada do mutex e cinco
entradas e saídas em cada rodada da barreira.

O conjunto mínimo de repetições exigido no enunciado pode ser executado com:

```bash
scripts/run-required-suite.sh
```

Esse comando realiza três repetições da carga normal, três de alta contenção e uma execução de
controle sem mutex. Para alterar a quantidade de pedidos, mantendo o intervalo exigido de 10 a 50:

```bash
REQUEST_COUNT=20 scripts/run-experiment.sh normal
```

Outros parâmetros úteis são `CRITICAL_SECTION_MIN_MS`, `CRITICAL_SECTION_MAX_MS` e as cinco sementes
`RANDOM_SEED_1` a `RANDOM_SEED_5`. Os tempos de aquisição são medidos dentro do processo com
`System.nanoTime()`, portanto não dependem de relógios físicos sincronizados entre contêineres.

Para encerrar e remover manualmente os contêineres e o estado experimental do ZooKeeper:

```bash
docker compose down --volumes
```

## Eventos dos logs

Os processos registram `REQUEST`, `ACQUIRE`, `ENTER`, `EXIT`, `RELEASE` e `COMMITTED`, sempre com
`request_id`. Também são registrados os eventos de entrada e saída da barreira. O formato
`chave=valor` facilita a leitura durante a apresentação e o processamento automático pelo script
`scripts/summarize.py`.

Com o mutex habilitado, o resultado esperado da auditoria é:

```json
{
  "maximum_concurrent_accesses": 1,
  "overlap_violations": 0
}
```

Sem o mutex, espera-se `maximum_concurrent_accesses` maior que 1 e pelo menos uma sobreposição. Essa
segunda execução valida a instrumentação; ela não é uma configuração correta da aplicação.

## Garantias e limites

- Exclusão mútua depende de todos os processos acessarem `R` usando o mesmo caminho de lock.
- O atendimento segue a ordem dos pedidos registrada pelo ZooKeeper, não a ordem de timestamps dos
  clientes nem uma ordenação de Lamport.
- O lock sempre é liberado em um bloco de limpeza, mesmo quando ocorre uma exceção.
- Nenhum processo espera a barreira enquanto mantém o mutex.
- A demonstração considera participantes e serviços disponíveis, sem quedas ou perdas de mensagens,
  conforme o escopo do TP2.
- O trabalho não mede replicação interna, failover ou recuperação de sessão do ZooKeeper.

## Fluxo Git adotado

O desenvolvimento usa branches curtas a partir de `main`, com nomes como `feat/curator-cluster-sync`,
e commits no padrão Conventional Commits: `feat:`, `test:`, `docs:`, `fix:` e `chore:`. Alterações só
devem chegar à `main` depois da revisão e dos testes da branch.
