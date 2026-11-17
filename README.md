# TP2 — ZooKeeper + Apache Curator (B2)

BCC362 · Sistemas Distribuídos · Professor Joubert Lima.

A aplicação coordena o acesso ao recurso hipotético R com **InterProcessMutex** e acrescenta uma **DistributedDoubleBarrier** por rodada. São processos Java independentes conectados pela rede. O acesso simulado continua sendo um sleep; COMMITTED não representa persistência em banco de dados.

## Começar

Requisitos: Linux, Docker Compose v2 e Python 3.10+ no computador. As JVMs executam nos contêineres; Java e Maven locais são necessários apenas se você optar pela compilação local. O ZIP inclui `target/tp2-sync.jar`; ao clonar o Git, compile primeiro com `USE_DOCKER=1 bash scripts/build.sh`.

Primeiro, suba os contêineres:

```bash
docker compose up -d
```

O Compose sobe **somente 12 contêineres: 5 clientes, 5 nós Sync, 1 observador e 1 ZooKeeper**. Os participantes ficam aguardando o comando do cenário; nenhuma carga começa com o `up`. A comunicação usa a rede interna e os nomes dos serviços (`node-1`, `observer`, `zookeeper`). Cada cliente conhece somente seu nó Sync.

Depois, execute um cenário por vez, aguardando o término de cada comando:

```bash
bash scripts/scenario.sh normal       # Carga normal
bash scripts/scenario.sh contention   # Alta contenção
bash scripts/scenario.sh barrier      # Barreira dupla
```

Os comandos reutilizam os mesmos contêineres com `docker compose exec`. O script inicia o observador, espera os nós ficarem prontos e só então inicia os clientes. Ao terminar, confere os logs e exporta `verification.json`, `summary.json` e `results/summary.csv`. Quando aparecer **VERIFICADO**, o cenário terminou e você pode iniciar outro. Não há contêineres `prepare` ou `verify`.

Para escolher outra semente, passe-a como segundo argumento. Os parâmetros de carga são lidos a cada execução, sem recriar os contêineres:

```bash
bash scripts/scenario.sh normal 2
CS_MIN=500 CS_MAX=500 bash scripts/scenario.sh contention 1
```

Também são aceitos `nosync` (controle sem mutex) e `unequal` (carga desigual). `REQ`, `CS_MIN`, `CS_MAX`, `TIMEOUT` e `LIVE` mantêm os limites dos scripts. No `nosync`, o comando retorna 2 somente depois de verificar a conclusão e a sobreposição esperada. Erros de processo, logs incompletos ou timeout reprovam a execução.

Os logs ao vivo aparecem no terminal do cenário e ficam em `logs/<execução>/`, incluindo `terminal.log`. `docker compose logs` mostra os logs do processo principal de cada contêiner, não os comandos iniciados por `exec`. Use `docker compose ps` para conferir os 12 contêineres ativos. Ctrl+C encerra as JVMs do cenário; os contêineres continuam disponíveis. Para encerrar o ambiente, use `docker compose down`.

Se você já usou o Compose anterior, execute uma vez `docker compose down --remove-orphans` antes de subir a nova configuração, para remover os antigos auxiliares. Os logs no projeto são preservados.

O Compose fixa cinco pares cliente/nó. Para a comparação com dez nós e a bateria completa, use os scripts abaixo. No Linux, os contêineres usam UID/GID 1000 por padrão; se seu usuário tiver outros IDs, exporte `LOCAL_UID=$(id -u)` e `LOCAL_GID=$(id -g)` antes de iniciar.

## Cenários nos contêineres do Compose

Com os 12 contêineres ativos, escolha um comando abaixo. O segundo argumento é a semente aleatória; quando omitido, vale 1.

| Comando | O que verificar |
| --- | --- |
| `bash scripts/scenario.sh nosync 1` | Controle sem mutex: mais de um acesso ativo e sobreposição detectada; código esperado **2**. |
| `bash scripts/scenario.sh normal 1` | 5 clientes × 10 pedidos, pausas de 1–5 s, sem sobreposição. |
| `bash scripts/scenario.sh contention 1` | Clientes disparados conjuntamente; sem pausa entre pedidos; sleep de R preservado. |
| `bash scripts/scenario.sh unequal 1` | c1 faz 40 pedidos sem pausa; os demais fazem 10 com pausas. |
| `bash scripts/scenario.sh barrier 1` | 5 participantes, 10 rodadas, entrada/saída coletivas e mutex sobre R. |

No `nosync`, a sobreposição é o resultado esperado: ela demonstra que o observador detecta acessos simultâneos. O resumo mostra **Sobreposição detectada (esperada)**, e o comando retorna 2 após conferir todos os pedidos. Nos cenários protegidos, espera-se código 0, máximo de um acesso ativo e zero sobreposições. Na barreira dupla, também devem ocorrer zero violações de rodada.

Execute um cenário por vez e aguarde **VERIFICADO** antes do próximo. O script recusa uma segunda execução simultânea nos mesmos contêineres. Não é necessário dar `down` para repetir um cenário ou trocar a semente.

## Resumo no terminal

Ao final, as métricas aparecem em um quadro com uma informação por linha. Trecho do resumo de uma execução normal:

```text
RESUMO: Carga normal
  Pedidos concluídos            : 50/50
  Acessos ainda ativos          : 0
  Tempo medido pelo observador  : 37,930 s
  Tempo total do comando        : 41,315 s
  Espera mediana pelo lock      : 247,827 ms
  Vazão                         : 1,318 pedidos/s
  Máximo de acessos simultâneos : 1
  Sobreposições                 : 0
  Violações da barreira         : 0

>> VERIFICADO: 50/50 pedidos
```

Os valores acima são de uma execução; tempos e vazão variam. O quadro também inclui status, semente, nós finalizados, eventos inválidos, mensagens do observador, acessos por cliente e caminhos dos arquivos.

**A vírgula separa os decimais no terminal:** `1,801 pedidos/s` significa aproximadamente 1,8 pedido por segundo. A vazão é `pedidos concluídos ÷ tempo medido pelo observador`; por exemplo, 50 pedidos em 27,756 s correspondem a cerca de 1,801 pedidos/s. `50/50` é o total concluído em relação ao esperado.

A espera mediana mede o tempo para adquirir o lock, antes do sleep de R. O tempo do observador vai do primeiro BEGIN ao último END; o tempo total do comando inclui a inicialização dos participantes e a conferência. Os logs e arquivos CSV/JSON preservam os dados originais com precisão completa.

## Bateria completa e comparação de 5 e 10 nós

`scenario.sh` reutiliza os cinco pares cliente/nó do Compose. A bateria usa `run_all.sh`, que cria contêineres temporários para cada execução e permite variar a quantidade de participantes. Se estiver usando os 12 contêineres, aguarde o cenário atual terminar e prepare somente o ZooKeeper para esse fluxo:

```bash
docker compose down --remove-orphans
docker compose up -d --wait zookeeper
bash scripts/run_all.sh
```

A bateria executa 11 experimentos: um controle sem mutex; três repetições de carga normal e três de alta contenção, com sementes 1, 2 e 3; uma barreira dupla; uma carga desigual; e duas execuções de contenção para comparar 5×20 com 10×10 pedidos. A comparação mantém o total de 100 pedidos e os mesmos limites da seção crítica.

O código 2 do controle é aceito somente depois de conferir a conclusão, o recebimento dos COMMITTED e a sobreposição esperada. Qualquer falha de processo, log incompleto, timeout ou divergência interrompe a bateria. O término bem-sucedido retorna 0 e gera `results/battery.json` e `results/relatorio.md`.

A bateria leva vários minutos. Para acompanhar todos os eventos ao vivo, use `LIVE=1 bash scripts/run_all.sh`.

Também é possível executar a comparação manualmente, um comando por vez, com somente o ZooKeeper iniciado:

```bash
N=5 REQ=20 bash scripts/run.sh contention 1
N=10 REQ=10 bash scripts/run.sh contention 1
```

Cada cliente, nó Sync e observador executa em seu próprio contêiner temporário. Esse fluxo usa `--network host` no Linux. O terminal informa os nomes e um comando `docker ps --filter name=...` para acompanhá-los; os contêineres são removidos ao terminar, e os logs permanecem no projeto.

Para executar as JVMs localmente, com Java 17 instalado, use `USE_DOCKER=0 bash scripts/run.sh contention 1`. A mesma opção vale para `scripts/run_all.sh`.

Depois da bateria, para voltar aos cenários nos 12 contêineres, execute `docker compose up -d` e use novamente `scripts/scenario.sh`.

## Compilação e testes

Para recompilar e executar os testes automatizados, use Maven 3.9.9 e JDK 17 locais ou o contêiner de compilação:

```bash
bash scripts/build.sh
# Ou, sem JDK/Maven no sistema:
USE_DOCKER=1 bash scripts/build.sh
```

A compilação executa os testes Java e as regressões dos scripts. Para repetir somente os testes dos scripts: `python3 scripts/test_scripts.py`. `build.sh` usa JDK/Maven locais por padrão; `USE_DOCKER=1` seleciona a compilação em contêiner.

## Parâmetros

| Variável | Padrão | Significado |
| --- | --- | --- |
| `N` | 5 | Quantidade de nós e clientes em `run.sh`; entre 5 e 20 nos experimentos acadêmicos. `scenario.sh` usa sempre os 5 pares do Compose. |
| `REQ` | 10 | Pedidos por cliente, entre 10 e 50. No unequal, entre 10 e 12, pois c1 faz 4×REQ. |
| `ZK` | localhost:2181 | Servidor ZooKeeper para `run.sh` e a bateria. `scenario.sh` usa `zookeeper:2181` na rede do Compose. |
| `ZK_METRICS` | localhost:2181 | Endereço publicado do ZooKeeper para consultar contadores ao usar `scenario.sh`. |
| `CS_MIN`, `CS_MAX` | 200, 1000 | Limites do sleep de R em ms; precisam permanecer entre 200 e 1000. |
| `TIMEOUT` | 1800 | Prazo total em segundos, incluindo todos os processos; encerra os filhos ao falhar. |
| `LIVE` | 1 | Use 0 para mostrar apenas resumo/erros e preservar os logs completos em disco. |
| `NO_COLOR` | não definido | Se definido, desabilita cores. Redirecionamento para arquivo também não usa cores. |
| `USE_DOCKER` | 1 (execução) | Seleciona contêineres ou JVMs locais em `run.sh` e na bateria. No `build.sh`, o padrão é 0. `scenario.sh` executa sempre nos contêineres existentes do Compose. |
| `LOCAL_UID`, `LOCAL_GID` | 1000, 1000 | Usuário e grupo dos contêineres do Compose; exporte antes de executar `docker compose up -d` se seus IDs forem diferentes. |
| `OBS_PORT`, `BASE_PORT` | 9000, 7000 | Porta do observador e base das portas dos nós. |
| `DEMO` | 0 | Use 1 para permitir demonstrações pequenas, explicitamente marcadas como reduzidas. |

Exemplo reduzido nos mesmos cinco pares do Compose; **não substitui a carga do professor**:

```bash
DEMO=1 REQ=2 bash scripts/scenario.sh barrier 1
```

Para demonstrar com dois pares em contêineres temporários, use o fluxo com somente ZooKeeper ativo e execute `DEMO=1 N=2 REQ=2 bash scripts/run.sh barrier 1`.

Os cenários devem ser executados **um por vez**. Não há um requisito de conectividade direta entre SyncNodes: as receitas se comunicam pelo ZooKeeper. É permitido usar processos no mesmo computador. A instalação mínima de um ZooKeeper é suficiente para o escopo sem falhas do TP2.

## Organização do código

| Arquivo | Responsabilidade |
| --- | --- |
| `Client.java` | Conhece um único nó, gera IDs, solicita R e confirma recebimento de COMMITTED. |
| `SyncNode.java` | Recebe conexões, enfileira pedidos, processa a seção crítica e controla rodadas. |
| `ResourceLock.java` | Operações `acquire(R, request_id)` e `release(R, request_id)`; delega a receita ao Curator. |
| `ObsClient.java` | Canal de registro síncrono; OK confirma registro, não autorização. |
| `Observer.java` | Rede e exportação do resumo do verificador. |
| `ObserverState.java` | IDs únicos, ocupação, participantes/rodadas e conclusão. |
| `Metrics.java` | Mediana, percentil e CSV com ponto decimal independentemente de locale. |
| `Util.java` | Argumentos, conexão e formato de log. |
| `scripts/run.py` | Inicia/acompanha processos, reúne logs, mede tráfego agregado e verifica o resultado. |
| `scripts/scenario.sh`, `scripts/compose.py`, `scripts/compose-service.sh` | Executam e encerram as JVMs nos contêineres existentes e verificam o cenário. |
| `scripts/verify.py` | Conferência independente: eventos, IDs, COMMITTED, ordem dos znodes e barreiras. |
| `scripts/run_all.py`, `scripts/analyze.py` | Bateria reproduzível e relatório gerado dos dados reais. |

O fluxo por pedido é:

`REQUEST → acquire → BEGIN/OK → sleep → END/OK → release → COMMITTED`.

Com barreira, `enter()` ocorre antes do acquire e `leave()` depois do COMMITTED individual. O mutex já está livre ao aguardar a saída coletiva; o nó só processa o próximo pedido depois de leave().

Um worker serializa os pedidos de cada nó. O cliente envia HELLO, REQUESTs e BYE. O nó espera todos os clientes configurados terminarem; BYE de um deles não interrompe os demais. O cenário padrão associa um cliente a cada nó; o argumento Java `--clients` permite configurar múltiplos clientes por nó fora da barreira.

## Como interpretar os logs

- Prefixo colorido: processo de origem, como `node-1`, `client-1` e `observer`.
- `request_id=c3-7`: sétimo pedido de c3; IDs são únicos dentro da execução.
- `LOCK_WAIT`: início da chamada do mutex; o processo pode estar aguardando na fila.
- `ACQUIRE wait_s=... znode=... queue=...`: aquisição e estado da fila naquele instante.
- `BEGIN/END active=... seq=...`: ocupação medida pelo observador. `seq` ordena o registro central sem comparar relógios de máquinas.
- `ENTER sleep_ms=...` e `EXIT cs_s=...`: duração programada e observada da seção crítica.
- `RELEASE`: liberação de R; no controle sem mutex aparece `sync=false`.
- `RECEIVED_COMMITTED`: confirmação recebida pelo cliente, com latência própria e espera reportada pelo nó.
- `ARRIVE/PASS/LEAVE`: evidências da entrada/saída da barreira, com rodada e identidades.
- `SUMMARY`: conclusão, espera mediana, concorrência, erros e acessos por cliente. A linha original fica nos logs; no terminal, essas métricas aparecem em um quadro ao fim da verificação, com uma informação por linha e unidades explícitas.

Mensagens internas rotineiras do Curator/ZooKeeper ficam no nível WARN. As cores não fazem parte dos arquivos; são adicionadas apenas na exibição interativa.

Na saída reunida, o UUID de proteção do Curator é abreviado: `lock-0000000002` permite ver a sequência sem uma fila longa de UUIDs. Os logs individuais dos nós preservam os nomes completos dos znodes para conferência.

## Verificações e métricas

O observador mantém conjuntos por pedido e por participante; END sem BEGIN, evento duplicado, nó/rodada incorretos e DONE repetido não podem fabricar sucesso. Mesmo com `active>0`, um BEGIN válido é registrado e recebe OK: o observador **não** implementa exclusão mútua.

Depois, `verify.py` cruza a carga esperada, os seis eventos dos nós, todos os BEGIN/END, os recebimentos dos clientes, a ordem dos znodes e todas as fases da barreira. É possível conferir novamente um experimento:

```bash
python3 scripts/verify.py logs/NOME_DA_EXECUCAO
```

Arquivos principais:

- `logs/<execução>/config.json`: cenário, semente, processos, cargas e duração de R.
- Logs individuais e `terminal.log`: todos os eventos, preservados sem cores.
- `observer.csv`: resumo original deste observador, sem dependência de locale.
- `verification.json`: conferência independente.
- `summary.json`: resumo, configuração, códigos e contadores da execução.
- `results/summary.csv`: resumos acumulados; não edite seu cabeçalho.
- `results/battery.json`: execuções exatas da última bateria.
- `results/relatorio.md`: análise gerada dessas execuções, incluindo tráfego e limitações.
- `apresentacao/apresentacao_tp2_b2.pptx`: slides para o seminário.
- `docs/roteiro-seminario.md`: sugestões de demonstração e perguntas.

Espera do acquire é medida com `System.nanoTime()` no nó e exclui fila local/barreira. Latência do cliente também usa relógio monotônico próprio. `total_s` do observador vai do primeiro BEGIN ao último END; `wall_s` inclui preparação/encerramento. A mediana de amostras pares usa a média dos dois valores centrais.

REQUEST/COMMITTED, instrumentação/OK, chamadas da API e leituras adicionais de evidência têm contadores separados. Quando `mntr` está habilitado, são registrados também os pacotes do protocolo ZooKeeper recebidos/enviados no intervalo. São agregados com sessões, heartbeat e consultas extras; não são a quantidade exata de mensagens só do mutex. Consulte a discussão em `docs/mecanismo.md` e o relatório de resultados.

## Versões e limites

- JDK: Java 17; compilação/verificação local com OpenJDK 17.0.19.
- Maven 3.9.9; compiler plugin 3.13.0, surefire 3.5.0, shade 3.5.3.
- Apache Curator 5.7.1; cliente ZooKeeper transitivo 3.9.2.
- Servidor ZooKeeper 3.9.3; slf4j-simple 2.0.13; JUnit 5.11.0.
- Contêiner Java: `eclipse-temurin:17.0.17_10-jre-jammy`; compilação: `maven:3.9.9-eclipse-temurin-17`.
- Orquestração/verificação dos cenários do Compose: Python 3.10+ no computador.

As evidências originais usam processos Java nativos e ZooKeeper Apache 3.9.3. A validação adicional do Compose está descrita em `docs/validacao.md`.

O escopo assume disponibilidade e ausência de perdas/quedas. Não há avaliação de replicação, recuperação de sessão ou fencing. Ausência de starvation em uma execução finita não é prova geral. O mutex é cooperativo: todos os concorrentes precisam usar o mesmo caminho e namespace. O namespace muda a cada execução.

Para encerrar os serviços: `docker compose down`.
