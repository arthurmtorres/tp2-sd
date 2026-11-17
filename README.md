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

## Execução pelos scripts

Os scripts também executam **cada cliente, cada nó Sync e o observador em um contêiner separado por padrão**. Requisitos: Linux, Python **3.10+** e Docker. Eles usam `--network host`, preparado para Docker no Linux. Não é necessário instalar Java para executar os cenários dessa forma.

Inicie **somente** a infraestrutura antes de usar os scripts:

```bash
docker compose up -d --wait zookeeper
bash scripts/run.sh contention 1
```

O terminal mostra o nome de cada contêiner iniciado e um comando `docker ps --filter name=...` para acompanhar somente os participantes daquela execução. Por padrão são 11 contêineres da aplicação e o ZooKeeper. Os contêineres dos scripts são removidos ao terminar. Para reutilizar os 12 contêineres do Compose entre cenários, use `scripts/scenario.sh`, descrito acima.

Para executar explicitamente como processos Java locais, com Java **17** instalado: `USE_DOCKER=0 bash scripts/run.sh contention 1`. A mesma escolha vale para `scripts/run_all.sh`; a bateria usa contêineres por padrão.

Os scripts exibem logs ao vivo, alinhados e com cores quando o terminal permite, e preservam `terminal.log` e os logs individuais sem cores. Cada cenário termina com a mesma conferência automática usada pelo Compose.

Para recompilar e executar os testes automatizados, use Maven **3.9.9** e JDK 17 ou o contêiner de compilação:

```bash
bash scripts/build.sh
# Ou, sem JDK/Maven no sistema:
USE_DOCKER=1 bash scripts/build.sh
```

A compilação executa os testes Java e as regressões dos scripts. Para repetir somente os testes dos scripts: `python3 scripts/test_scripts.py`. A compilação mantém sua escolha independente: `build.sh` usa JDK/Maven locais por padrão; `USE_DOCKER=1` usa o contêiner de compilação.

## Cenários

| Comando | O que verificar |
| --- | --- |
| `bash scripts/scenario.sh nosync 1` | Controle sem mutex: mais de um ativo e sobreposição detectada; código esperado **2**. |
| `bash scripts/scenario.sh normal 1` | 5 clientes × 10 pedidos, pausas de 1–5 s, sem sobreposição. |
| `bash scripts/scenario.sh contention 1` | Clientes disparados conjuntamente; sem pausa entre pedidos; sleep de R preservado. |
| `bash scripts/scenario.sh unequal 1` | c1 faz 40 pedidos sem pausa; os demais fazem 10 com pausas. |
| `bash scripts/scenario.sh barrier 1` | 5 participantes, 10 rodadas, entrada/saída coletivas e mutex sobre R. |
| `bash scripts/run_all.sh` | Bateria completa, três repetições obrigatórias, extensão e comparação de escala. |

Na bateria, o código 2 do controle é tratado como esperado **somente depois de verificar conclusão, recebimento dos COMMITTED e sobreposição**. Qualquer falha de processo, log incompleto, timeout ou divergência interrompe a bateria com erro. O término da bateria completa retorna 0.

O cenário normal preserva os intervalos exigidos; a bateria leva vários minutos. Para acompanhar tudo em tempo real durante a bateria: `LIVE=1 bash scripts/run_all.sh`.

## Parâmetros

| Variável | Padrão | Significado |
| --- | --- | --- |
| `N` | 5 | Quantidade de nós e clientes; entre 5 e 20 nos experimentos acadêmicos. |
| `REQ` | 10 | Pedidos por cliente, entre 10 e 50. No unequal, entre 10 e 12, pois c1 faz 4×REQ. |
| `ZK` | localhost:2181 | Endereço do servidor ZooKeeper. |
| `CS_MIN`, `CS_MAX` | 200, 1000 | Limites do sleep de R em ms; precisam permanecer entre 200 e 1000. |
| `TIMEOUT` | 1800 | Prazo total em segundos, incluindo todos os processos; encerra os filhos ao falhar. |
| `LIVE` | 1 | Use 0 para mostrar apenas resumo/erros e preservar os logs completos em disco. |
| `NO_COLOR` | não definido | Se definido, desabilita cores. Redirecionamento para arquivo também não usa cores. |
| `USE_DOCKER` | 1 (execução) | Cada JVM executa em um contêiner separado. Use 0 para processos Java locais. No `build.sh`, o padrão continua sendo 0. |
| `OBS_PORT`, `BASE_PORT` | 9000, 7000 | Porta do observador e base das portas dos nós. |
| `DEMO` | 0 | Use 1 para permitir demonstrações pequenas, explicitamente marcadas como reduzidas. |

Exemplo reduzido, útil para explicar os eventos; **não substitui a carga do professor**:

```bash
DEMO=1 N=2 REQ=2 bash scripts/run.sh barrier 1
```

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
