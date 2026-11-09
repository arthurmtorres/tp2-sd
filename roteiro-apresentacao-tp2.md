# Roteiro de apresentação — TP2: ZooKeeper + Apache Curator

**Disciplina:** BCC362 — Sistemas Distribuídos  
**Tema:** exclusão mútua distribuída com `InterProcessMutex` e barreira dupla com `DistributedDoubleBarrier`  
**Duração sugerida:** aproximadamente 10 a 12 minutos, incluindo demonstrações curtas.

Este roteiro distingue os resultados já obtidos dos resultados esperados nos testes que ainda precisam ser executados.

## 1. Abertura e objetivo — 40 segundos

**Fala sugerida:**

> Nosso trabalho implementa sincronização distribuída usando ZooKeeper e Apache Curator. O problema é coordenar processos independentes que disputam um recurso compartilhado, chamado R, para que apenas um o utilize de cada vez.
>
> Além da exclusão mútua, implementamos uma barreira dupla para organizar a execução em rodadas.

**Mostrar:** o título e o desenho da arquitetura no `README.md`.

## 2. Arquitetura — 1 minuto

**Fala sugerida:**

> A arquitetura tem clientes, nós de sincronização, ZooKeeper e um observador.
>
> Cada cliente conhece apenas seu nó e envia pedidos por TCP. O nó coloca esses pedidos em uma fila local e usa o Curator para disputar o mutex no ZooKeeper.
>
> O observador recebe eventos dos nós e verifica se houve sobreposição ou violação das regras da barreira.

**Fluxo para apontar durante a explicação:**

```text
Cliente ──TCP──> SyncNode ──Curator──> ZooKeeper
                    │
                    └──TCP──> Observer
```

**Arquivos para localizar:**

- `Client.java`: gera os pedidos e aguarda a confirmação.
- `SyncNode.java`: processa a fila e realiza a sincronização.
- `Observer.java`: verifica os eventos e produz o resumo.
- `ObsClient.java`: comunicação do nó com o observador.
- `Util.java`: funções auxiliares.

Nos scripts há um cliente por nó. A demonstração executa processos separados na mesma máquina.

## 3. O recurso e o fluxo de um pedido — 50 segundos

**Fala sugerida:**

> O recurso R é representado por uma seção crítica simulada, com uma espera de 0,2 a 1 segundo. O projeto não realiza uma escrita real em banco de dados.
>
> Para cada pedido, o nó retira um item da fila, adquire o mutex, registra a entrada, simula o acesso, registra a saída e libera o mutex. Depois envia COMMITTED ao cliente.

**Mostrar em `SyncNode.java`:**

```text
REQUEST → fila local → acquire → BEGIN → acesso a R → END → release → COMMITTED
```

A fila FIFO organiza os pedidos dentro de cada nó. O mutex coordena o acesso entre nós diferentes.

## 4. Por que um lock local não basta — 35 segundos

**Fala sugerida:**

> Um synchronized protegeria apenas threads dentro da mesma JVM. Nossos nós são processos independentes, então precisamos de um mecanismo de coordenação compartilhado.
>
> Todos os nós de uma execução disputam o mesmo caminho de lock, /locks/R, dentro do namespace daquela execução.

**Mostrar em `SyncNode.java`:**

```java
.namespace(ns)
```

```java
InterProcessMutex mutex = new InterProcessMutex(cf, "/locks/R");
```

O namespace isola uma execução das outras.

## 5. Como o mutex funciona — 1 minuto e 10 segundos

**Fala sugerida:**

> O Curator implementa a disputa usando znodes efêmeros sequenciais. O participante com a menor sequência pode adquirir o mutex.
>
> Os demais acompanham o predecessor. Quando o dono libera o mutex, seu znode é removido e o sucessor pode reavaliar a disputa.
>
> A ordem é a dos znodes criados no ZooKeeper. Não afirmamos que ela corresponde à ordem dos relógios dos clientes.

**Destacar:**

- O algoritmo interno pertence à receita do Curator.
- O projeto chama `acquire()` e `release()`.
- A liberação fica em um bloco `finally`.
- Mutex e barreira usam timeout de 120 segundos.

O `finally` libera o mutex adquirido ao sair do bloco protegido. Isso não implementa tratamento completo de perda de sessão.

## 6. Papel do observador — 50 segundos

**Fala sugerida:**

> O observador recebe BEGIN e END e mantém um contador de acessos ativos. Se esse contador ultrapassar um, registra uma sobreposição.
>
> Ele também conta os pedidos concluídos e verifica as regras da barreira.
>
> A resposta OK confirma o registro do evento. Quem fornece a exclusão mútua é o mutex do Curator coordenado pelo ZooKeeper.

**Métricas importantes:**

| Campo | Significado |
| --- | --- |
| `completed` | Acessos concluídos, contados pelos eventos END. |
| `expected` | Quantidade esperada de acessos. |
| `max_active` | Maior quantidade de acessos ativos simultaneamente. |
| `overlaps` | Entradas detectadas enquanto já havia um acesso ativo. |
| `barrier_violations` | Violações das regras da barreira. |
| `median_wait_ms` | Mediana da espera pela aquisição, medida no nó. |
| `p95_wait_ms` | Percentil 95 dessa espera. |
| `per_client` | Quantidade de acessos por cliente. |

Os eventos são enviados de forma síncrona. Essa instrumentação também acrescenta custo ao experimento.

## 7. Preparação da demonstração — fazer antes de apresentar

Execute na pasta do projeto:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

docker compose up -d
mvn -q clean package
```

Esse é o caminho do Java 17 que já funcionou na máquina utilizada.

**Fala sugerida:**

> O ZooKeeper executa em um container Docker. A aplicação foi compilada com Java 17 e gera um JAR com as dependências necessárias.

**Cuidados práticos:**

- Use `bash scripts/run.sh`, pois os scripts do ZIP não possuem permissão de execução.
- Execute os cenários um por vez, porque reutilizam as mesmas portas.
- Os comandos de compilação e teste geram `target/`, `logs/` e `results/`.
- Os comandos abaixo são uma demonstração curta, com dois nós e dois pedidos por cliente.

## 8. Demonstração: disputa com mutex — 45 segundos mais execução

**Fala antes do comando:**

> Primeiro vamos usar dois nós e dois pedidos por cliente, sem pausa entre os pedidos. São quatro acessos esperados.
>
> Queremos criar disputa e verificar se o mutex mantém apenas um acesso ativo por vez.

```bash
N=2 REQ=2 bash scripts/run.sh contention 1
```

**Resultado esperado:**

```text
status=OK
completed=4 expected=4
max_active=1
overlaps=0
barrier_violations=0
per_client=c1=2;c2=2
rc=0
```

O resumo real aparece em uma linha, junto de outras métricas.

## 9. Explicar os resultados já obtidos — 45 segundos

**Fala sugerida:**

> As duas execuções que já realizamos desse cenário terminaram com status OK.
>
> Todos os quatro pedidos foram concluídos, cada cliente concluiu dois, e o máximo de acessos simultâneos foi um. Portanto, nesses testes não houve violação de exclusão mútua.
>
> A mediana e o percentil 95 mostram a espera pela aquisição do mutex.

Na segunda execução fornecida no terminal:

```text
status=OK
completed=4 expected=4
total_s=2,7
median_wait_ms=943
p95_wait_ms=1020
max_active=1
overlaps=0
per_client=c1=2;c2=2
```

Os tempos podem variar entre execuções, mesmo com a mesma semente, por causa do escalonamento e das condições do ambiente.

**Importante para a fala:** `barrier_violations=0` em `contention` não valida a barreira, pois esse cenário não a habilita.

## 10. Demonstração: controle sem mutex — 40 segundos mais execução

**Fala antes do comando:**

> Agora vamos desativar o mutex. Isso permite que nós diferentes entrem ao mesmo tempo na seção crítica.
>
> Esperamos que o observador detecte a sobreposição. Uma violação nesse cenário é o resultado esperado e mostra que a instrumentação consegue detectar o problema.

```bash
N=2 REQ=2 bash scripts/run.sh nosync 1
```

**O que procurar:**

```text
VIOLATION_OVERLAP
status=VIOLACAO
max_active > 1
overlaps > 0
rc=2
```

Esses são resultados esperados; ainda não foram confirmados pelas saídas fornecidas na conversa.

Como a sobreposição depende do escalonamento, uma execução pequena pode não mostrá-la. Nesse caso, repetir com uma carga maior:

```bash
N=5 REQ=10 bash scripts/run.sh nosync 1
```

## 11. Explicar a barreira dupla — 1 minuto

**Fala sugerida:**

> O mutex limita quantos processos utilizam o recurso ao mesmo tempo. A barreira dupla coordena as fases.
>
> Na entrada, os participantes aguardam todos chegarem à rodada. Depois continuam disputando o recurso um por vez pelo mutex.
>
> Na saída, cada nó espera os demais terminarem a rodada antes de processar a próxima. Cada rodada usa seu próprio caminho no ZooKeeper.

**Mostrar a sequência:**

```text
barrier.enter
    → mutex.acquire
    → acesso a R
    → mutex.release
    → COMMITTED
    → barrier.leave
```

**Pontos para destacar:**

- O mutex é liberado antes da espera na barreira de saída.
- Os participantes precisam ter a mesma quantidade de pedidos.
- Os caminhos são separados por rodada: `/barriers/round-<r>`.
- `COMMITTED` confirma o pedido individual antes da conclusão coletiva da barreira.
- A barreira libera a fase; o mutex continua protegendo R.

## 12. Demonstração: barreira e mutex — 45 segundos mais execução

**Fala antes do comando:**

> Agora vamos executar duas rodadas com dois participantes. Esperamos concluir quatro pedidos, manter apenas um acesso ativo e não registrar violações de barreira.

```bash
N=2 REQ=2 bash scripts/run.sh barrier 1
```

**Resultado esperado:**

```text
status=OK
completed=4 expected=4
max_active=1
overlaps=0
barrier_violations=0
rc=0
```

**Nos logs dos nós, procurar:**

```text
BARRIER_ENTER_WAIT
BARRIER_PASSED
BARRIER_LEFT
```

O observador verifica se todos chegaram antes de alguém passar e se todos terminaram a rodada anterior antes de haver chegada à próxima.

Esse cenário precisa ser executado e conferido antes de ser apresentado como um resultado obtido.

## 13. Outros cenários e escala — 40 segundos

**Fala sugerida:**

> Além dos testes mostrados, temos cenários para clientes com pausas, carga desigual e maior quantidade de nós.
>
> No unequal, o primeiro cliente faz quatro vezes mais pedidos e não pausa entre eles. Com diferentes sementes e quantidades de nós, podemos comparar espera e vazão.

| Cenário | Comando | Acessos esperados |
| --- | --- | --- |
| Normal | `N=5 REQ=10 bash scripts/run.sh normal 1` | 50 |
| Carga desigual | `N=5 REQ=10 bash scripts/run.sh unequal 1` | 80 |
| Mais nós | `N=10 REQ=10 bash scripts/run.sh contention 1` | 100 |

Execute esses cenários durante a apresentação apenas se houver tempo.

Segundo o README, o trabalho pede de 10 a 50 acessos por cliente. O teste de dois pedidos serve para uma demonstração rápida; ele não substitui a avaliação com a carga solicitada.

## 14. Onde estão as evidências — 35 segundos

**Fala sugerida:**

> Cada execução cria uma pasta de logs. O observador gera um resumo e o projeto acumula resultados em um CSV.
>
> Nos logs dos nós, o request_id permite acompanhar o pedido desde a solicitação até COMMITTED, incluindo a aquisição e a liberação do mutex.

**Mostrar os caminhos da execução recém-concluída:**

```text
logs/<execução>/observer.log
logs/<execução>/node-1.log
logs/<execução>/client-1.log
results/summary.csv
```

O terminal informa o nome exato da pasta ao encerrar o teste.

**Observação sobre a planilha:** a máquina apresentada usa vírgula decimal, e o código também usa vírgulas como separador no CSV de métricas. Se as colunas aparecerem deslocadas ao importar, confira os números no `SUMMARY` do observador. Este roteiro não altera o código para corrigir esse comportamento.

## 15. Limitações e encerramento — 1 minuto

**Fala sugerida:**

> A avaliação assume execução sem falhas e utiliza um único servidor ZooKeeper.
>
> O projeto registra mudanças de conexão, mas não implementa tratamento de perda de sessão nem fencing tokens. Também não mede exatamente as mensagens internas geradas pelo Curator.
>
> Os testes mostram o comportamento observado; não provam ausência de starvation em qualquer execução.
>
> No cenário de disputa que já testamos, todos os pedidos terminaram sem sobreposição. A execução do cenário barrier permite verificar também a coordenação das rodadas.

Ao encerrar, cite os resultados de `nosync` e `barrier` como obtidos somente se tiver executado os cenários e conferido seus resumos.

## Perguntas possíveis

### O observador controla quem entra?

Não. Quando a sincronização está habilitada, o nó adquire o mutex do Curator antes de acessar R. O observador registra os eventos e confirma o recebimento. No `nosync`, essa mesma instrumentação pode registrar sobreposições.

### Por que utilizar Curator?

Porque ele fornece receitas de coordenação sobre o ZooKeeper, como o mutex e a barreira dupla usados no projeto. O código da aplicação organiza o fluxo dos pedidos e utiliza essas receitas.

### A barreira faz todos acessarem R simultaneamente?

Não. Ela permite avançar de fase depois que todos chegaram. O mutex continua permitindo somente um acesso a R por vez.

### E se um participante não chegar à barreira?

As chamadas de entrada e saída possuem timeout de 120 segundos. Se não completarem, o nó lança uma exceção. O projeto não implementa recuperação automática da rodada após a falha de um participante.

### Existe garantia de ordem de chegada dos clientes?

A fila é FIFO dentro de cada nó. A ordem de aquisição entre nós depende da criação dos znodes sequenciais no ZooKeeper. Não é uma ordenação global pelos relógios ou pelo instante em que os clientes enviaram os pedidos.

### Como interpretar os códigos de saída?

- `rc=0`: observador finalizou sem detectar violação ou quantidade incorreta de pedidos.
- `rc=2`: violação detectada; é esperado no `nosync` quando há sobreposição.
- `rc=3`: quantidade de pedidos incompleta detectada ao finalizar.

Uma falha de ambiente ou timeout pode impedir a geração do resumo e produzir outro código. Se não houver `SUMMARY`, consultar os logs.

### O que foi efetivamente validado até agora?

As saídas fornecidas mostram duas execuções de `contention`, cada uma com dois nós e dois pedidos por cliente. Ambas concluíram quatro pedidos, com `max_active=1` e `overlaps=0`. Os demais cenários deste roteiro precisam ser executados para que seus resultados sejam confirmados.
