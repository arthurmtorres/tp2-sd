# Roteiro para o seminário

A apresentação contém 12 slides de conteúdo e uma página de referências. Distribuam os tópicos entre os integrantes do grupo. O professor sugere cerca de 25 minutos, incluindo demonstração e discussão.

## Problema e arquitetura — cerca de 3 minutos

Apresentar o objetivo: cinco clientes conhecem um nó cada; cinco processos Sync disputam R. O ZooKeeper coordena as receitas e o observador mede ocupação. Explicar que R é simulado e que COMMITTED não significa gravação replicada.

## Receita e extensão — cerca de 6 minutos

Usar o exemplo de três concorrentes de docs/mecanismo.md. Apontar a sequência dos znodes, o watch do predecessor e a reavaliação. Localizar ResourceLock.acquire/release. Explicar FIFO local e fila global do ZooKeeper.

Depois, mostrar enter → mutex → R → release → leave. Identificar rodadas e explicar por que o mutex nunca fica retido ao esperar a barreira de saída.

## Demonstração — cerca de 8 minutos

Para mostrar cada cliente e nó Sync em seu próprio contêiner, usar `SCENARIO=contention docker compose up`. Em outro terminal, `docker compose ps -a` mostra todos os serviços e `docker compose logs verify` mostra a conferência final. Para trocar para `nosync` ou `barrier`, executar `docker compose down` e iniciar novamente com o novo `SCENARIO`. `prepare` e `verify` são auxiliares que encerram ao concluir suas etapas.

Os comandos abaixo continuam disponíveis para demonstrar pelos scripts:

Deixar o ZooKeeper e as imagens Java preparados antes de apresentar. Abrir o terminal na pasta do projeto. Usar inicialmente cinco clientes e cinco nós, dez pedidos por cliente:

```bash
docker compose up -d --wait zookeeper
bash scripts/run.sh nosync 1
bash scripts/run.sh contention 1
bash scripts/run.sh barrier 1
```

Cada cliente, nó Sync e observador já executa em seu próprio contêiner por padrão. O terminal mostra os nomes e o comando para acompanhá-los com docker ps durante a execução. Para manter os contêineres encerrados visíveis, usar a execução completa pelo Docker Compose e consultar docker compose ps -a. Para optar por Java local, acrescentar USE_DOCKER=0. Cada experimento guarda os próprios logs. Explicar o código 2 esperado no controle sem mutex. Mostrar que o cenário protegido retorna 0, todos os pedidos terminam e max_active=1. Na barreira, destacar ARRIVE/PASS/LEAVE e as rodadas.

Se o tempo estiver curto, usar uma demonstração reduzida explicitamente identificada e mostrar os logs completos já executados para o cenário mínimo:

```bash
DEMO=1 N=2 REQ=2 bash scripts/run.sh barrier 1
```

Selecionar algumas linhas de ACQUIRE com a fila e os respectivos RELEASE. Comparar COMMITTED do nó com RECEIVED_COMMITTED do cliente. O log do observador tem sua própria sequência seq; não ordenar logs de hosts diferentes por horário para provar exclusão.

## Resultados — cerca de 3 minutos

Usar results/relatorio.md e os resultados reais dos slides. Informar as três repetições normais/de contenção, sementes e faixa do sleep. Comparar 5×20 com 10×10 para manter 100 pedidos. Distinguir mensagens de instrumentação, chamadas da API e tráfego agregado do ZooKeeper.

## Discussão — cerca de 5 minutos

- Por que um único ZooKeeper é permitido? Implantação mínima e sem exigência de alta disponibilidade no TP2.
- O observador é um coordenador de acesso? Não; aceita BEGIN sobreposto e acusa. O controle sem mutex comprova.
- O mutex garante ordem dos timestamps de clientes? Não; a ordem é a fila sequencial observada pelo ZooKeeper.
- Ausência de starvation foi provada pelo teste? Não; foi observada nesta carga finita, sob as hipóteses declaradas.
- Por que COMMITTED vem antes de leave? Confirma a simulação individual; o nó ainda não processa a próxima rodada.
- Que parte muda no TP3? simulateCriticalSection pode ser substituída pelo acesso ao Store, preservando acquire/release.
- E quedas, sessões expiradas ou replicação? Estão fora da avaliação solicitada; não afirmar que foram testadas.

Evitar rodar toda a bateria durante a apresentação. Ela conserva os intervalos do enunciado e leva vários minutos; usar os resultados preservados e demonstrar cenários selecionados ao vivo.
