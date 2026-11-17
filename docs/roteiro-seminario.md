# Roteiro para o seminário

A apresentação contém 12 slides de conteúdo e uma página de referências. Distribuam os tópicos entre os integrantes do grupo. O professor sugere cerca de 25 minutos, incluindo demonstração e discussão.

## Problema e arquitetura — cerca de 3 minutos

Apresentar o objetivo: cinco clientes conhecem um nó cada; cinco processos Sync disputam R. O ZooKeeper coordena as receitas e o observador mede ocupação. Explicar que R é simulado e que COMMITTED não significa gravação replicada.

## Receita e extensão — cerca de 6 minutos

Usar o exemplo de três concorrentes de docs/mecanismo.md. Apontar a sequência dos znodes, o watch do predecessor e a reavaliação. Localizar ResourceLock.acquire/release. Explicar FIFO local e fila global do ZooKeeper.

Depois, mostrar enter → mutex → R → release → leave. Identificar rodadas e explicar por que o mutex nunca fica retido ao esperar a barreira de saída.

## Demonstração — cerca de 8 minutos

Deixar as imagens e o JAR preparados antes de apresentar. Abrir o terminal na pasta do projeto e subir os 12 contêineres. O Compose apenas prepara o ambiente, sem começar os pedidos:

```bash
docker compose up -d
```

Depois executar um cenário por vez nos mesmos contêineres, com cinco clientes, cinco nós e dez pedidos por cliente:

```bash
bash scripts/scenario.sh nosync 1
bash scripts/scenario.sh normal 1
bash scripts/scenario.sh contention 1
bash scripts/scenario.sh barrier 1
```

Em outro terminal, `docker compose ps` mostra os 12 contêineres ativos. Os eventos aparecem no terminal do cenário e são preservados em `logs/<execução>/`. Aguardar **VERIFICADO** antes de iniciar o próximo cenário. Não é necessário executar `down` entre eles; não há auxiliares `prepare` ou `verify`.

Explicar o código 2 esperado no controle sem mutex. Mostrar que o cenário protegido retorna 0, todos os pedidos terminam e max_active=1. Na barreira, destacar ARRIVE/PASS/LEAVE e as rodadas.

Se o tempo estiver curto, usar uma demonstração reduzida explicitamente identificada e mostrar os logs completos já executados para o cenário mínimo:

```bash
DEMO=1 REQ=2 bash scripts/scenario.sh barrier 1
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
