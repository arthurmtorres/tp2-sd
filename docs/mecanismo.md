# Mecanismo e propriedades

## Mutex: o que existe por trás de acquire()

Todos os nós usam `/locks/R` no mesmo namespace. O Curator cria um znode efêmero sequencial para o pedido. A sequência define sua posição. Quem está primeiro pode adquirir; os demais observam o predecessor. Se o predecessor desaparece antes da instalação do watch, a condição é reavaliada. Uma notificação acorda o participante, que verifica novamente a fila. Release remove o znode e permite ao sucessor prosseguir.

Exemplo de disputa: n3 cria `lock-...001`, n1 cria `lock-...002`, n2 cria `lock-...003`. n3 entra; n1 aguarda n3; n2 aguarda n1. Quando n3 libera, n1 reavalia e entra. A ordem é a criação no ZooKeeper, não o instante físico em que cada cliente enviou seu pedido.

O InterProcessMutex é reentrante por thread, mas esta aplicação faz exatamente um acquire/release por acesso. A mesma thread do worker adquire e libera. A receita é reutilizada no nó. A classe ResourceLock só identifica o pedido, expõe o znode real e delega ao Curator; ela não implementa uma segunda versão do algoritmo.

Os znodes armazenam o request_id como dados para associar o estado do serviço ao pedido. O UUID de proteção do Curator e a sequência fazem parte do nome. O log queue mostra um snapshot de participantes, não uma medição contínua de toda a disputa.

A raiz `/locks/R` é preparada como persistente. Assim, uma pausa sem concorrentes não causa a limpeza automática de um nó CONTAINER e o reinício de sua sequência. Isso mantém a interpretação da sequência nos logs durante toda a execução; os filhos do lock continuam efêmeros sequenciais. Preparar essa raiz é configuração do recurso, não uma implementação própria do mutex.

## Exclusão mútua, progresso e justiça

- Exclusão mútua: participantes cooperativos no mesmo caminho obtêm o lock segundo a receita do ZooKeeper.
- Progresso: com serviços disponíveis, seção crítica finita e liberações normais, o sucessor pode prosseguir após a remoção do predecessor.
- Justiça: a ordem é a fila sequencial do ZooKeeper. FIFO dentro do SyncNode não estabelece ordem global entre clientes de nós diferentes.
- Starvation: os experimentos mostram atendimento de todos, inclusive com um cliente mais frequente. Isso é evidência finita; não é uma prova empírica para todas as execuções. As hipóteses e garantias vêm da receita e da operação normal.
- Deadlock: R é um único mutex. No modo barreira, nenhum nó espera enter/leave enquanto mantém R adquirido. Essa ordem evita a espera circular que ocorreria se um participante retivesse R e aguardasse os demais terminarem.

## Barreira dupla

Cada rodada usa `/barriers/round-N` com cinco participantes. Enter registra o participante e espera o limiar de membros; leave sincroniza o término. O limiar da API não é uma capacidade máxima, portanto a aplicação usa exatamente os participantes configurados.

Os cinco clientes fazem a mesma quantidade de pedidos no cenário barreira. A aplicação envia ARRIVE antes de enter, PASS depois de enter e LEAVE depois de leave. BEGIN e END continuam verificando a exclusão de R dentro de cada rodada. O observador usa conjuntos de IDs, sem contar duas vezes a chegada do mesmo nó.

COMMITTED confirma o fim individual do acesso e ocorre antes da saída coletiva. O nó ainda precisa sair da barreira antes de processar o próximo pedido. Isso não antecipa a próxima rodada.

## Observação não é autorização

BEGIN incrementa ocupação mesmo se outro participante estiver ativo. Não existe espera por active=0 nem resposta concedendo permissão. OK apenas confirma que o registro ocorreu antes do sleep. END é registrado após o sleep, antes da liberação do mutex. O contador cobre conservadoramente o intervalo observado e dispensa comparar timestamps físicos de hosts diferentes.

Um teste sem mutex precisa demonstrar max_active>1 e overlap>0; uma execução sem sobreposição nesse controle é reprovada pelo conferidor. Um erro de contagem, evento duplicado ou cliente sem COMMITTED também não pode ser confundido com sucesso.

## Custo: distinguir as unidades

1. Mensagens da aplicação: REQUEST/COMMITTED são duas por acesso. HELLO/READY/BYE acrescentam três por conexão de cliente.
2. Instrumentação: BEGIN/END com OK somam quatro por acesso. ARRIVE/PASS/LEAVE com OK somam seis extras no modo barreira. DONE/OK soma duas por nó.
3. Chamadas Java: contam-se acquire e release da receita, uma vez por acesso protegido. Não são contagens de pacotes.
4. Evidência de fila: getParticipantNodes produz uma consulta adicional por aquisição e é contada separadamente.
5. Servidor ZooKeeper: mntr fornece pacotes recebidos/enviados no intervalo. Inclui lock, barreira, sessões, heartbeat e consultas. Não isola o lock nem representa pacotes TCP capturados.

A receita usa criação sequencial, listagem, possível watch e remoção. Reavaliações e concorrência alteram a quantidade de operações; a listagem transporta uma fila proporcional ao número de participantes. Observar o predecessor evita o efeito de acordar todos a cada liberação. Não atribuímos uma constante exata de mensagens por acquire a partir da quantidade de chamadas de API.

A bateria compara 5×20 e 10×10 pedidos sob alta contenção: mesmo total de 100 e mesma faixa de duração de R. A maior quantidade de competidores pode aumentar espera mesmo com vazão limitada pelo recurso único. Não se deve comparar tempos totais de cargas de 50 e 100 como se fossem equivalentes.

## Referências

- Enunciado BCC362 TP02, seções 1–3, B2, 7 e 8.
- https://curator.apache.org/docs/recipes-shared-reentrant-lock/
- https://curator.apache.org/docs/recipes-double-barrier/
- https://zookeeper.apache.org/doc/r3.9.3/recipes.html
- Implementação e dependência efetivamente usadas: Apache Curator 5.7.1.
