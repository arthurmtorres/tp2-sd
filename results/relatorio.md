# Resultados reais — ZooKeeper + Apache Curator

Configurações e logs individuais estão em logs/<run_id>/. Espera é medida com relógio monotônico no próprio nó.

| Cenário | Semente | Nós | Pedidos | Total BEGIN–END (s) | Mediana (ms) | P95 (ms) | Máx. ativos | Sobreposições |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nosync | 1 | 5 | 50/50 | 6.000 | 0.000 | 0.000 | 5 | 49 |
| normal | 1 | 5 | 50/50 | 37.266 | 87.015 | 1436.624 | 1 | 0 |
| contention | 1 | 5 | 50/50 | 27.616 | 1976.439 | 3047.608 | 1 | 0 |
| normal | 2 | 5 | 50/50 | 45.813 | 140.130 | 2107.048 | 1 | 0 |
| contention | 2 | 5 | 50/50 | 28.923 | 2217.661 | 3011.710 | 1 | 0 |
| normal | 3 | 5 | 50/50 | 39.258 | 120.866 | 1652.396 | 1 | 0 |
| contention | 3 | 5 | 50/50 | 28.426 | 2113.672 | 2965.777 | 1 | 0 |
| barrier | 1 | 5 | 50/50 | 57.609 | 1150.415 | 2841.492 | 1 | 0 |
| unequal | 1 | 5 | 80/80 | 47.736 | 638.237 | 1827.200 | 1 | 0 |
| contention | 1 | 5 | 100/100 | 59.427 | 2404.410 | 3071.388 | 1 | 0 |
| contention | 1 | 10 | 100/100 | 59.182 | 5112.003 | 6365.913 | 1 | 0 |

Foram concluídos 680 pedidos, sendo 630 protegidos por mutex. A conferência independente também validou os COMMITTED recebidos pelos clientes.

## Contenção e escala

Normal e alta contenção usam três repetições, sementes 1, 2 e 3. A alta contenção retira somente a pausa entre pedidos; preserva o intervalo da seção crítica.
- normal: 3 repetições; média das medianas de espera = 116.004 ms; intervalo entre medianas = 87.015–140.130 ms.
- contention: 3 repetições; média das medianas de espera = 2102.591 ms; intervalo entre medianas = 1976.439–2217.661 ms.

Comparação de escala com a mesma carga total de 100 acessos e a mesma distribuição de duração de R:

- 5 nós, 20 pedidos/cliente: mediana 2404.410 ms, vazão 1.683 acessos/s.
- 10 nós, 10 pedidos/cliente: mediana 5112.003 ms, vazão 1.690 acessos/s.

## Justiça e progresso

Na carga desigual: c1=40;c2=10;c3=10;c4=10;c5=10. Todos concluíram. O FIFO local organiza um nó; entre nós, a ordem é a dos znodes sequenciais no ZooKeeper, e não a do relógio físico dos clientes.
A ausência de starvation neste conjunto finito é uma observação, não uma prova. Sob as hipóteses de operação normal, a receita atende os participantes pela ordem da fila do ZooKeeper; atrasos de rede e escalonamento ainda alteram quando cada pedido entra nessa fila.

## Barreira e deadlock

A entrada espera os cinco participantes e a saída impede a próxima rodada enquanto a atual está em execução. Os logs do observador registram ARRIVE, PASS, BEGIN, END e LEAVE com rodada e IDs únicos. O mutex é liberado antes de leave(), evitando esperar os outros participantes enquanto se retém R.

## Mensagens, chamadas e tráfego

As unidades abaixo são diferentes e não devem ser somadas como se fossem iguais. Chamadas Java não equivalem a pacotes do protocolo ZooKeeper; pacotes do ZooKeeper também não são segmentos TCP capturados.

| Execução | REQUEST/COMMITTED | Instrumentação + OK | acquire/release | Leituras extra da fila | ZK recebidos/enviados |
| --- | --- | --- | --- | --- | --- |
| nosync n=5 r=10 s=1 | 100 | 210 | 0/0 | 0 | 21/23 |
| normal n=5 r=10 s=1 | 100 | 210 | 50/50 | 50 | 295/326 |
| contention n=5 r=10 s=1 | 100 | 210 | 50/50 | 50 | 333/384 |
| normal n=5 r=10 s=2 | 100 | 210 | 50/50 | 50 | 291/321 |
| contention n=5 r=10 s=2 | 100 | 210 | 50/50 | 50 | 337/388 |
| normal n=5 r=10 s=3 | 100 | 210 | 50/50 | 50 | 295/326 |
| contention n=5 r=10 s=3 | 100 | 210 | 50/50 | 50 | 337/388 |
| barrier n=5 r=10 s=1 | 100 | 510 | 50/50 | 50 | 799/944 |
| unequal n=5 r=10 s=1 | 160 | 330 | 80/80 | 80 | 481/546 |
| contention n=5 r=20 s=1 | 200 | 410 | 100/100 | 100 | 633/734 |
| contention n=10 r=10 s=1 | 200 | 420 | 100/100 | 100 | 663/764 |

Teoricamente, a receita cria um znode efêmero sequencial, lista a fila, observa o predecessor quando precisa esperar e remove seu znode ao liberar. Notificações provocam reavaliação da condição; disputas e corridas podem acrescentar operações. Observar apenas o predecessor evita acordar todos os concorrentes a cada liberação. A lista de participantes tem tamanho proporcional à concorrência.
A aplicação registra uma leitura adicional getParticipantNodes() por acesso protegido para exibir a fila. BEGIN/END + OK usam quatro mensagens por acesso. Na barreira, ARRIVE/PASS/LEAVE + OK acrescentam seis; DONE/OK acrescentam duas por nó na execução. O handshake HELLO/READY e BYE acrescenta três mensagens por cliente, registradas separadamente.
Os contadores mntr medem o tráfego agregado do serviço durante cada execução: incluem estabelecimento/fechamento de sessões, heartbeat, mutex, barreira e consultas de evidência. Não isolam o custo exato do lock nem eliminam o próprio monitoramento. Não há alegação de uma constante exata de mensagens por acquire. As mensagens TCP do observador e dos clientes ficam fora do servidor ZooKeeper e são contadas separadamente.

## Limites da avaliação

Os experimentos assumem serviços disponíveis e ausência de perdas/quedas, conforme o TP2. Não avaliam tolerância a falhas, replicação, fencing ou persistência do recurso. COMMITTED confirma a simulação individual, não uma gravação em banco. A espera reportada é a do acquire; não inclui fila local ou espera de entrada da barreira. A janela total_s vai do primeiro BEGIN ao último END; wall_s inclui também preparação e encerramento.
