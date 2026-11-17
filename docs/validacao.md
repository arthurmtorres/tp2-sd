# Validação desta distribuição

## Executado

- Compilação: JDK OpenJDK 17.0.19, Maven 3.9.9, `mvn clean package`.
- Doze testes Java passaram, sem falhas/erros: validação de IDs, contagem, sobreposição, barreira, mediana, percentil e CSV em locale brasileiro.
- Cinco testes Python passaram: configuração mínima, limites de carga e duração, propagação de erro da bateria e abreviação dos UUIDs no terminal.
- Bateria acadêmica completa aprovada: 11 experimentos, 680 pedidos. Os 630 acessos protegidos terminaram com no máximo um ativo. O controle sem mutex concluiu 50 e registrou max_active=5, com 49 entradas sobrepostas.
- Foram feitas três repetições da carga normal e três de alta contenção; também controle sem mutex, barreira com cinco participantes/dez rodadas, carga desigual e comparação de 5×20 com 10×10 pedidos.
- Todos os experimentos foram conferidos por scripts/verify.py: IDs, eventos, BEGIN/END, recebimento dos COMMITTED, ordem de znodes e fases da barreira.
- A medição mntr estava disponível nas 11 execuções. Os contadores do ZooKeeper são agregados e não isolam o custo exato do mutex.
- Regressão adicional: dois clientes, dez pedidos cada, no mesmo SyncNode. Todos os 20 pedidos terminaram; o nó continuou atendendo c2 depois de receber BYE de c1. Evidência em validacao/multiple-clients/.
- Apresentação: 12 slides de conteúdo e 1 de referências; tabelas/gráfico editáveis. Arquivo exportado, validado estruturalmente e todos os slides renderizados e inspecionados. Não foi aberto no PowerPoint.

## Ambiente e limites

Os experimentos usaram JVMs independentes, comunicação TCP e servidor nativo da distribuição Apache ZooKeeper 3.9.3. Não houve uso de um mock de ZooKeeper nem substituição do lock por mutex local. As falhas deliberadas dos testes unitários pertencem ao verificador e à propagação de status, não são experimentos de tolerância a falhas do protocolo.

Docker não estava instalado no ambiente da validação original. A validação adicional abaixo cobre o novo Compose. O escopo não inclui quedas, perda de mensagens, replicação, fencing ou recuperação de sessão.

O ZIP contém resultados completos da versão corrigida. Os primeiros ensaios usados durante o desenvolvimento não estão misturados na bateria aprovada.

## Validação adicional do Compose

O Compose passou a declarar um contêiner para cada um dos cinco clientes, cinco nós Sync, observador e ZooKeeper, além dos auxiliares de preparação e verificação. O código Java não foi alterado. Os serviços usam a rede interna, com prontidão confirmada pelos logs antes de iniciar os dependentes.

Foram executados três cenários completos com Docker, cinco clientes e dez pedidos por cliente:

| Cenário | Concluídos | Máximo de ativos | Sobreposições | Violações da barreira |
| --- | --- | --- | --- | --- |
| Alta contenção | 50/50 | 1 | 0 | 0 |
| Sem sincronização | 50/50 | 5 | 49 | 0 |
| Barreira dupla, dez rodadas | 50/50 | 1 | 0 | 0 |

As três execuções passaram pelo mesmo verificador independente e geraram os resumos habituais. `validacao/compose.json` identifica os diretórios dos logs e seus resultados. O observador do controle retornou 2 internamente, aceito como esperado pelo serviço; a conclusão e a sobreposição foram conferidas separadamente antes de aprovar o cenário.

Também passaram oito testes Python, incluindo o endereçamento de um cliente para seu nó, a preservação das opções dos cenários e a propagação de falhas através da gravação do log. `docker compose config --quiet` validou a configuração. Uma demonstração nativa reduzida com dois clientes e quatro pedidos também passou, confirmando a compatibilidade dos scripts existentes; ela não substitui os experimentos acadêmicos.

Essas execuções complementam as evidências originais; não substituem a bateria de três repetições da carga normal e da alta contenção.

## Contêineres por padrão nos scripts

Em 17/11/2026, os cinco cenários foram executados por `scripts/run.sh` sem definir `USE_DOCKER`. Em cada execução, `docker ps` confirmou cinco clientes, cinco nós e um observador, cada um em seu próprio contêiner, além do ZooKeeper iniciado separadamente.

A conferência usou carga reduzida (`DEMO=1 N=5 REQ=2 CS_MIN=500 CS_MAX=500`): dez pedidos em cada cenário, exceto `unequal`, com dezesseis. Todos passaram pelo verificador independente. O controle `nosync` retornou 2 e registrou cinco acessos simultâneos; os demais retornaram 0, sem sobreposição. Os oito testes Python também passaram.

`validacao/containers-default.json` registra os nomes observados e os diretórios de resultados. Essas demonstrações verificam o novo padrão de execução; não substituem a bateria acadêmica.

## Reproduzir

1. Iniciar ZooKeeper: `docker compose up -d --wait zookeeper` ou configurar ZK para um serviço existente.
2. Recompilar opcionalmente: `bash scripts/build.sh` (ou USE_DOCKER=1).
3. Rodar: `bash scripts/run_all.sh`.
4. Conferir results/battery.json, results/relatorio.md e verification.json de cada execução.

A distribuição já inclui target/tp2-sync.jar para Java 17. Os resultados preservados podem ser conferidos novamente sem iniciar Java/ZooKeeper, com `python3 scripts/verify.py logs/NOME_DA_EXECUCAO`.

## Integridade

SHA-256 do JAR incluído: `06d55d93a7fccd17b32e568c59ba3430bf0b18be08f57a4e7fc09042278bdc67`.

O manifesto validacao/arquivos.sha256 registra os arquivos entregues, exceto ele próprio. As permissões de execução dos scripts são preservadas no ZIP quando o extrator suporta; os exemplos usam bash/python3 e também funcionam sem esse bit.
