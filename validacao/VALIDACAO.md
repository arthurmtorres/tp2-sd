# Validação da versão corrigida

- Compilação: Maven 3.9.16, Temurin Java 17.0.20.1, ZooKeeper 3.9.3.
- Cinco cenários reais, semente 11, cinco nós e cinco clientes, total de 280 pedidos concluídos.
- Sem proteção: 50 pedidos, pico de cinco acessos, 49 sobreposições detectadas. Resultado esperado aceito pelo script.
- Contenção, barreira dupla, carga desigual e carga normal: todos os pedidos concluídos, zero sobreposições, zero violações de barreira e zero violações de protocolo.
- Seis regressões do observador aprovadas, executadas com Java 25 e classes compiladas para Java 17. Cobrem mediana, locale pt_BR, END sem BEGIN, eventos duplicados, sobreposição e saída prematura da última barreira.
- REQ=51 rejeitado antes da execução.
- TIMEOUT=2 interrompeu o cenário com código 124 e não deixou processos Java da aplicação ativos.
- Sintaxe dos scripts verificada com bash -n.

Os logs desta pasta pertencem somente à versão corrigida. Esta validação usa uma repetição de cada cenário para conferir as alterações. Para os experimentos finais do trabalho, execute scripts/run_all.sh, que realiza as três repetições e a comparação com dez nós.

Execute python3 tests/test_observer.py após compilar para repetir as regressões do observador. O código original mantém Java, ZooKeeper, Curator, mutex e barreira dupla. Os slides não fazem parte deste pacote.
