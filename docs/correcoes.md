# O que foi corrigido

- Scripts funcionam com bash mesmo em extrações que descartem a permissão executável; a distribuição preserva o bit quando o extrator suporta.
- Bateria confere os códigos esperados e a conclusão real; não imprime sucesso depois de uma falha.
- Todos os processos são acompanhados, têm prazo total e são encerrados quando a execução falha.
- Prontidão usa READY; não depende apenas de sleeps fixos.
- Observer identifica pedidos ativos/encerrados e participantes únicos. Eventos duplicados, END inválido, nó/rodada divergentes e pedidos não previstos são acusados.
- Conclusão exige zero acessos ativos, todos os nós encerrados e as quantidades esperadas por cliente.
- ARRIVE/PASS/LEAVE são registrados com rodada e sequência central; a barreira não é validada só por contagem bruta.
- Mediana par corrigida e percentil com critério explícito. CSV usa ponto decimal e escape adequado.
- Verificação independente cruza os eventos completos com todos os RECEIVED_COMMITTED.
- ResourceLock separa acquire/release da seção crítica simulada; queue e znode real aparecem na aquisição.
- BYE de um cliente não encerra o nó antes dos demais clientes configurados.
- Parâmetros respeitam os intervalos do TP. Cargas reduzidas exigem DEMO=1 e recebem aviso explícito.
- Logs são alinhados, reunidos ao vivo no terminal e coloridos por processo quando há suporte; arquivos permanecem sem sequências de cor.
- Mensagens da aplicação/instrumentação, chamadas da API, consultas de evidência e tráfego agregado têm medidas distintas.
- Comparação de escala conserva 100 acessos: 5 clientes×20 e 10 clientes×10.
- Foram acrescentados testes de regressão, resultados reais, discussão das garantias/limites, roteiro e apresentação para o seminário.

O arquivo results/relatorio.md é gerado dos experimentos concluídos. Não há resultados simulados apresentados como medidos. O documento docs/validacao.md identifica o ambiente e quais caminhos foram efetivamente executados.
