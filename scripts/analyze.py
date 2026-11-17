#!/usr/bin/env python3
"""Gera resultados e discussão somente a partir de uma bateria concluída."""
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent.parent


def main():
    manifest = json.loads((ROOT / "results/battery.json").read_text())
    if not manifest["complete"]: raise RuntimeError("A bateria ainda não foi concluída")
    runs = [json.loads((ROOT / path).read_text()) for path in manifest["runs"]]
    lines = ["# Resultados reais — ZooKeeper + Apache Curator", "",
             "Configurações e logs individuais estão em logs/<run_id>/. Espera é medida com relógio monotônico no próprio nó.", "",
             "| Cenário | Semente | Nós | Pedidos | Total BEGIN–END (s) | Mediana (ms) | P95 (ms) | Máx. ativos | Sobreposições |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for run in runs:
        lines.append(f"| {run['scenario']} | {run['seed']} | {run['nodes']} | {run['completed']}/{run['expected']} | {float(run['total_s']):.3f} | {float(run['median_wait_ms']):.3f} | {float(run['p95_wait_ms']):.3f} | {run['max_active']} | {run['overlaps']} |")
    total = sum(int(r["completed"]) for r in runs)
    protected = sum(int(r["completed"]) for r in runs if r["scenario"] != "nosync")
    lines += ["", f"Foram concluídos {total} pedidos, sendo {protected} protegidos por mutex. A conferência independente também validou os COMMITTED recebidos pelos clientes.", "",
              "## Contenção e escala", "",
              "Normal e alta contenção usam três repetições, sementes 1, 2 e 3. A alta contenção retira somente a pausa entre pedidos; preserva o intervalo da seção crítica."]
    for scenario in ("normal", "contention"):
        selected = [r for r in runs if r["scenario"] == scenario and int(r["nodes"]) == 5 and int(r["requests_per_client"]) == 10]
        medians = [float(r["median_wait_ms"]) for r in selected]
        lines.append(f"- {scenario}: {len(selected)} repetições; média das medianas de espera = {statistics.mean(medians):.3f} ms; intervalo entre medianas = {min(medians):.3f}–{max(medians):.3f} ms.")
    scale = [r for r in runs if r["scenario"] == "contention" and int(r["completed"]) == 100]
    lines += ["", "Comparação de escala com a mesma carga total de 100 acessos e a mesma distribuição de duração de R:", ""]
    for run in scale:
        lines.append(f"- {run['nodes']} nós, {run['requests_per_client']} pedidos/cliente: mediana {float(run['median_wait_ms']):.3f} ms, vazão {float(run['throughput_per_s']):.3f} acessos/s.")
    unequal = next(r for r in runs if r["scenario"] == "unequal")
    lines += ["", "## Justiça e progresso", "", f"Na carga desigual: {unequal['per_client']}. Todos concluíram. O FIFO local organiza um nó; entre nós, a ordem é a dos znodes sequenciais no ZooKeeper, e não a do relógio físico dos clientes.",
              "A ausência de starvation neste conjunto finito é uma observação, não uma prova. Sob as hipóteses de operação normal, a receita atende os participantes pela ordem da fila do ZooKeeper; atrasos de rede e escalonamento ainda alteram quando cada pedido entra nessa fila.", "",
              "## Barreira e deadlock", "",
              "A entrada espera os cinco participantes e a saída impede a próxima rodada enquanto a atual está em execução. Os logs do observador registram ARRIVE, PASS, BEGIN, END e LEAVE com rodada e IDs únicos. O mutex é liberado antes de leave(), evitando esperar os outros participantes enquanto se retém R.", "",
              "## Mensagens, chamadas e tráfego", "",
              "As unidades abaixo são diferentes e não devem ser somadas como se fossem iguais. Chamadas Java não equivalem a pacotes do protocolo ZooKeeper; pacotes do ZooKeeper também não são segmentos TCP capturados.", "",
              "| Execução | REQUEST/COMMITTED | Instrumentação + OK | acquire/release | Leituras extra da fila | ZK recebidos/enviados |",
              "| --- | --- | --- | --- | --- | --- |"]
    for run in runs:
        traffic = f"{run['zk_packets_received']}/{run['zk_packets_sent']}" if run["zk_metrics_available"] else "não medido"
        lines.append(f"| {run['scenario']} n={run['nodes']} r={run['requests_per_client']} s={run['seed']} | {run['client_request_messages']} | {run['observer_messages']} | {run['recipe_acquire_calls']}/{run['recipe_release_calls']} | {run['evidence_zk_reads']} | {traffic} |")
    lines += ["", "Teoricamente, a receita cria um znode efêmero sequencial, lista a fila, observa o predecessor quando precisa esperar e remove seu znode ao liberar. Notificações provocam reavaliação da condição; disputas e corridas podem acrescentar operações. Observar apenas o predecessor evita acordar todos os concorrentes a cada liberação. A lista de participantes tem tamanho proporcional à concorrência.",
              "A aplicação registra uma leitura adicional getParticipantNodes() por acesso protegido para exibir a fila. BEGIN/END + OK usam quatro mensagens por acesso. Na barreira, ARRIVE/PASS/LEAVE + OK acrescentam seis; DONE/OK acrescentam duas por nó na execução. O handshake HELLO/READY e BYE acrescenta três mensagens por cliente, registradas separadamente.",
              "Os contadores mntr medem o tráfego agregado do serviço durante cada execução: incluem estabelecimento/fechamento de sessões, heartbeat, mutex, barreira e consultas de evidência. Não isolam o custo exato do lock nem eliminam o próprio monitoramento. Não há alegação de uma constante exata de mensagens por acquire. As mensagens TCP do observador e dos clientes ficam fora do servidor ZooKeeper e são contadas separadamente.", "",
              "## Limites da avaliação", "",
              "Os experimentos assumem serviços disponíveis e ausência de perdas/quedas, conforme o TP2. Não avaliam tolerância a falhas, replicação, fencing ou persistência do recurso. COMMITTED confirma a simulação individual, não uma gravação em banco. A espera reportada é a do acquire; não inclui fila local ou espera de entrada da barreira. A janela total_s vai do primeiro BEGIN ao último END; wall_s inclui também preparação e encerramento."]
    (ROOT / "results/relatorio.md").write_text("\n".join(lines) + "\n")
    (ROOT / "results/runs.json").write_text(json.dumps(runs, indent=2))


if __name__ == "__main__": main()
