#!/usr/bin/env python3
"""Confere logs por IDs e pela sequência do observador, sem comparar relógios de hosts."""
import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import re
import statistics


def records(path):
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s+(\S+)\s+(\S+)\s*(.*)$", line)
        if match:
            actor, event, detail = match.groups()
            yield actor, event, dict(re.findall(r"(\w+)=([^\s]+)", detail))


def verify(directory):
    directory = Path(directory)
    config = json.loads((directory / "config.json").read_text())
    counts = config["client_requests"]
    nodes = config["nodes"]
    expected = {f"{client}-{i}" for client, count in counts.items() for i in range(1, count + 1)}
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    trace = defaultdict(list)
    ownership = defaultdict(set)
    znode = {}
    waits = []
    node_counters = Counter()
    for i in range(1, nodes + 1):
        node_done = 0
        for actor, event, fields in records(directory / f"node-{i}.log"):
            request = fields.get("request_id")
            if event in ("REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"):
                trace[request].append(event)
                ownership[request].add(actor)
            if event == "ACQUIRE":
                waits.append(float(fields["wait_s"]) * 1000)
                if config["sync"]:
                    znode[request] = int(re.search(r"(\d+)$", fields["znode"])[1])
            if event == "ENTER":
                require(config["cs_min_ms"] <= int(fields["sleep_ms"]) <= config["cs_max_ms"], "Duração programada fora da configuração")
            if event == "NODE_DONE":
                node_done += 1
                for key in ("recipe_acquire_calls", "recipe_release_calls", "evidence_zk_reads", "observer_messages"):
                    node_counters[key] += int(fields[key])
            require(event != "ERROR", f"Erro no nó n{i}")
        require(node_done == 1, f"Encerramento incompleto ou duplicado: n{i}")
    require(set(trace) == expected, "Pedidos dos nós diferem da carga esperada")
    stages = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]
    for request, events in trace.items():
        require(events == stages and len(ownership[request]) == 1, f"Fluxo inválido/duplicado: {request}")

    sent, received = Counter(), Counter()
    for client, count in counts.items():
        number = int(client[1:])
        finished = 0
        for actor, event, fields in records(directory / f"client-{number}.log"):
            if event == "REQUEST": sent[fields["request_id"]] += 1
            if event == "RECEIVED_COMMITTED": received[fields["request_id"]] += 1
            if event == "CLIENT_DONE":
                finished += 1
                require(int(fields["completed"]) == count, f"Contagem incorreta: {client}")
        require(finished == 1, f"Cliente não encerrou corretamente: {client}")
    for label, counter in (("REQUEST", sent), ("RECEIVED_COMMITTED", received)):
        require(set(counter) == expected and all(n == 1 for n in counter.values()), f"{label}: há pedidos ausentes, extras ou duplicados")

    active, done = {}, set()
    begins, ends = Counter(), Counter()
    arrivals, passes, finished_rounds, leaves = (defaultdict(set) for _ in range(4))
    max_active = overlaps = 0
    seq = 0
    acquisition_order = []
    for actor, event, fields in records(directory / "observer.log"):
        if event in ("INVALID_EVENT", "ERROR", "VIOLATION_BARRIER"):
            errors.append(f"Observador registrou {event}")
        if event not in ("BEGIN", "END", "ARRIVE", "PASS", "LEAVE", "DONE"):
            continue
        current = int(fields["seq"])
        require(current > seq, "Sequência do observador não é estritamente crescente")
        seq = current
        node = fields["node"]
        number = int(fields.get("round", 0))
        if event == "BEGIN":
            request = fields["request_id"]
            begins[request] += 1
            require(request not in active, f"BEGIN duplicado: {request}")
            require(ownership.get(request) == {node}, f"Nó divergente: {request}")
            if config["barrier"]:
                require(len(arrivals[number]) == nodes and node in passes[number], "Entrada antes de todos chegarem à barreira")
            active[request] = (node, number)
            max_active = max(max_active, len(active))
            overlaps += int(len(active) > 1)
            require(int(fields["active"]) == len(active), "Contador de ativos do observador diverge dos IDs")
            if config["sync"]: acquisition_order.append(znode.get(request, -1))
        elif event == "END":
            request = fields["request_id"]
            ends[request] += 1
            require(active.pop(request, None) == (node, number), f"END sem BEGIN correspondente: {request}")
            finished_rounds[number].add(node)
            require(int(fields["active"]) == len(active), "Contagem de ativos incorreta após END")
        elif event == "ARRIVE":
            require(node not in arrivals[number], "ARRIVE duplicado")
            if number > 1:
                require(len(finished_rounds[number - 1]) == nodes and node in leaves[number - 1], "Rodada seguinte começou antes do término da anterior")
            arrivals[number].add(node)
        elif event == "PASS":
            require(len(arrivals[number]) == nodes and node not in passes[number], "PASS antecipado ou duplicado")
            passes[number].add(node)
        elif event == "LEAVE":
            require(len(finished_rounds[number]) == nodes and node not in leaves[number], "Saída da barreira antecipada ou duplicada")
            leaves[number].add(node)
        elif event == "DONE":
            require(node not in done, "DONE duplicado")
            done.add(node)
    require(not active, "Há acessos ainda ativos")
    require(done == {f"n{i}" for i in range(1, nodes + 1)}, "Nem todos os nós encerraram")
    require(set(begins) == expected and set(ends) == expected and all(v == 1 for v in begins.values())
            and all(v == 1 for v in ends.values()), "BEGIN/END não correspondem aos pedidos esperados")
    if config["barrier"]:
        require(all(len(leaves[r]) == nodes for r in arrivals), "Barreira final incompleta")
    if config["sync"]:
        require(max_active == 1 and overlaps == 0, "Exclusão mútua violada")
        require(acquisition_order == sorted(acquisition_order) and len(set(acquisition_order)) == len(expected), "Aquisições fora da ordem dos znodes")
    else:
        require(max_active > 1 and overlaps > 0, "O controle sem proteção não demonstrou sobreposição")
    rows = list(csv.DictReader((directory / "observer.csv").open()))
    require(len(rows) == 1, "CSV do observador deve conter exatamente uma execução")
    summary = rows[0]
    require(int(summary["completed"]) == len(received) == len(expected), "Resumo não corresponde aos COMMITTED recebidos")
    require(int(summary["active"]) == 0 and int(summary["invalid_events"]) == 0, "Resumo possui estado inválido")
    require(int(summary["overlaps"]) == overlaps and int(summary["max_active"]) == max_active, "Resumo de concorrência inconsistente")
    require(abs(float(summary["median_wait_ms"]) - statistics.median(waits)) < .002, "Mediana divergente dos logs")
    require(int(summary["observer_messages"]) == node_counters["observer_messages"], "Contagem de mensagens de instrumentação inconsistente")
    expected_calls = len(expected) if config["sync"] else 0
    require(node_counters["recipe_acquire_calls"] == node_counters["recipe_release_calls"] == node_counters["evidence_zk_reads"] == expected_calls,
            "Contagem de chamadas da receita/leitura de evidência inconsistente")
    return {"passed": not errors, "errors": errors, "completed": len(received), "max_active": max_active,
            "overlaps": overlaps, "per_client": counts, "node_counters": dict(node_counters)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = verify(args.directory)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    raise SystemExit(0 if result["passed"] else 1)
