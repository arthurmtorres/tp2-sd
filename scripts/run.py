#!/usr/bin/env python3
"""Inicia contêineres separados por padrão, reúne logs e verifica o experimento completo."""
import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

from verify import verify

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = ("nosync", "normal", "contention", "unequal", "barrier")


def readable(line):
    """Oculta só o UUID de proteção do Curator na exibição, preservando a sequência."""
    return re.sub(r"_c_[0-9a-fA-F-]{36}-lock-", "lock-", line)


def terminal_summary(summary, directory):
    """Apresenta as métricas sem alterar o formato dos arquivos de evidência."""
    names = {"normal": "Carga normal", "contention": "Alta contenção", "barrier": "Barreira dupla",
             "nosync": "Sem sincronização", "unequal": "Carga desigual"}

    def row(label, value):
        return f"  {label:<29} : {value}"

    def number(key, unit):
        value = f"{float(summary[key]):.3f}".replace(".", ",")
        return f"{value} {unit}"

    status = summary["status"]
    if summary["scenario"] == "nosync" and status == "VIOLACAO":
        status = "Sobreposição detectada (esperada)"
    lines = ["", "=" * 60,
             f"RESUMO: {names.get(summary['scenario'], summary['scenario'])}", "=" * 60,
             row("Status do observador", status),
             row("Semente", summary["seed"]),
             row("Nós do Cluster Sync", summary["nodes"]),
             row("Pedidos concluídos", f"{summary['completed']}/{summary['expected']}"),
             row("Acessos ainda ativos", summary["active"]), "",
             row("Tempo medido pelo observador", number("total_s", "s")),
             row("Tempo total do comando", number("wall_s", "s")),
             row("Espera mediana pelo lock", number("median_wait_ms", "ms")),
             row("Vazão", number("throughput_per_s", "pedidos/s")), "",
             row("Máximo de acessos simultâneos", summary["max_active"]),
             row("Sobreposições", summary["overlaps"]),
             row("Violações da barreira", summary["barrier_violations"]),
             row("Eventos inválidos", summary["invalid_events"]),
             row("Nós finalizados", f"{summary['done_nodes']}/{summary['nodes']}"),
             row("Mensagens do observador", summary["observer_messages"]), "",
             "Pedidos concluídos por cliente:"]
    for item in summary["per_client"].split(";"):
        client, count = item.split("=", 1)
        lines.append(row(client, count))
    lines += ["", f">> VERIFICADO: {summary['completed']}/{summary['expected']} pedidos",
              row("Código de saída", summary["return_code"]), "",
              "Arquivos da execução:", "  Logs:", f"    {directory}",
              "  Resumo acumulado:", "    results/summary.csv", "=" * 60]
    return "\n".join(lines)


def zk_stats(address):
    """Contadores do servidor: incluem sessões, pings e operações de evidência."""
    try:
        host, port = address.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=2) as connection:
            connection.sendall(b"mntr")
            chunks = []
            while block := connection.recv(8192): chunks.append(block)
        data = dict(line.split("\t", 1) for line in b"".join(chunks).decode().splitlines() if "\t" in line)
        return {"received": int(data["zk_packets_received"]), "sent": int(data["zk_packets_sent"])}
    except (OSError, KeyError, ValueError):
        return None


@dataclass
class Child:
    name: str
    process: subprocess.Popen
    ready: threading.Event
    reader: threading.Thread
    container: str | None


class Experiment:
    def __init__(self, scenario, seed, run_id=None, terminal_log=True):
        self.scenario, self.seed = scenario, seed
        self.nodes = int(os.environ.get("N", "5"))
        self.requests = int(os.environ.get("REQ", "10"))
        self.demo = os.environ.get("DEMO", "0") == "1"
        if not (1 if self.demo else 5) <= self.nodes <= 20: raise ValueError("N deve estar entre 5 e 20 (DEMO=1 permite menos)")
        if not (1 if self.demo else 10) <= self.requests <= 50: raise ValueError("REQ deve estar entre 10 e 50")
        self.counts = {f"c{i}": self.requests * (4 if scenario == "unequal" and i == 1 else 1) for i in range(1, self.nodes + 1)}
        if max(self.counts.values()) > 50: raise ValueError("No unequal, use REQ de 10 a 12: c1 faz 4*REQ, com limite de 50")
        self.cs_min, self.cs_max = int(os.environ.get("CS_MIN", "200")), int(os.environ.get("CS_MAX", "1000"))
        if not 200 <= self.cs_min <= self.cs_max <= 1000: raise ValueError("A seção crítica deve durar de 200 a 1000 ms")
        self.zk = os.environ.get("ZK", "localhost:2181")
        self.docker = os.environ.get("USE_DOCKER", "1") == "1"
        self.live = os.environ.get("LIVE", "1") != "0"
        self.timeout = float(os.environ.get("TIMEOUT", "1800"))
        self.obs_port = int(os.environ.get("OBS_PORT", "9000"))
        self.base_port = int(os.environ.get("BASE_PORT", "7000"))
        suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        self.run_id = run_id or f"{scenario}-n{self.nodes}-r{self.requests}-s{seed}-{suffix}"
        self.directory = ROOT / "logs" / self.run_id
        self.directory.mkdir(parents=True, exist_ok=run_id is not None)
        self.events = queue.Queue()
        self.children = []
        self.colors = sys.stdout.isatty() and "NO_COLOR" not in os.environ
        self.combined = (self.directory / "terminal.log").open("w", encoding="utf-8", buffering=1) if terminal_log else None
        self.config = dict(run_id=self.run_id, scenario=scenario, seed=seed, nodes=self.nodes,
                           client_requests=self.counts, sync=scenario != "nosync", barrier=scenario == "barrier",
                           cs_min_ms=self.cs_min, cs_max_ms=self.cs_max, zk=self.zk, demo=self.demo,
                           observer_port=self.obs_port, base_port=self.base_port,
                           think_min_ms=1000 if scenario in ("normal", "unequal", "barrier") else 0,
                           think_max_ms=5000 if scenario in ("normal", "unequal", "barrier") else 0)
        (self.directory / "config.json").write_text(json.dumps(self.config, indent=2))

    def commands(self, start_at=0, network=False):
        """Mesmos argumentos para processos locais e serviços do Compose."""
        observer_host = "observer" if network else "localhost"
        observer = ["--port", self.obs_port, "--nodes", self.nodes, "--expected", sum(self.counts.values()),
                    "--client-requests", ",".join(f"{client}:{count}" for client, count in self.counts.items()),
                    "--label", self.scenario, "--seed", self.seed, "--csv", f"logs/{self.run_id}/observer.csv"]
        if self.scenario == "barrier": observer += ["--barrier"]
        commands = {"observer": ("Observer", observer)}
        for i in range(1, self.nodes + 1):
            node = ["--id", f"n{i}", "--port", self.base_port + i, "--zk", self.zk, "--ns", self.run_id,
                    "--observer", f"{observer_host}:{self.obs_port}",
                    "--participants", self.nodes, "--seed", self.seed, "--cs-min", self.cs_min, "--cs-max", self.cs_max]
            if self.scenario == "nosync": node += ["--no-sync"]
            if self.scenario == "barrier": node += ["--barrier"]
            commands[f"node-{i}"] = ("SyncNode", node)
        for i in range(1, self.nodes + 1):
            pause = self.scenario in ("normal", "barrier", "unequal") and not (self.scenario == "unequal" and i == 1)
            node_host = f"node-{i}" if network else "localhost"
            client = ["--id", f"c{i}", "--node", f"{node_host}:{self.base_port + i}",
                      "--requests", self.counts[f"c{i}"], "--think-min", 1000 if pause else 0,
                      "--think-max", 5000 if pause else 0, "--seed", self.seed, "--start-at", start_at]
            if self.demo: client += ["--demo"]
            commands[f"client-{i}"] = ("Client", client)
        return commands

    def process_command(self, name, main, arguments):
        container = "tp2-" + self.run_id + "-" + name if self.docker else None
        java = ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-cp", "target/tp2-sync.jar", "tp2." + main, *map(str, arguments)]
        command = java
        if self.docker:
            command = ["docker", "run", "--rm", "--network", "host", "--name", container,
                       "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{ROOT}:/app", "-w", "/app",
                       "eclipse-temurin:17.0.17_10-jre-jammy", *java]
        return command, container

    def launch(self, name, main, arguments):
        command, container = self.process_command(name, main, arguments)
        print(f">> Iniciando {name}: " + (f"contêiner {container}" if container else "processo Java"), flush=True)
        ready = threading.Event()
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8", bufsize=1, start_new_session=True)

        def read():
            with (self.directory / f"{name}.log").open("w", encoding="utf-8", buffering=1) as log:
                for line in process.stdout:
                    log.write(line)
                    self.events.put((name, line.rstrip("\n")))
                    if re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s+\S+\s+READY\s", line): ready.set()
        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        child = Child(name, process, ready, reader, container)
        self.children.append(child)
        return child

    def pump(self, timeout=.1):
        try: name, line = self.events.get(timeout=timeout)
        except queue.Empty: return
        line = readable(line)
        plain = f"{name:<10} | {line}"
        if self.combined: self.combined.write(plain + "\n")
        # O resumo legível é exibido após reunir os logs e verificar o cenário.
        if name == "observer" and re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s+\S+\s+SUMMARY\s", line):
            return
        if self.live or " ERROR " in line:
            if self.colors:
                color = 36 if name == "observer" else 32 if name.startswith("client") else (33, 35, 34)[int(name.split("-")[-1]) % 3]
                print(f"\033[{color}m{name:<10}\033[0m | {line}", flush=True)
            else: print(plain, flush=True)

    def wait_ready(self, children, deadline):
        while not all(child.ready.is_set() for child in children):
            self.pump()
            if time.monotonic() > deadline: raise TimeoutError("Timeout esperando READY; consulte os logs")
            for child in children:
                if child.process.poll() is not None: raise RuntimeError(f"{child.name} encerrou antes de READY")

    def cleanup(self):
        for child in self.children:
            if child.process.poll() is None:
                try: os.killpg(child.process.pid, signal.SIGTERM)
                except ProcessLookupError: pass
        for child in self.children:
            try: child.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.process.pid, signal.SIGKILL)
                child.process.wait()
            if child.container:
                subprocess.run(["docker", "rm", "-f", child.container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
            child.reader.join(timeout=3)
        while not self.events.empty(): self.pump(0)
        if self.combined: self.combined.close()

    def stats(self):
        return zk_stats(self.zk)

    def run(self):
        started = time.monotonic()
        expected_code = 2 if self.scenario == "nosync" else 0
        stats_before = self.stats()
        print(f"\n>> {self.run_id} | clientes={self.nodes} nós={self.nodes} pedidos={sum(self.counts.values())}", flush=True)
        if self.docker:
            print(f">> Docker: {2 * self.nodes + 1} contêineres nesta execução (1 observador + {self.nodes} nós + {self.nodes} clientes), além do ZooKeeper.", flush=True)
            print(f">> Para acompanhar em outro terminal: docker ps --filter name=tp2-{self.run_id}", flush=True)
        if self.demo: print(">> DEMONSTRAÇÃO REDUZIDA: não substitui os experimentos mínimos do professor.", flush=True)
        try:
            commands = self.commands()
            observer = self.launch("observer", *commands["observer"])
            self.wait_ready([observer], min(started + self.timeout, time.monotonic() + 45))
            workers = [self.launch(f"node-{i}", *commands[f"node-{i}"]) for i in range(1, self.nodes + 1)]
            self.wait_ready(workers, min(started + self.timeout, time.monotonic() + 60))
            commands = self.commands(start_at=int(time.time() * 1000) + 1000)
            for i in range(1, self.nodes + 1):
                self.launch(f"client-{i}", *commands[f"client-{i}"])
            while any(child.process.poll() is None for child in self.children):
                self.pump()
                if time.monotonic() - started > self.timeout: raise TimeoutError("Tempo total excedido; encerrando todos os processos")
                for child in self.children:
                    code = child.process.poll()
                    allowed = expected_code if child.name == "observer" else 0
                    if code is not None and code != allowed: raise RuntimeError(f"{child.name} encerrou com código {code}; esperado {allowed}")
            for child in self.children: child.reader.join()
            while not self.events.empty(): self.pump(0)
            stats_after = self.stats()
            return self.summarize(started, stats_before, stats_after)
        finally:
            self.cleanup()

    def summarize(self, started, stats_before, stats_after):
        """Verifica e exporta o mesmo resumo em todos os modos de execução."""
        expected_code = 2 if self.scenario == "nosync" else 0
        checked = verify(self.directory)
        (self.directory / "verification.json").write_text(json.dumps(checked, indent=2, ensure_ascii=False))
        if not checked["passed"]: raise RuntimeError("Verificação independente falhou: " + "; ".join(checked["errors"]))
        with (self.directory / "observer.csv").open() as file: summary = next(csv.DictReader(file))
        summary.update(run_id=self.run_id, requests_per_client=self.requests, cs_min_ms=self.cs_min, cs_max_ms=self.cs_max,
                       demo=self.demo, wall_s=round(time.monotonic() - started, 6),
                       client_request_messages=2 * sum(self.counts.values()), connection_messages=3 * self.nodes,
                       **checked["node_counters"])
        for key in ("received", "sent"):
            summary["zk_packets_" + key] = stats_after[key] - stats_before[key] if stats_before and stats_after else ""
        summary["zk_metrics_available"] = bool(stats_before and stats_after)
        summary["verified"] = True
        summary["return_code"] = expected_code
        (self.directory / "summary.json").write_text(json.dumps(summary, indent=2))
        result_file = ROOT / "results/summary.csv"
        result_file.parent.mkdir(exist_ok=True)
        header = not result_file.exists() or result_file.stat().st_size == 0
        if not header:
            with result_file.open() as file:
                if next(csv.reader(file)) != list(summary): raise RuntimeError("CSV de outra versão; mova results/summary.csv antes de continuar")
        with result_file.open("a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(summary))
            if header: writer.writeheader()
            writer.writerow(summary)
        print(terminal_summary(summary, self.directory.relative_to(ROOT)), flush=True)
        if not stats_before or not stats_after:
            print("Tráfego ZooKeeper: não medido (mntr indisponível).", flush=True)
        return expected_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=SCENARIOS, nargs="?", default="normal")
    parser.add_argument("seed", type=int, nargs="?", default=1)
    arguments = parser.parse_args()
    try:
        if not (ROOT / "target/tp2-sync.jar").exists(): raise RuntimeError("Compile primeiro: mvn clean package")
        executable = "docker" if os.environ.get("USE_DOCKER", "1") == "1" else "java"
        if not shutil.which(executable): raise RuntimeError(f"Executável não encontrado: {executable}")
        raise SystemExit(Experiment(arguments.scenario, arguments.seed).run())
    except (ValueError, OSError, RuntimeError, TimeoutError, KeyboardInterrupt) as error:
        print(f"ERRO: {error}", file=sys.stderr)
        raise SystemExit(1)
