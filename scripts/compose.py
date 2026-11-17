#!/usr/bin/env python3
"""Executa um cenário nos 12 contêineres já iniciados pelo Compose."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from run import Experiment, ROOT, SCENARIOS, zk_stats

COMPOSE = ["docker", "compose", "--progress", "plain", "-f", str(ROOT / "docker-compose.yml")]
SERVICES = {"zookeeper", "observer", *[f"{kind}-{i}" for kind in ("node", "client") for i in range(1, 6)]}


@contextmanager
def session():
    # Impede que dois comandos disputem as mesmas portas dentro dos contêineres.
    key = hashlib.sha256(str(ROOT).encode()).hexdigest()[:16]
    path = Path(tempfile.gettempdir()) / f"tp2-compose-{key}.lock"
    with path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Já existe um cenário em execução neste projeto") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class ComposeExperiment(Experiment):
    def __init__(self, scenario, seed):
        super().__init__(scenario, seed)
        self.docker = False  # As JVMs usam docker compose exec, sem criar contêineres.
        self.zk = "zookeeper:2181"
        self.config["zk"] = self.zk
        (self.directory / "config.json").write_text(json.dumps(self.config, indent=2))

    def commands(self, start_at=0, network=True):
        return super().commands(start_at, network=True)

    def process_command(self, name, main, arguments):
        command = [*COMPOSE, "exec", "-T", "--interactive=false", "-e", f"TIMEOUT={self.timeout}",
                   name, "bash", "scripts/compose-service.sh", "--run", self.run_id,
                   main, *map(str, arguments)]
        return command, None

    def stats(self):
        return zk_stats(os.environ.get("ZK_METRICS", "localhost:2181"))

    def cleanup(self):
        try:
            # Encerrar o cliente docker exec sozinho não encerra sua JVM remota.
            for child in self.children:
                if child.process.poll() is None:
                    try:
                        result = subprocess.run(
                            [*COMPOSE, "exec", "-T", "--interactive=false", child.name,
                             "bash", "scripts/compose-service.sh", "--stop", self.run_id],
                            capture_output=True, text=True, timeout=15)
                        if result.returncode:
                            print(f"ERRO ao encerrar {child.name}: {result.stderr.strip()}", file=sys.stderr)
                    except (OSError, subprocess.TimeoutExpired) as error:
                        print(f"ERRO ao encerrar {child.name}: {error}", file=sys.stderr)
        finally:
            super().cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=SCENARIOS, nargs="?", default="normal")
    parser.add_argument("seed", type=int, nargs="?", default=1)
    args = parser.parse_args()
    if int(os.environ.get("N", "5")) != 5:
        raise ValueError("O Compose usa 5 pares; para variar N, use scripts/run.sh")
    if not (ROOT / "target/tp2-sync.jar").is_file():
        raise RuntimeError("Compile primeiro: bash scripts/build.sh (ou USE_DOCKER=1 bash scripts/build.sh)")
    with session():
        running = subprocess.run([*COMPOSE, "ps", "--status", "running", "--services"],
                                 capture_output=True, text=True, check=True)
        missing = SERVICES - set(running.stdout.split())
        if missing:
            raise RuntimeError("Inicie primeiro: docker compose up -d (faltam: " + ", ".join(sorted(missing)) + ")")
        print(">> Usando os 12 contêineres do Compose; aguarde VERIFICADO ao final.", flush=True)
        return ComposeExperiment(args.scenario, args.seed).run()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCenário interrompido; contêineres disponíveis para a próxima execução.", file=sys.stderr)
        raise SystemExit(130)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"ERRO: {error}", file=sys.stderr)
        raise SystemExit(1)
