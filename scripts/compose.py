#!/usr/bin/env python3
"""Prepara os argumentos do Compose e reaproveita a verificação dos experimentos."""
import json
import os
from pathlib import Path
import sys
import time

from run import Experiment, ROOT, SCENARIOS, zk_stats

STATE = Path("/state")


def prepare():
    # O volume começa como root; somente a preparação precisa ajustar seu dono.
    uid, gid = int(os.environ.get("LOCAL_UID", "1000")), int(os.environ.get("LOCAL_GID", "1000"))
    os.chown(STATE, uid, gid)
    if not (ROOT / "logs").exists():
        (ROOT / "logs").mkdir()
        os.chown(ROOT / "logs", uid, gid)
    os.setgid(gid)
    os.setuid(uid)
    scenario = os.environ.get("SCENARIO", "normal")
    if scenario not in SCENARIOS:
        raise ValueError(f"SCENARIO deve ser um de: {', '.join(SCENARIOS)}")
    experiment = Experiment(scenario, int(os.environ.get("SEED", "1")), terminal_log=False)
    try:
        if experiment.nodes != 5:
            raise ValueError("O Compose declara 5 nós; para variar N, use scripts/run.sh")
        if not (ROOT / "target/tp2-sync.jar").is_file():
            raise ValueError("Compile primeiro: bash scripts/build.sh")
        for name, (main, arguments) in experiment.commands(network=True).items():
            (STATE / f"{name}.args").write_text("\n".join(map(str, (main, *arguments))) + "\n")
            expected = 2 if name == "observer" and scenario == "nosync" else 0
            (STATE / f"{name}.expected").write_text(str(expected))
            (STATE / f"{name}.exit").unlink(missing_ok=True)
        (STATE / "experiment.json").write_text(json.dumps(dict(
            run_id=experiment.run_id, scenario=scenario, seed=experiment.seed,
            started=time.monotonic(), stats_before=zk_stats(experiment.zk))))
        (STATE / "run-id").write_text(experiment.run_id)
        print(f"Preparado: {experiment.run_id} | 5 clientes + 5 nós Sync", flush=True)
    finally:
        experiment.cleanup()


def finish():
    state = json.loads((STATE / "experiment.json").read_text())
    experiment = Experiment(state["scenario"], state["seed"], run_id=state["run_id"], terminal_log=False)
    try:
        for name in experiment.commands():
            actual = (STATE / f"{name}.exit").read_text()
            expected = (STATE / f"{name}.expected").read_text()
            if actual != expected:
                raise RuntimeError(f"{name} encerrou com código {actual}; esperado {expected}")
        # summarize verifica também a conclusão e a sobreposição esperada no nosync.
        experiment.summarize(state["started"], state["stats_before"], zk_stats(experiment.zk))
    finally:
        experiment.cleanup()


if __name__ == "__main__":
    try:
        {"prepare": prepare, "finish": finish}[sys.argv[1]]()
    except (OSError, ValueError, RuntimeError) as error:
        print(f"ERRO: {error}", file=sys.stderr)
        raise SystemExit(1)
