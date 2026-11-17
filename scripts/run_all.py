#!/usr/bin/env python3
"""Executa a bateria acadêmica e só conclui se cada cenário for verificado."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent


def main():
    # N=5 e REQ=10 atendem ao mínimo. O controle nosync deve retornar 2.
    cases = [("nosync", 1, 5, 10)]
    cases += [(scenario, seed, 5, 10) for seed in (1, 2, 3) for scenario in ("normal", "contention")]
    cases += [("barrier", 1, 5, 10), ("unequal", 1, 5, 10)]
    # Mesmo total (100), mesma duração de R e think=0 na comparação de escala.
    cases += [("contention", 1, 5, 20), ("contention", 1, 10, 10)]
    manifest = {"created": datetime.now(timezone.utc).isoformat(), "runs": [], "complete": False}
    result_dir = ROOT / "results"
    result_dir.mkdir(exist_ok=True)
    path = result_dir / "battery.json"
    environment = os.environ.copy()
    environment.pop("DEMO", None)
    environment.setdefault("LIVE", "0")
    for scenario, seed, nodes, requests in cases:
        environment.update(N=str(nodes), REQ=str(requests))
        before = set((ROOT / "logs").glob("*/summary.json")) if (ROOT / "logs").exists() else set()
        completed = subprocess.run([sys.executable, "scripts/run.py", scenario, str(seed)], cwd=ROOT, env=environment)
        expected = 2 if scenario == "nosync" else 0
        if completed.returncode != expected:
            path.write_text(json.dumps(manifest, indent=2))
            print(f"BATERIA INTERROMPIDA: {scenario}, código {completed.returncode}; esperado {expected}", file=sys.stderr)
            return 1
        created = set((ROOT / "logs").glob("*/summary.json")) - before
        if len(created) != 1:
            print("Não foi possível identificar um único resumo novo", file=sys.stderr)
            return 1
        summary_path = created.pop()
        summary = json.loads(summary_path.read_text())
        if not summary["verified"] or summary["demo"]:
            print("Resumo sem verificação acadêmica válida", file=sys.stderr)
            return 1
        manifest["runs"].append(str(summary_path.relative_to(ROOT)))
        path.write_text(json.dumps(manifest, indent=2))
    manifest["complete"] = True
    path.write_text(json.dumps(manifest, indent=2))
    subprocess.run([sys.executable, "scripts/analyze.py"], cwd=ROOT, check=True)
    print(f"BATERIA APROVADA: {len(cases)} experimentos; resultados em results/relatorio.md")
    return 0


if __name__ == "__main__":
    try: raise SystemExit(main())
    except (OSError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        print(f"BATERIA FALHOU: {error}", file=sys.stderr)
        raise SystemExit(1)
