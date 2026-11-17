#!/usr/bin/env python3
"""Regressões de configuração e propagação de erros da bateria, sem precisar de ZK."""
import json
import io
import os
from contextlib import redirect_stderr
from pathlib import Path
import subprocess
import shutil
import tempfile
import unittest
from unittest.mock import patch

import run
import run_all


class ScriptTests(unittest.TestCase):
    def test_terminal_abbreviates_only_the_curator_protection_uuid(self):
        text = "request_id=c1-1 znode=_c_cb338f4a-aa14-4a71-875c-5c027861e09d-lock-0000000002"
        self.assertEqual("request_id=c1-1 znode=lock-0000000002", run.readable(text))

    def test_academic_scenario_requires_at_least_five_nodes(self):
        with patch.dict(run.os.environ, {"N": "2", "REQ": "10", "DEMO": "0"}):
            with self.assertRaises(ValueError): run.Experiment("normal", 1)

    def test_hot_client_cannot_exceed_fifty_requests(self):
        with patch.dict(run.os.environ, {"N": "5", "REQ": "13", "DEMO": "0"}):
            with self.assertRaises(ValueError): run.Experiment("unequal", 1)

    def test_critical_section_bounds_are_enforced(self):
        with patch.dict(run.os.environ, {"N": "5", "REQ": "10", "CS_MIN": "100", "CS_MAX": "1000"}):
            with self.assertRaises(ValueError): run.Experiment("contention", 1)

    def test_battery_does_not_hide_a_failed_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(run_all, "ROOT", root), patch.object(run_all.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)), redirect_stderr(io.StringIO()) as output:
                self.assertEqual(1, run_all.main())
            self.assertIn("BATERIA INTERROMPIDA", output.getvalue())
            manifest = json.loads((root / "results/battery.json").read_text())
            self.assertFalse(manifest["complete"])
            self.assertEqual([], manifest["runs"])

    def test_compose_keeps_each_client_connected_to_its_own_node(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(run, "ROOT", Path(directory)), \
                patch.dict(os.environ, {"N": "5", "REQ": "10", "CS_MIN": "200", "CS_MAX": "1000", "DEMO": "0"}):
            experiment = run.Experiment("normal", 1)
            try:
                local = experiment.commands()
                network = experiment.commands(network=True)
                self.assertEqual(11, len(network))
                for i in range(1, 6):
                    client = network[f"client-{i}"][1]
                    node = network[f"node-{i}"][1]
                    self.assertEqual(f"node-{i}:{7000+i}", client[client.index("--node") + 1])
                    self.assertEqual("observer:9000", node[node.index("--observer") + 1])
                    expected_node = list(local[f"node-{i}"][1])
                    expected_node[expected_node.index("--observer") + 1] = "observer:9000"
                    self.assertEqual(expected_node, node)
            finally:
                experiment.cleanup()

    def test_compose_preserves_scenario_flags_and_unequal_load(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(run, "ROOT", Path(directory)), \
                patch.dict(os.environ, {"N": "5", "REQ": "10", "CS_MIN": "200", "CS_MAX": "1000", "DEMO": "0"}):
            for scenario in ("nosync", "barrier", "unequal"):
                experiment = run.Experiment(scenario, 1)
                try:
                    commands = experiment.commands(network=True)
                    node = commands["node-1"][1]
                    self.assertEqual(scenario == "nosync", "--no-sync" in node)
                    self.assertEqual(scenario == "barrier", "--barrier" in node)
                    client = commands["client-1"][1]
                    self.assertEqual(40 if scenario == "unequal" else 10, client[client.index("--requests") + 1])
                finally:
                    experiment.cleanup()

    def test_container_logging_preserves_failures_and_expected_nosync_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "logs/test").mkdir(parents=True)
            (root / "state").mkdir()
            (root / "bin").mkdir()
            script = root / "scripts/compose-service.sh"
            shutil.copy(Path(__file__).with_name("compose-service.sh"), script)
            (root / "state/run-id").write_text("test")
            (root / "state/observer.args").write_text("Observer\n")
            java = root / "bin/java"
            java.write_text('#!/bin/sh\necho "12:00:00.000 observer READY port=9000"\nexit "$FAKE_EXIT"\n')
            java.chmod(0o755)
            environment = dict(os.environ, TP2_STATE=str(root / "state"), PATH=f"{root / 'bin'}:{os.environ['PATH']}")
            for actual, expected, success in ((7, 0, False), (2, 2, True), (0, 2, False)):
                (root / "state/observer.expected").write_text(str(expected))
                result = subprocess.run(["bash", str(script), "observer"], env=dict(environment, FAKE_EXIT=str(actual)),
                                        capture_output=True, text=True)
                self.assertEqual(success, result.returncode == 0, result.stderr)
                self.assertEqual(str(actual), (root / "state/observer.exit").read_text())
                self.assertIn("READY", (root / "logs/test/observer.log").read_text())


if __name__ == "__main__": unittest.main(verbosity=2)
