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
import time
import unittest
from unittest.mock import patch

import run
import run_all
import compose


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

    def test_compose_reuses_services_and_prevents_concurrent_scenarios(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(run, "ROOT", Path(directory)), \
                patch.dict(os.environ, {"N": "5", "REQ": "10", "CS_MIN": "200", "CS_MAX": "1000", "DEMO": "0"}):
            experiment = compose.ComposeExperiment("barrier", 1)
            try:
                commands = experiment.commands()
                self.assertIn("node-1:7001", commands["client-1"][1])
                self.assertIn("observer:9000", commands["node-1"][1])
                self.assertIn("--barrier", commands["node-1"][1])
                command, container = experiment.process_command("node-1", *commands["node-1"])
                self.assertIsNone(container)
                self.assertIn("exec", command)
                self.assertNotIn("run", command)
                self.assertIn(experiment.run_id, command)
                self.assertEqual("zookeeper:2181", experiment.config["zk"])
            finally:
                experiment.cleanup()
            with patch.object(compose, "ROOT", Path(directory)), compose.session():
                with self.assertRaisesRegex(RuntimeError, "Já existe"):
                    with compose.session():
                        self.fail("Dois cenários não podem ocupar os mesmos contêineres")

    def test_compose_wrapper_preserves_java_exit_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "bin").mkdir()
            script = root / "scripts/compose-service.sh"
            shutil.copy(Path(__file__).with_name("compose-service.sh"), script)
            java = root / "bin/java"
            java.write_text('#!/bin/sh\necho "12:00:00.000 observer READY port=9000"\nexit "$FAKE_EXIT"\n')
            java.chmod(0o755)
            environment = dict(os.environ, TP2_PID_DIR=str(root), PATH=f"{root / 'bin'}:{os.environ['PATH']}")
            for actual in (7, 2, 0):
                result = subprocess.run(["bash", str(script), "--run", "test", "Observer"], env=dict(environment, FAKE_EXIT=str(actual)),
                                        capture_output=True, text=True)
                self.assertEqual(actual, result.returncode, result.stderr)
                self.assertIn("READY", result.stdout)
                self.assertFalse((root / "tp2-test.pid").exists())

    def test_compose_stop_terminates_the_remote_process(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "bin").mkdir()
            script = root / "scripts/compose-service.sh"
            shutil.copy(Path(__file__).with_name("compose-service.sh"), script)
            java = root / "bin/java"
            java.write_text('#!/bin/sh\necho $$ > "$FAKE_PID"\nexec sleep 60\n')
            java.chmod(0o755)
            pid_file = root / "java.pid"
            environment = dict(os.environ, TP2_PID_DIR=str(root), FAKE_PID=str(pid_file),
                               PATH=f"{root / 'bin'}:{os.environ['PATH']}")
            process = subprocess.Popen(["bash", str(script), "--run", "test", "Observer"],
                                       env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                while not pid_file.exists() and time.monotonic() < deadline:
                    time.sleep(.02)
                self.assertTrue(pid_file.exists(), "A JVM simulada não iniciou")
                pid = int(pid_file.read_text())
                subprocess.run(["bash", str(script), "--stop", "test"], env=environment, check=True)
                process.communicate(timeout=7)
                self.assertEqual(124, process.returncode)  # timeout recebeu TERM e encerrou a JVM.
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                self.assertFalse((root / "tp2-test.pid").exists())
            finally:
                if process.poll() is None:
                    subprocess.run(["bash", str(script), "--stop", "test"], env=environment, check=True)
                    process.communicate(timeout=7)


if __name__ == "__main__": unittest.main(verbosity=2)
