#!/usr/bin/env python3
"""Focused tests for the open-loop sweep's fail-closed topology binding."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CELL_RUNNER = ROOT / "hack" / "arena-sc-inference-cell.sh"
SWEEP_RUNNER = ROOT / "hack" / "arena-sc-inference-open-loop-sweep.sh"
SUMMARY_RUNNER = ROOT / "hack" / "arena-sc-open-loop-summarize.py"

SPEC = importlib.util.spec_from_file_location("arena_sc_open_loop_summarize", SUMMARY_RUNNER)
assert SPEC and SPEC.loader
SUMMARY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SUMMARY
SPEC.loader.exec_module(SUMMARY)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class OpenLoopTopologyIntegrationTests(unittest.TestCase):
    def make_cell(self, root: Path, report_uid: str = "uid-1") -> tuple[dict, dict]:
        targets = {
            "items": [
                {
                    "metadata": {"name": "target-1", "uid": "uid-1"},
                    "spec": {"nodeName": "node-a"},
                }
            ]
        }
        report_summary = {
            "nodes": 1,
            "pods": 1,
            "pods_validated": 1,
            "placement_violations": 0,
            "warnings": 0,
            "invalid_reasons": 0,
            "gate_ineligibility_reasons": 0,
        }
        captured_pod = {
            "name": "target-1",
            "uid": report_uid,
            "node": "node-a",
            "cpuset": "2,6",
            "ready": True,
        }
        report = {
            "schema_version": 1,
            "verdict": "PASS",
            "placement_verdict": "PASS",
            "gate_passed": True,
            "exit_code": 0,
            "summary": report_summary,
            "pods": [captured_pod],
            "snapshot": {
                "capture": {
                    "mode": "live-read-only",
                    "namespace": "bench",
                    "selector": "app=target",
                    "expected_pods": 1,
                },
                "pods": [captured_pod],
            },
        }
        (root / "targets-before.json").write_text(json.dumps(targets) + "\n", encoding="utf-8")
        report_path = root / "topology-preflight-report.json"
        stdout_path = root / "topology-preflight-stdout.txt"
        stderr_path = root / "topology-preflight-stderr.txt"
        report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
        stdout_path.write_text(json.dumps(report) + "\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        execution = {
            "schema_version": 1,
            "gate": "cpu_topology_pre_load",
            "enabled": True,
            "runner": "/runner/topology.py",
            "runner_exit_code": 0,
            "report_json_valid": True,
            "report_gate_valid": True,
            "target_identity_match": True,
            "load_authorized": True,
            "disposition": "pass",
            "evidence_sha256": {
                "report": sha256(report_path),
                "raw_stdout": sha256(stdout_path),
                "stderr": sha256(stderr_path),
            },
        }
        execution_path = root / "topology-preflight-execution.json"
        execution_path.write_text(json.dumps(execution, sort_keys=True) + "\n", encoding="utf-8")
        recorded = execution | {
            "required_by_caller": True,
            "execution_sha256": sha256(execution_path),
            "report_verdict": "PASS",
            "placement_verdict": "PASS",
            "report_summary": report_summary,
        }
        provenance = {
            "replicas": 1,
            "namespace": "bench",
            "topology_preflight": {
                "required": True,
                "runner": "/runner/topology.py",
                "selector": "app=target",
            },
        }
        return {"topology_preflight": recorded}, provenance

    def test_gate_order_and_default_are_explicit(self) -> None:
        cell = CELL_RUNNER.read_text(encoding="utf-8")
        sweep = SWEEP_RUNNER.read_text(encoding="utf-8")
        rollout = cell.index('rollout status deployment/"$DEPLOYMENT"')
        gate = cell.index('"$TOPOLOGY_PREFLIGHT_RUNNER" "${topology_args[@]}"')
        driver = cell.index('"${k[@]}" create job "$job"')
        self.assertLess(rollout, gate)
        self.assertLess(gate, driver)
        self.assertIn("TOPOLOGY_PREFLIGHT_ENABLED=${TOPOLOGY_PREFLIGHT_ENABLED:-1}", sweep)
        self.assertIn('TOPOLOGY_PREFLIGHT_ENABLED="$TOPOLOGY_PREFLIGHT_ENABLED"', sweep)
        self.assertIn("topology_preflight_cell_json=$(jq -cn", cell)
        self.assertIn("|| jq -cn --slurpfile execution", cell)
        self.assertIn("status=invalid", sweep)
        self.assertIn("CPU-topology preflight invalidated the cell before driver load", sweep)

    def test_summary_accepts_identity_bound_hashed_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cell, provenance = self.make_cell(root)
            result = SUMMARY.validate_topology_preflight(root, cell, provenance)
            self.assertTrue(result["attested"])
            self.assertTrue(result["load_authorized"])

    def test_summary_rejects_report_for_different_pod_uid(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cell, provenance = self.make_cell(root, report_uid="wrong-uid")
            with self.assertRaisesRegex(SUMMARY.ValidationError, "identities differ"):
                SUMMARY.validate_topology_preflight(root, cell, provenance)

    def test_summary_rejects_tampered_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cell, provenance = self.make_cell(root)
            with (root / "topology-preflight-report.json").open("a", encoding="utf-8") as handle:
                handle.write(" \n")
            with self.assertRaisesRegex(SUMMARY.ValidationError, "report hash mismatch"):
                SUMMARY.validate_topology_preflight(root, cell, provenance)

    def test_nonzero_preflight_exits_before_any_driver_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            log_path = root / "oc.log"
            target_digest = "sha256:" + "b" * 64
            pod = {
                "metadata": {"name": "target-1", "uid": "uid-1"},
                "spec": {"nodeName": "node-a"},
                "status": {
                    "phase": "Running",
                    "podIP": "10.0.0.1",
                    "qosClass": "Guaranteed",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "restartCount": 0,
                            "imageID": "registry.invalid/target@" + target_digest,
                        }
                    ],
                },
            }
            pods_path = root / "pods.json"
            deployment_path = root / "deployment.json"
            nodes_path = root / "nodes.json"
            events_path = root / "events.json"
            pods_path.write_text(json.dumps({"items": [pod]}), encoding="utf-8")
            deployment_path.write_text(
                json.dumps(
                    {
                        "spec": {
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "env": [],
                                            "resources": {"requests": {}, "limits": {}},
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            nodes_path.write_text(json.dumps({"items": []}), encoding="utf-8")
            events_path.write_text(json.dumps({"items": []}), encoding="utf-8")

            fake_oc = bin_dir / "oc"
            fake_oc.write_text(
                "#!/bin/sh\n"
                'printf "%s\\n" "$*" >>"$FAKE_OC_LOG"\n'
                'case " $* " in\n'
                '  *" wait --for=condition=Ready node/"*) exit 0 ;;\n'
                '  *" scale deployment "*) exit 0 ;;\n'
                '  *" rollout status deployment/"*) exit 0 ;;\n'
                '  *" get pods "*" -o json "*) command cat "$FAKE_PODS_JSON" ;;\n'
                '  *" get deployment "*" -o json "*) command cat "$FAKE_DEPLOYMENT_JSON" ;;\n'
                '  *" get nodes -o json "*) command cat "$FAKE_NODES_JSON" ;;\n'
                '  *" get events "*" -o json "*) command cat "$FAKE_EVENTS_JSON" ;;\n'
                '  *) exit 97 ;;\n'
                "esac\n",
                encoding="utf-8",
            )
            fake_oc.chmod(0o755)

            failed_report = {
                "schema_version": 1,
                "verdict": "FAIL",
                "placement_verdict": "FAIL",
                "gate_passed": False,
                "exit_code": 1,
                "summary": {
                    "nodes": 1,
                    "pods": 1,
                    "pods_validated": 1,
                    "placement_violations": 1,
                    "warnings": 0,
                    "invalid_reasons": 0,
                    "gate_ineligibility_reasons": 0,
                },
                "pods": [{"name": "target-1", "uid": "uid-1", "node": "node-a"}],
                "snapshot": {
                    "capture": {
                        "mode": "live-read-only",
                        "namespace": "bench",
                        "selector": "app=target",
                        "expected_pods": 1,
                    },
                    "pods": [{"name": "target-1", "uid": "uid-1", "node": "node-a"}],
                },
            }
            failed_report_path = root / "failed-report.json"
            failed_report_path.write_text(json.dumps(failed_report) + "\n", encoding="utf-8")
            fake_gate = root / "topology-gate"
            fake_gate.write_text(
                "#!/bin/sh\n"
                f"command cat {shlex.quote(str(failed_report_path))}\n"
                "exit 1\n",
                encoding="utf-8",
            )
            fake_gate.chmod(0o755)

            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{bin_dir}:{environment['PATH']}",
                    "FAKE_OC_LOG": str(log_path),
                    "FAKE_PODS_JSON": str(pods_path),
                    "FAKE_DEPLOYMENT_JSON": str(deployment_path),
                    "FAKE_NODES_JSON": str(nodes_path),
                    "FAKE_EVENTS_JSON": str(events_path),
                    "KUBECONFIG_PATH": str(root / "kubeconfig"),
                    "NAMESPACE": "bench",
                    "DEPLOYMENT": "target",
                    "TARGET_SELECTOR": "app=target",
                    "TARGET_NODE": "node-a",
                    "DRIVER_NODE": "node-b",
                    "REPLICAS": "1",
                    "CONCURRENCY": "1",
                    "CONNECTIONS": "1",
                    "DURATION_SECONDS": "1",
                    "START_DELAY_SECONDS": "2",
                    "MAX_ROWS_PER_ENDPOINT": "2",
                    "SEQUENCE_BASE": "1",
                    "RUN_ID": "fail-closed-test",
                    "DRIVER_IMAGE": "registry.invalid/driver@sha256:" + "a" * 64,
                    "TARGET_IMAGE": target_digest,
                    "MODEL_SHA256": "c" * 64,
                    "RESULT_ROOT": str(root / "results"),
                    "RESET_TARGETS": "false",
                    "OFFERED_RPS": "1",
                    "MAX_IN_FLIGHT": "1",
                    "TOPOLOGY_PREFLIGHT_ENABLED": "1",
                    "TOPOLOGY_PREFLIGHT_RUNNER": str(fake_gate),
                }
            )
            result = subprocess.run(
                [str(CELL_RUNNER)],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(result.returncode, 6, result.stderr)
            execution = json.loads(
                (root / "results/fail-closed-test/topology-preflight-execution.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertFalse(execution["load_authorized"])
            self.assertEqual(execution["disposition"], "invalid_pre_load")
            self.assertNotIn(" create job ", f" {log_path.read_text(encoding='utf-8')} ")


if __name__ == "__main__":
    unittest.main()
