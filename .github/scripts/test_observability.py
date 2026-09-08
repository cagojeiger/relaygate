"""Dashboard scope/units, actual PromQL results, and repeatable DATA probe contract."""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = ROOT / "monitoring/grafana/dashboards/relaygate-overview.json"
SELECTOR = re.compile(r'\b((?:relaygate_|container_|kube_)\w*|up)(\{[^}]*\})?')


def panels():
    return {p["id"]: p for p in json.loads(DASHBOARD.read_text())["panels"]}


def expression(panel_id, index=0):
    value = panels()[panel_id]["targets"][index]["expr"]
    for key, replacement in {"cluster": "c1", "namespace": "ns1", "gateway": ".*",
                             "route_table": ".*", "sdk": ".*", "pod": ".*",
                             "statefulset": ".*", "__rate_interval": "5m"}.items():
        value = value.replace(f"${key}", replacement)
    return value


def verify_probe(path):
    records = []
    for line in Path(path).read_text().splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("probe") == "established_pipe_rtt":
            records.append(value)
    if len(records) != 1:
        raise ValueError("expected exactly one completed latency probe record")
    record = records[0]
    cases = record["cases"]
    if record["concurrency"] != 1 or len(cases) != 9:
        raise ValueError("expected the 3x3 serial path matrix")
    paths = {(c["entry_index"], c["owner_index"]) for c in cases}
    if paths != {(entry, owner) for entry in range(3) for owner in range(3)}:
        raise ValueError("missing or duplicated path")
    for case in cases:
        expected_path = "local" if case["entry_index"] == case["owner_index"] else "one_hop"
        if case["path"] != expected_path or case["errors"] != 0:
            raise ValueError("wrong topology or failed DATA exchange")
        if case["completed_samples"] != case["requested_samples"] or case["completed_samples"] <= 0:
            raise ValueError("incomplete measurement")
        rtt = case["rtt_seconds"]
        if not 0 < rtt["p50"] <= rtt["p95"] <= rtt["p99"] <= rtt["max"]:
            raise ValueError("invalid RTT distribution")
        expected_bytes = case["completed_samples"] * case["payload_bytes"] * 2
        if case["echo_payload_bytes"] != expected_bytes or case["measurement_seconds"] <= 0:
            raise ValueError("invalid payload accounting")
        expected_rate = expected_bytes / case["measurement_seconds"]
        if abs(case["echo_goodput_bytes_per_second"] / expected_rate - 1) > 1e-9:
            raise ValueError("goodput denominator mismatch")


class ObservabilityTests(unittest.TestCase):
    def test_every_runtime_selector_is_scoped(self):
        for panel in panels().values():
            for target in panel["targets"]:
                matches = list(SELECTOR.finditer(target["expr"]))
                self.assertTrue(matches, panel["title"])
                for match in matches:
                    selector = match.group(2) or ""
                    self.assertIn('cluster=~"$cluster"', selector)
                    self.assertIn('namespace=~"$namespace"', selector)

    def test_current_counts_do_not_mix_with_rates(self):
        for panel in panels().values():
            if panel["fieldConfig"]["defaults"]["unit"] == "short":
                for target in panel["targets"]:
                    self.assertNotRegex(target["expr"], r'\b(rate|irate)\(')
        self.assertIn("originated_pipes", expression(8, 2))
        self.assertIn("resource_limit", expression(22))
        self.assertIn("reconnect_in_progress", expression(24))

    @unittest.skipUnless(os.environ.get("PROMTOOL"), "PromQL execution runs in the pinned CI container")
    def test_promql_scope_counts_capacity_and_result_ratios(self):
        series = []

        def sample(metric, values, **labels):
            for cluster, namespace, data in [("c1", "ns1", values), ("c2", "ns1", "999+0x5"), ("c1", "ns2", "999+0x5")]:
                fields = dict(cluster=cluster, namespace=namespace, **labels)
                label_string = ",".join(f'{k}="{v}"' for k, v in fields.items())
                series.append({"series": f"{metric}{{{label_string}}}", "values": data})

        for instance, unique, local_state in [("gw-a", 1, 1), ("gw-b", 1, 1), ("gw-c", 0, 1)]:
            sample("relaygate_gateway_originated_pipes", f"{unique}+0x5", instance=instance)
            sample("relaygate_gateway_live_pipes", f"{local_state}+0x5", instance=instance)
        sample("relaygate_gateway_resource_used", "3+0x5", instance="gw-a", resource="sessions")
        sample("relaygate_gateway_resource_limit", "10+0x5", instance="gw-a", resource="sessions")
        sample("relaygate_gateway_dial_results_total", "0+540x5", instance="gw-a", outcome="success", code="ok", **{"class": "success"})
        sample("relaygate_gateway_dial_results_total", "0+60x5", instance="gw-a", outcome="error", code="not_found", **{"class": "request"})
        sample("relaygate_sdk_reconnect_in_progress", "2+0x5", instance="sdk-a")
        checks = [
            (expression(8, 2), [("{}", 2)]),
            (expression(8, 3), [("{}", 3)]),
            (expression(22), [('{cluster="c1",namespace="ns1",instance="gw-a",resource="sessions"}', 0.3)]),
            (expression(6), [('{class="success"}', 0.9), ('{class="request"}', 0.1)]),
            (expression(24), [('{__name__="relaygate_sdk_reconnect_in_progress",cluster="c1",namespace="ns1",instance="sdk-a"}', 2)]),
        ]
        def check(expr, expected):
            return {"expr": expr, "eval_time": "5m", "exp_samples": [{"labels": labels, "value": value} for labels, value in expected]}
        fixture = {"evaluation_interval": "1m", "tests": [
            {"interval": "1m", "input_series": series, "promql_expr_test": [check(expr, expected) for expr, expected in checks]},
            # Parse every real panel query and keep missing telemetry distinct from healthy zero.
            {"interval": "1m", "input_series": [], "promql_expr_test": [check(expression(panel_id, i), []) for panel_id, panel in panels().items() for i in range(len(panel["targets"]))]},
        ]}
        with tempfile.TemporaryDirectory(prefix=".promql-test-", dir=ROOT) as directory:
            os.chmod(directory, 0o755)  # The pinned Prometheus image runs as nobody.
            path = Path(directory) / "dashboard.test.yml"
            path.write_text(json.dumps(fixture))  # JSON is valid YAML; no YAML dependency.
            subprocess.run([*shlex.split(os.environ["PROMTOOL"]), "test", "rules", str(path)], cwd=ROOT, check=True)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        verify_probe(sys.argv[2])
    else:
        unittest.main()
