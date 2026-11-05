"""Dashboard scope/units, actual PromQL results, and repeatable DATA probe contract."""

import argparse
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = ROOT / "monitoring/grafana/dashboards"
SELECTOR = re.compile(r'\b((?:relaygate_|container_|kube_)\w*|up)(\{[^}]*\})?')


def dashboards():
    return {d["uid"]: d for path in sorted(DASHBOARDS.glob("*.json"))
            for d in [json.loads(path.read_text())]}


def flatten(items):
    for panel in items:
        yield panel
        yield from flatten(panel.get("panels", []))


def panels(dashboard="relaygate-overview"):
    return {p["id"]: p for p in flatten(dashboards()[dashboard]["panels"])}


def metric_panels():
    return [(uid, panel) for uid in dashboards() for panel in panels(uid).values()
            if "targets" in panel]


def expression(panel_id, index=0, dashboard="relaygate-overview"):
    value = panels(dashboard)[panel_id]["targets"][index]["expr"]
    for key, replacement in {"cluster": "c1", "namespace": "ns1", "gateway": ".*",
                             "route_table": ".*", "sdk": ".*", "pod": ".*",
                             "statefulset": ".*", "__rate_interval": "5m"}.items():
        value = value.replace(f"${key}", replacement)
    return value


def bounded_integer(value, minimum, maximum, field):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be an integer in {minimum}..{maximum}")
    return value


def finite_number(value, field, *, positive=True):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{field} must be finite")
    if (positive and value <= 0) or (not positive and value < 0):
        raise ValueError(f"{field} must be {'positive' if positive else 'nonnegative'}")
    return value


def verify_probe(path, expected_concurrency=1, expected_idle_pipes=0):
    bounded_integer(expected_concurrency, 1, 64, "expected concurrency")
    bounded_integer(expected_idle_pipes, 0, 1024, "expected idle Pipes")
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
    concurrency = bounded_integer(record.get("concurrency"), 1, 64, "concurrency")
    # Historical serial artifacts predate the explicit per-Pipe/idle metadata.
    if "idle_pipes" not in record and (concurrency != 1 or expected_idle_pipes != 0):
        raise ValueError("concurrent or idle profiles require explicit idle_pipes")
    idle_pipes = bounded_integer(record.get("idle_pipes", 0), 0, 1024, "idle_pipes")
    if (concurrency, idle_pipes) != (expected_concurrency, expected_idle_pipes):
        raise ValueError("probe concurrency/idle Pipes differ from the expected profile")
    legacy_serial = concurrency == 1 and idle_pipes == 0
    cases = record.get("cases")
    if not isinstance(cases, list) or len(cases) != 9:
        raise ValueError("expected the 3x3 path matrix")
    paths = set()
    profiles = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("invalid path record")
        entry = bounded_integer(case.get("entry_index"), 0, 2, "entry_index")
        owner = bounded_integer(case.get("owner_index"), 0, 2, "owner_index")
        paths.add((entry, owner))
        expected_path = "local" if entry == owner else "one_hop"
        if case.get("path") != expected_path:
            raise ValueError("wrong topology")
        bounded_integer(case.get("errors"), 0, 0, "errors")
        requested = bounded_integer(case.get("requested_samples"), 1, 100_000, "requested_samples")
        completed = bounded_integer(case.get("completed_samples"), 1, 100_000, "completed_samples")
        samples = bounded_integer(case.get("samples_per_pipe", requested if legacy_serial else None),
                                  1, 100_000, "samples_per_pipe")
        case_idle = bounded_integer(case.get("idle_pipes", 0 if legacy_serial else None),
                                    0, 1024, "case idle_pipes")
        per_pipe = case.get("completed_samples_per_pipe", [completed] if legacy_serial else None)
        if not isinstance(per_pipe, list) or len(per_pipe) != concurrency:
            raise ValueError("wrong completed_samples_per_pipe length")
        for count in per_pipe:
            bounded_integer(count, 0, samples, "per-Pipe completed samples")
        if (requested != samples * concurrency or completed != requested
                or sum(per_pipe) != completed or any(count != samples for count in per_pipe)):
            raise ValueError("incomplete or inconsistent per-Pipe measurement")
        if case_idle != idle_pipes:
            raise ValueError("case idle Pipes differ from the root profile")
        payload_bytes = bounded_integer(case.get("payload_bytes"), 1, 65_536, "payload_bytes")
        warmup = bounded_integer(case.get("warmup"), 0, 10_000, "warmup")
        profiles.add((samples, payload_bytes, warmup))
        rtt = case.get("rtt_seconds")
        if not isinstance(rtt, dict):
            raise ValueError("missing RTT distribution")
        quantiles = [finite_number(rtt.get(key), f"RTT {key}") for key in ("p50", "p95", "p99", "max")]
        if quantiles != sorted(quantiles):
            raise ValueError("invalid RTT distribution")
        expected_bytes = completed * payload_bytes * 2
        bounded_integer(case.get("echo_payload_bytes"), expected_bytes, expected_bytes, "echo_payload_bytes")
        elapsed = finite_number(case.get("measurement_seconds"), "measurement_seconds")
        goodput = finite_number(case.get("echo_goodput_bytes_per_second"), "echo_goodput_bytes_per_second")
        expected_rate = expected_bytes / elapsed
        if not math.isfinite(expected_rate) or not math.isclose(goodput, expected_rate, rel_tol=1e-9):
            raise ValueError("goodput denominator mismatch")
        finite_number(case.get("session_connect_seconds"), "session_connect_seconds", positive=False)
        finite_number(case.get("dial_seconds"), "dial_seconds", positive=False)
    if paths != {(entry, owner) for entry in range(3) for owner in range(3)}:
        raise ValueError("missing or duplicated path")
    if len(profiles) != 1:
        raise ValueError("path workloads differ")


class ProbeTests(unittest.TestCase):
    @staticmethod
    def fixture(concurrency=1, idle_pipes=0, samples=25):
        requested = concurrency * samples
        total_bytes = requested * 64 * 2
        return {"probe": "established_pipe_rtt", "concurrency": concurrency,
                "idle_pipes": idle_pipes, "cases": [
                    {"entry_index": entry, "owner_index": owner,
                     "path": "local" if entry == owner else "one_hop",
                     "payload_bytes": 64, "warmup": 5, "samples_per_pipe": samples,
                     "idle_pipes": idle_pipes, "requested_samples": requested,
                     "completed_samples": requested, "completed_samples_per_pipe": [samples] * concurrency,
                     "errors": 0, "rtt_seconds": {"p50": .001, "p95": .002, "p99": .003, "max": .004},
                     "echo_payload_bytes": total_bytes, "measurement_seconds": .25,
                     "echo_goodput_bytes_per_second": total_bytes / .25,
                     "session_connect_seconds": .01, "dial_seconds": .02}
                    for entry in range(3) for owner in range(3)]}

    def verify(self, record, concurrency=1, idle_pipes=0):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latency.log"
            path.write_text("probe setup log\n" + json.dumps(record) + "\n")
            verify_probe(path, concurrency, idle_pipes)

    def test_accepts_serial_concurrent_and_limit_profiles(self):
        for concurrency, idle, samples in [(1, 0, 1000), (4, 4, 25), (64, 1024, 1), (10, 0, 10_000)]:
            with self.subTest(concurrency=concurrency, idle=idle, samples=samples):
                self.verify(self.fixture(concurrency, idle, samples), concurrency, idle)

    def test_accepts_historical_serial_artifact(self):
        record = self.fixture()
        del record["idle_pipes"]
        for case in record["cases"]:
            for field in ("samples_per_pipe", "idle_pipes", "completed_samples_per_pipe"):
                del case[field]
        self.verify(record)

    def test_rejects_unexpected_profile(self):
        for concurrency, idle in [(4, 0), (1, 4)]:
            with self.subTest(concurrency=concurrency, idle=idle), self.assertRaises(ValueError):
                self.verify(self.fixture(concurrency, idle))

    def test_concurrent_and_idle_profiles_require_metadata(self):
        for concurrency, idle in [(4, 0), (1, 4)]:
            for field in ("root idle_pipes", "samples_per_pipe", "idle_pipes", "completed_samples_per_pipe"):
                record = self.fixture(concurrency, idle)
                if field == "root idle_pipes":
                    del record["idle_pipes"]
                else:
                    del record["cases"][0][field]
                with self.subTest(concurrency=concurrency, idle=idle, field=field), self.assertRaises(ValueError):
                    self.verify(record, concurrency, idle)

    def test_rejects_missing_duplicate_and_wrong_paths(self):
        for change in ("missing", "duplicate", "wrong"):
            record = self.fixture()
            if change == "missing":
                record["cases"].pop()
            elif change == "duplicate":
                record["cases"][-1] = record["cases"][0].copy()
            else:
                record["cases"][0]["path"] = "one_hop"
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.verify(record)

    def test_rejects_incomplete_failed_or_miscounted_samples(self):
        changes = [("errors", 1), ("completed_samples", 99), ("requested_samples", 99),
                   ("samples_per_pipe", 24), ("completed_samples_per_pipe", [25] * 3),
                   ("completed_samples_per_pipe", [24, 26, 25, 25]),
                   ("completed_samples_per_pipe", [24, 25, 25, 25]), ("idle_pipes", 3)]
        for field, value in changes:
            record = self.fixture(4, 4)
            record["cases"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.verify(record, 4, 4)

    def test_rejects_out_of_bounds_or_noninteger_workloads(self):
        for field, value in [("warmup", -1), ("warmup", 10001), ("payload_bytes", 0),
                             ("payload_bytes", 65537), ("requested_samples", 100001),
                             ("samples_per_pipe", True), ("completed_samples", 25.0)]:
            record = self.fixture()
            record["cases"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.verify(record)
        with self.assertRaises(ValueError):
            self.verify(self.fixture(2, samples=50_001), 2)
        for concurrency, idle in [(0, 0), (65, 0), (1, -1), (1, 1025)]:
            with self.subTest(concurrency=concurrency, idle=idle), self.assertRaises(ValueError):
                self.verify(self.fixture(concurrency, idle), concurrency, idle)

    def test_rejects_nonfinite_or_nonpositive_measurements(self):
        for field in ("measurement_seconds", "echo_goodput_bytes_per_second"):
            for value in (float("nan"), float("inf"), 0, -1, True):
                record = self.fixture()
                record["cases"][0][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.verify(record)
        for field in ("p50", "p95", "p99", "max"):
            for value in (float("nan"), float("inf"), 0, -1):
                record = self.fixture()
                record["cases"][0]["rtt_seconds"][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.verify(record)

    def test_rejects_wrong_quantiles_bytes_denominator_or_workload(self):
        for change in ("quantiles", "bytes", "denominator", "workload"):
            record = self.fixture()
            case = record["cases"][0]
            if change == "quantiles":
                case["rtt_seconds"]["p50"] = .005
            elif change == "bytes":
                case["echo_payload_bytes"] += 1
            elif change == "denominator":
                case["measurement_seconds"] *= 4
            else:
                case["warmup"] += 1
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.verify(record)

    def test_probe_cli_preserves_defaults_and_accepts_expected_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latency.log"
            for concurrency, idle in [(1, 0), (4, 4)]:
                path.write_text(json.dumps(self.fixture(concurrency, idle)))
                args = [sys.executable, __file__, "--probe", str(path)]
                if concurrency != 1:
                    args.extend(["--probe-concurrency", str(concurrency), "--probe-idle-pipes", str(idle)])
                result = subprocess.run(args, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_missing_or_multiple_completed_records(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latency.log"
            for content in ("setup only\n", (json.dumps(self.fixture()) + "\n") * 2):
                path.write_text(content)
                with self.subTest(content=content[:20]), self.assertRaises(ValueError):
                    verify_probe(path)


class ObservabilityTests(unittest.TestCase):
    def test_overview_is_small_and_detail_links_preserve_context(self):
        views = dashboards()
        self.assertEqual(set(views), {"relaygate-overview", "relaygate-runtime", "relaygate-sdk"})
        overview = views["relaygate-overview"]
        self.assertEqual(len(overview["panels"]), 8)
        self.assertEqual([p["type"] for p in overview["panels"]], ["stat"] * 4 + ["timeseries"] * 4)
        self.assertEqual([v["name"] for v in overview["templating"]["list"]], ["cluster", "namespace", "gateway"])
        for uid, view in views.items():
            items = list(flatten(view["panels"]))
            self.assertEqual(len({p["id"] for p in items}), len(items))
            self.assertEqual({link["url"] for link in view["links"]}, {f"/d/{other}" for other in views if other != uid})
            for link in view["links"]:
                self.assertTrue(link["includeVars"])
                self.assertTrue(link["keepTime"])
            for panel in items:
                if panel["type"] == "stat":
                    self.assertTrue(all(t["instant"] and not t["range"] for t in panel["targets"]))
        rows = [p for p in views["relaygate-runtime"]["panels"] if p["type"] == "row"]
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["collapsed"] and row["panels"] for row in rows))

    def test_every_runtime_selector_is_scoped(self):
        for uid, panel in metric_panels():
            variables = {v["name"] for v in dashboards()[uid]["templating"]["list"]}
            for target in panel["targets"]:
                used = set(re.findall(r'\$(\w+)', target["expr"])) - {"__rate_interval"}
                self.assertTrue(used <= variables, (uid, panel["title"], used - variables))
                matches = list(SELECTOR.finditer(target["expr"]))
                self.assertTrue(matches, panel["title"])
                for match in matches:
                    selector = match.group(2) or ""
                    self.assertIn('cluster=~"$cluster"', selector)
                    self.assertIn('namespace=~"$namespace"', selector)

    def test_current_counts_do_not_mix_with_rates(self):
        for _, panel in metric_panels():
            if panel["fieldConfig"]["defaults"]["unit"] == "short":
                for target in panel["targets"]:
                    self.assertNotRegex(target["expr"], r'\b(rate|irate)\(')
        self.assertIn("originated_pipes", expression(29))
        self.assertIn("resource_limit", expression(22))
        self.assertIn("reconnect_in_progress", expression(24, dashboard="relaygate-sdk"))

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
        sample("relaygate_gateway_resource_used", "9+0x5", instance="gw-b", resource="sessions")
        sample("relaygate_gateway_resource_limit", "10+0x5", instance="gw-b", resource="sessions")
        sample("relaygate_gateway_dial_results_total", "0+540x5", instance="gw-a", outcome="success", code="ok", **{"class": "success"})
        sample("relaygate_gateway_dial_results_total", "0+60x5", instance="gw-a", outcome="error", code="not_found", **{"class": "request"})
        sample("relaygate_sdk_reconnect_in_progress", "2+0x5", instance="sdk-a")
        checks = [
            (expression(29), [("{}", 2)]),
            (expression(8, 3, "relaygate-runtime"), [("{}", 3)]),
            (expression(22), [('{resource="sessions"}', 0.9)]),
            (expression(22, dashboard="relaygate-runtime"), [
                ('{cluster="c1",namespace="ns1",instance="gw-a",resource="sessions"}', 0.3),
                ('{cluster="c1",namespace="ns1",instance="gw-b",resource="sessions"}', 0.9)]),
            (expression(6), [('{class="success"}', 0.9), ('{class="request"}', 0.1)]),
            (expression(24, dashboard="relaygate-sdk"), [('{__name__="relaygate_sdk_reconnect_in_progress",cluster="c1",namespace="ns1",instance="sdk-a"}', 2)]),
        ]
        def check(expr, expected):
            return {"expr": expr, "eval_time": "5m", "exp_samples": [{"labels": labels, "value": value} for labels, value in expected]}
        fixture = {"evaluation_interval": "1m", "tests": [
            {"interval": "1m", "input_series": series, "promql_expr_test": [check(expr, expected) for expr, expected in checks]},
            # Parse every real panel query and keep missing telemetry distinct from healthy zero.
            {"interval": "1m", "input_series": [], "promql_expr_test": [check(expression(panel["id"], i, uid), []) for uid, panel in metric_panels() for i in range(len(panel["targets"]))]},
        ]}
        with tempfile.TemporaryDirectory(prefix=".promql-test-", dir=ROOT) as directory:
            os.chmod(directory, 0o755)  # The pinned Prometheus image runs as nobody.
            path = Path(directory) / "dashboard.test.yml"
            path.write_text(json.dumps(fixture))  # JSON is valid YAML; no YAML dependency.
            subprocess.run([*shlex.split(os.environ["PROMTOOL"]), "test", "rules", str(path)], cwd=ROOT, check=True)


if __name__ == "__main__":
    if any(argument.startswith("--probe") for argument in sys.argv[1:]):
        parser = argparse.ArgumentParser(description="Validate the 3x3 DATA RTT probe output")
        parser.add_argument("--probe", required=True, metavar="FILE")
        parser.add_argument("--probe-concurrency", type=int, default=1)
        parser.add_argument("--probe-idle-pipes", type=int, default=0)
        arguments = parser.parse_args()
        verify_probe(arguments.probe, arguments.probe_concurrency, arguments.probe_idle_pipes)
    else:
        unittest.main()
