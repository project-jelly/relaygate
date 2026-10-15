"""One temporary hosted-runner experiment; never used by the default CI branch."""

from pathlib import Path
import datetime
import hashlib
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import tarfile
import threading
import time


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "target/peer-tail-hosted"
PLAN = ROOT / ".github/experiments/peer-tail-hosted/plan.json"
SERVICES = ["rt-0", "rt-1", "gateway-a", "gateway-b", "gateway-c", "listener-a", "listener-b", "listener-c"]
COMPILERS = {"rustc", "cargo", "cc", "gcc", "g++", "clang", "clang++", "ld", "lld", "ninja", "cmake"}


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def capture(args, cwd=ROOT):
    return subprocess.check_output(args, cwd=cwd, text=True, timeout=30)


def parse_stat(text):
    opening, closing = text.index("("), text.rindex(")")
    fields = text[closing + 2:].split()
    return {"pid": int(text[:opening]), "comm": text[opening + 1:closing],
            "state": fields[0], "ppid": int(fields[1]),
            "kernel": bool(int(fields[6]) & 0x00200000),
            "ticks": int(fields[11]) + int(fields[12]), "start": int(fields[19])}


def processes():
    found = {}
    for directory in Path("/proc").iterdir():
        if directory.name.isdigit():
            try:
                proc = parse_stat((directory / "stat").read_text())
                proc["cgroup"] = (directory / "cgroup").read_text()
                found[proc["pid"]] = proc
            except (FileNotFoundError, ProcessLookupError):
                continue
    return found


def ownership(pid, procs, containers, harness_pid):
    """Cgroup identity first, then visible init-PID ancestry; unknown users are external."""
    original = procs[pid]
    for container in containers:
        if container["id"] in original.get("cgroup", ""):
            return "own_container", container["id"]
    roots = {container["pid"]: container["id"] for container in containers}
    visited = set()
    while pid in procs and pid not in visited:
        if pid in roots:
            return "own_container", roots[pid]
        if pid == harness_pid:
            return "harness", None
        visited.add(pid)
        pid = procs[pid]["ppid"]
    if original["kernel"]:
        return "kernel", None
    if original["comm"] in {"dockerd", "containerd", "containerd-shim"} or original["comm"].startswith("containerd-shim"):
        return "docker_infrastructure", None
    return "external_user", None


def cpu_delta(current, previous, previous_boot_ticks):
    old = previous.get(current["pid"])
    if old and old["start"] == current["start"]:
        return max(0, current["ticks"] - old["ticks"])
    return current["ticks"] if current["start"] >= previous_boot_ticks else 0


def system_cpu():
    return [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]


def unattributed_task_cpu(deltas, observed_cores, hz, interval):
    # user + nice + system are task CPU. IRQ/softirq and steal are recorded separately.
    # Exited processes can disappear from /proc, but their CPU remains in these counters.
    task_cores = sum(deltas[:3]) / hz / interval
    return max(0.0, task_cores - observed_cores)


def verify_membership(procs, containers, owners):
    by_service = {container["service"]: container for container in containers}
    for service in SERVICES:
        if service not in by_service:
            raise RuntimeError("runtime container absent during measurement: " + service)
        container = by_service[service]
        if owners.get(container["pid"]) != ("own_container", container["id"]):
            raise RuntimeError("container init PID classification incomplete: " + service)
        expected = ("relaygate-echo-listener" if service.startswith("listener-") else "relaygate-server")[:15]
        if not any(proc["comm"] == expected and owners[pid] == ("own_container", container["id"])
                   for pid, proc in procs.items()):
            raise RuntimeError("runtime child PID classification incomplete: " + service)


def regression_signal(blocks, criterion):
    threshold = criterion["p99_increase_percent"]
    return (sum(value > threshold for value in blocks) >= criterion["minimum_blocks_over_threshold"]
            and statistics.median(blocks) > threshold)


def summarize(plan):
    result = {"scope": "Independent linux/amd64 hosted run; do not pool with local arm64 attempts",
              "complete_pairs": {}, "profiles": {}, "validated_runs": 0,
              "requested_samples_completed": 0, "decision": "HOLD: incomplete experiment"}
    for profile in plan["profiles"]:
        runs = {}
        for pair in range(plan["pairs"]):
            for variant in ["baseline", "candidate"]:
                name = f"pair-{pair}-{variant}-{profile['name']}"
                if not (OUT / (name + ".validated")).exists():
                    continue
                records = []
                for line in (OUT / (name + ".log")).read_text().splitlines():
                    try:
                        value = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(value, dict) and value.get("probe") == "established_pipe_rtt":
                        records.append(value)
                assert len(records) == 1
                cases = {(case["entry_index"], case["owner_index"]): case for case in records[0]["cases"]}
                assert len(cases) == 9
                for case in cases.values():
                    assert case["errors"] == 0
                    assert case["completed_samples"] == case["requested_samples"] == profile["samples"]
                    assert case["payload_bytes"] == profile["payload"] and case["warmup"] == profile["warmup"]
                    result["requested_samples_completed"] += case["completed_samples"]
                runs[pair, variant] = cases
                result["validated_runs"] += 1
        pairs = [pair for pair in range(plan["pairs"]) if (pair, "baseline") in runs and (pair, "candidate") in runs]
        result["complete_pairs"][profile["name"]] = pairs
        rows = []
        for entry in range(3):
            for owner in range(3):
                row = {"entry": entry, "owner": owner, "path": "local" if entry == owner else "one_hop", "metrics": {}}
                for metric in ["p50", "p95", "p99", "goodput"]:
                    values = {}
                    for variant in ["baseline", "candidate"]:
                        values[variant] = [runs[pair, variant][entry, owner]["echo_goodput_bytes_per_second"] if metric == "goodput"
                                           else runs[pair, variant][entry, owner]["rtt_seconds"][metric] * 1000 for pair in pairs]
                    values["paired_percent_changes"] = [(b / a - 1) * 100 for a, b in zip(values["baseline"], values["candidate"])]
                    row["metrics"][metric] = values
                rows.append(row)
        groups = {group: {metric: [statistics.median(row["metrics"][metric]["paired_percent_changes"][index]
                                                    for row in rows if row["path"] == group) for index in range(len(pairs))]
                          for metric in ["p50", "p95", "p99", "goodput"]} for group in ["local", "one_hop"]}
        result["profiles"][profile["name"]] = {"per_path": rows, "paired_block_median_percent_changes": groups}
    loads = [json.loads(line) for line in (OUT / "load.jsonl").read_text().splitlines()] if (OUT / "load.jsonl").exists() else []
    expected_stages = {f"pair-{pair}-{variant}-{profile['name']}" for pair in range(plan["pairs"])
                       for variant in ["baseline", "candidate"] for profile in plan["profiles"]}
    covered = {row["stage"] for row in loads if row.get("classification_verified")}
    result["telemetry"] = {"samples": len(loads), "measurement_stages_without_verified_classification": sorted(expected_stages - covered)}
    if result["validated_runs"] == 20 and not (expected_stages - covered) and not (OUT / "failure.json").exists() and not (OUT / "abort.json").exists():
        primary = result["profiles"]["serial"]["paired_block_median_percent_changes"]["one_hop"]["p99"]
        criterion = plan["regression_signal"]
        signal_found = regression_signal(primary, criterion)
        result["primary"] = {"block_percent_changes": primary, "median_percent_change": statistics.median(primary), "regression_signal": signal_found}
        result["decision"] = "HOLD: repeated serial p99 regression signal" if signal_found else "No consistent serial p99 regression signal under the predeclared criterion; not equivalence or production capacity proof"
    write_json(OUT / "summary.json", result)
    print(json.dumps({key: result[key] for key in ["validated_runs", "requested_samples_completed", "decision"]}), flush=True)
    return result


class Experiment:
    def __init__(self):
        self.plan = json.loads(PLAN.read_text())
        self.project = "peer-tail-hosted-" + os.environ.get("GITHUB_RUN_ID", "invalid")
        self.sources = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / self.project
        self.compose = ["docker", "compose", "-f", str(self.sources / "candidate/docker-compose.yml"),
                        "-f", str(OUT / "override.json"), "-p", self.project]
        self.abort = threading.Event()
        self.stop = threading.Event()
        self.stage = "prepare"
        self.measurement_started = False
        self.monitor_finished = threading.Event()
        self.reason = None
        self.images = {}
        self.tls_image = None

    def set_stage(self, stage):
        self.stage = stage
        with (OUT / "stages.jsonl").open("a") as log:
            log.write(json.dumps({"time": utc(), "stage": stage}) + "\n")

    def fail(self, reason):
        if not self.abort.is_set():
            self.reason = reason
            write_json(OUT / "abort.json", {"time": utc(), "stage": self.stage, "reason": reason})
            self.abort.set()

    def run(self, args, name, cwd=ROOT, timeout=600, honor_gate=True, check=True):
        if honor_gate and self.abort.is_set():
            raise RuntimeError(self.reason)
        with (OUT / name).open("w") as log:
            child = subprocess.Popen(args, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            started = time.monotonic()
            while child.poll() is None:
                if (honor_gate and self.abort.is_set()) or time.monotonic() - started > timeout:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait(timeout=10)
                    raise RuntimeError(self.reason or "command timeout: " + name)
                time.sleep(0.2)
            if check and child.returncode:
                raise subprocess.CalledProcessError(child.returncode, args)

    def build(self):
        if platform.system() != "Linux" or platform.machine() not in {"x86_64", "amd64"}:
            raise RuntimeError("This independent experiment requires a native Linux amd64 runner")
        if os.environ.get("GITHUB_RUN_ATTEMPT") != "1":
            raise RuntimeError("No workflow reruns are allowed")
        self.sources.mkdir(parents=True, exist_ok=False)
        provenance = {"time": utc(), "run_id": os.environ["GITHUB_RUN_ID"], "workflow_sha": capture(["git", "rev-parse", "HEAD"]).strip(),
                      "platform": platform.platform(), "cpu_count": os.cpu_count(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
                      "plan_sha256": digest(PLAN), "sources": {}}
        if len(provenance["cpu_affinity"]) < 2:
            raise RuntimeError("At least two available runner CPUs are required")
        (OUT / "lscpu.txt").write_text(capture(["lscpu"]))
        (OUT / "docker-version.txt").write_text(capture(["docker", "version"]))
        (OUT / "docker-info.json").write_text(capture(["docker", "info", "--format", '{"cpus":{{.NCPU}},"memory":{{.MemTotal}},"architecture":"{{.Architecture}}","kernel":"{{.KernelVersion}}"}']))
        names = ["deploy/docker/Dockerfile", "Cargo.lock", "rust-toolchain.toml", "examples/echo-probe/src/latency.rs", "docker-compose.yml", "deploy/docker/authorization.test.json", "deploy/docker/shard-directory.json", ".github/scripts/test_observability.py"]
        for variant in ["baseline", "candidate"]:
            sha = self.plan[variant]
            assert capture(["git", "rev-parse", sha]).strip() == sha
            archive = self.sources / (variant + ".tar")
            subprocess.run(["git", "archive", "--format=tar", "--output=" + str(archive), sha], cwd=ROOT, check=True)
            destination = self.sources / variant
            destination.mkdir()
            with tarfile.open(archive) as bundle:
                bundle.extractall(destination, filter="data")
            archive.unlink()
            provenance["sources"][variant] = {"commit": sha, "tree": capture(["git", "rev-parse", sha + "^{tree}"]).strip(),
                                               "file_sha256": {name: digest(destination / name) for name in names}}
        assert provenance["sources"]["baseline"]["file_sha256"] == provenance["sources"]["candidate"]["file_sha256"]
        (OUT / "source-diff.txt").write_text(capture(["git", "diff", "--stat", self.plan["baseline"], self.plan["candidate"]]))
        self.run(["docker", "pull", "--platform", "linux/amd64", "rust:1.96.1-bookworm"], "rust-pull.log", timeout=300)
        rust = json.loads(capture(["docker", "image", "inspect", "rust:1.96.1-bookworm"]))[0]
        rust_digest = rust["RepoDigests"][0]
        original = (self.sources / "candidate/deploy/docker/Dockerfile").read_text()
        assert original.startswith("FROM rust:1.96.1-bookworm AS builder\n")
        effective = OUT / "Dockerfile.pinned"
        effective.write_text(original.replace("FROM rust:1.96.1-bookworm AS builder", "FROM " + rust_digest + " AS builder", 1))
        provenance["rust_base"] = {"id": rust["Id"], "digest": rust_digest, "effective_dockerfile_sha256": digest(effective)}
        write_json(OUT / "provenance.json", provenance)
        for variant in ["baseline", "candidate"]:
            self.set_stage("build-" + variant)
            iid = OUT / (variant + ".iid")
            self.run(["docker", "build", "--platform", "linux/amd64", "--progress=plain", "--file", str(effective), "--target", "test", "--iidfile", str(iid), "--tag", self.project + ":" + variant, "."], variant + "-build.log", cwd=self.sources / variant, timeout=1800)
            self.images[variant] = iid.read_text().strip()
            image = json.loads(capture(["docker", "image", "inspect", self.images[variant]]))[0]
            assert image["Architecture"] == "amd64" and image["Os"] == "linux"
            provenance["sources"][variant]["image"] = self.images[variant]
            write_json(OUT / "provenance.json", provenance)
        # Freeze the existing test TLS helper before measurements; no apk/network work in the runs.
        self.run(["docker", "pull", "--platform", "linux/amd64", "alpine:3.22"], "alpine-pull.log", timeout=300)
        alpine = json.loads(capture(["docker", "image", "inspect", "alpine:3.22"]))[0]
        tls_dockerfile = OUT / "Dockerfile.tls-fixture"
        tls_dockerfile.write_text("FROM " + alpine["RepoDigests"][0] + "\nRUN apk add --no-cache openssl\n")
        self.run(["docker", "build", "--platform", "linux/amd64", "--progress=plain", "--file", str(tls_dockerfile), "--iidfile", str(OUT / "tls.iid"), str(OUT)], "tls-build.log", timeout=300)
        self.tls_image = (OUT / "tls.iid").read_text().strip()
        provenance["tls_fixture"] = {"image": self.tls_image, "base": alpine["RepoDigests"][0], "dockerfile_sha256": digest(tls_dockerfile)}
        provenance["builds_finished"] = utc()
        write_json(OUT / "provenance.json", provenance)
        write_json(OUT / "images.json", self.images)

    def containers(self):
        ids = capture(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=" + self.project]).split()
        if not ids:
            return []
        inspected = json.loads(capture(["docker", "inspect"] + ids))
        return [{"id": value["Id"], "pid": value["State"]["Pid"], "service": value["Config"]["Labels"]["com.docker.compose.service"],
                 "image": value["Image"], "cpuset": value["HostConfig"]["CpusetCpus"], "quota": value["HostConfig"]["CpuQuota"], "nano_cpus": value["HostConfig"]["NanoCpus"]} for value in inspected]

    def monitor(self):
        try:
            self.monitor_loop()
        except BaseException as error:
            self.fail("Telemetry initialization/fatal error: " + str(error))
        finally:
            self.monitor_finished.set()

    def monitor_loop(self):
        hz = os.sysconf("SC_CLK_TCK")
        previous, previous_cpu = processes(), system_cpu()
        previous_time = time.monotonic()
        previous_boot = int(float(Path("/proc/uptime").read_text().split()[0]) * hz)
        external_bad = steal_bad = 0
        while not self.stop.wait(self.plan["telemetry_interval_seconds"]):
            started, stage = time.monotonic(), self.stage
            row = {"time": utc(), "stage": stage}
            try:
                containers = self.containers()
                current, current_cpu = processes(), system_cpu()
                now = time.monotonic()
                interval = now - previous_time
                owners = {pid: ownership(pid, current, containers, os.getpid()) for pid in current}
                totals = {key: 0.0 for key in ["own_container", "harness", "kernel", "docker_infrastructure", "external_user"]}
                details = []
                compilers = []
                for pid, proc in current.items():
                    kind, owner = owners[pid]
                    cores = cpu_delta(proc, previous, previous_boot) / hz / interval
                    totals[kind] += cores
                    detail = {"pid": pid, "comm": proc["comm"], "cpu_cores": cores, "class": kind, "container": owner}
                    if cores > 0:
                        details.append(detail)
                    if proc["comm"] in COMPILERS and proc["state"] != "Z":
                        compilers.append(detail)
                deltas = [max(0, new - old) for new, old in zip(current_cpu, previous_cpu)]
                total = sum(deltas)
                steal = 100 * deltas[7] / total if total else 0
                busy = 100 * (total - deltas[3] - deltas[4]) / total if total else 0
                residual = unattributed_task_cpu(deltas, sum(totals.values()), hz, interval)
                guarded_external = totals["external_user"] + residual
                guard = self.plan["load_guard"]
                external_bad = external_bad + 1 if guarded_external > guard["external_plus_unattributed_cpu_cores_max"] else 0
                steal_bad = steal_bad + 1 if steal > guard["steal_percent_max"] else 0
                row.update({"interval_seconds": interval, "cpu_cores": totals, "host_busy_percent": busy, "steal_percent": steal,
                            "unattributed_task_cpu_cores": residual, "external_guard_cpu_cores": guarded_external,
                            "irq_softirq_cpu_cores": sum(deltas[5:7]) / hz / interval,
                            "external_violations": external_bad, "steal_violations": steal_bad, "processes": sorted(details, key=lambda value: value["cpu_cores"], reverse=True), "compilers": compilers, "containers": containers})
                if stage == self.stage and stage.endswith(("-serial", "-idle")):
                    verify_membership(current, containers, owners)
                    row["classification_verified"] = True
                if max(external_bad, steal_bad) >= guard["consecutive_violations"]:
                    self.fail("External/unattributed CPU or steal guard exceeded: " + json.dumps(totals) + "; residual=" + str(residual) + "; steal=" + str(steal))
                if self.measurement_started and compilers:
                    self.fail("Unexpected compiler after measurement start: " + json.dumps(compilers))
                previous, previous_cpu, previous_time = current, current_cpu, now
                previous_boot = int(float(Path("/proc/uptime").read_text().split()[0]) * hz)
            except Exception as error:
                row["error"] = str(error)
                self.fail("Telemetry/classification failed: " + str(error))
            row["sample_duration_seconds"] = time.monotonic() - started
            with (OUT / "load.jsonl").open("a") as log:
                log.write(json.dumps(row) + "\n")

    def measure(self):
        config = json.loads(capture(["docker", "compose", "-f", str(self.sources / "candidate/docker-compose.yml"), "config", "--format", "json"]))
        tls_command = config["services"]["tls-init"]["command"]
        assert len(tls_command) == 1 and tls_command[0].startswith("apk add --no-cache openssl >/dev/null\n")
        tls_command = [tls_command[0].removeprefix("apk add --no-cache openssl >/dev/null\n")]
        for pair, variants in enumerate(self.plan["variant_order"]):
            for variant in variants:
                prefix = f"pair-{pair}-{variant}"
                image = self.images[variant]
                override = {"services": {service: {"image": image} for service in SERVICES + ["topology-probe"]}}
                override["services"]["tls-init"] = {"image": self.tls_image, "command": tls_command}
                for service in ["gateway-a", "gateway-b", "gateway-c"]:
                    override["services"][service]["environment"] = {"RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND": str(self.plan["control_limit"]), "RELAYGATE_SESSION_CONTROL_BURST": str(self.plan["control_limit"])}
                write_json(OUT / "override.json", override)
                (OUT / (prefix + "-config.json")).write_text(capture(self.compose + ["config", "--format", "json"]))
                try:
                    self.set_stage(prefix + "-startup")
                    self.run(self.compose + ["up", "--no-build", "-d", "--wait", "--wait-timeout", "60"] + SERVICES, prefix + "-startup.log", timeout=90)
                    containers = self.containers()
                    assert {value["service"] for value in containers} == set(SERVICES + ["tls-init"])
                    for container in containers:
                        assert container["image"] == (self.tls_image if container["service"] == "tls-init" else image)
                        assert container["cpuset"] == "" and container["quota"] == container["nano_cpus"] == 0
                    write_json(OUT / (prefix + "-running.json"), containers)
                    for profile_name in self.plan["profile_order"][pair]:
                        profile = next(value for value in self.plan["profiles"] if value["name"] == profile_name)
                        name = prefix + "-" + profile_name
                        self.set_stage(name)
                        self.measurement_started = True
                        args = []
                        for key, value in [("WARMUP", profile["warmup"]), ("SAMPLES", profile["samples"]), ("PAYLOAD_BYTES", profile["payload"]), ("CONCURRENCY", profile["active"]), ("IDLE_PIPES", profile["idle"])]:
                            args += ["-e", "RELAYGATE_LATENCY_" + key + "=" + str(value)]
                        self.run(self.compose + ["run", "--rm", "--no-deps"] + args + ["topology-probe", "relaygate-echo-probe", "latency"], name + ".log")
                        self.run(["python3", str(self.sources / "candidate/.github/scripts/test_observability.py"), "--probe", str(OUT / (name + ".log")), "--probe-concurrency", str(profile["active"]), "--probe-idle-pipes", str(profile["idle"])], name + "-validation.log", timeout=30)
                        (OUT / (name + ".validated")).write_text(utc() + "\n")
                finally:
                    self.cleanup(prefix)
                print(prefix + " complete", flush=True)

    def cleanup(self, prefix="final"):
        self.set_stage(prefix + "-cleanup")
        if (OUT / "override.json").exists():
            try:
                self.run(self.compose + ["logs", "--no-color"], prefix + "-compose.log", honor_gate=False, check=False, timeout=60)
            finally:
                self.run(self.compose + ["down", "--volumes", "--remove-orphans"], prefix + "-cleanup.log", honor_gate=False, timeout=90)
        leftovers = {kind: capture(["docker", kind, "ls", "-q", "--filter", "label=com.docker.compose.project=" + self.project]).split() for kind in ["volume", "network"]}
        leftovers["containers"] = capture(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + self.project]).split()
        write_json(OUT / "cleanup.json", leftovers)
        assert not any(leftovers.values()), leftovers

    def execute(self):
        OUT.mkdir(parents=True, exist_ok=True)
        if (OUT / "started.json").exists():
            raise RuntimeError("This fixed attempt already started; no retry/overwrite")
        os.environ.pop("RELAYGATE_ACCESS_TOKEN", None)
        (OUT / "plan.json").write_bytes(PLAN.read_bytes())
        write_json(OUT / "started.json", {"time": utc(), "plan_sha256": digest(PLAN), "project": self.project})
        monitor = None
        try:
            self.build()
            self.set_stage("settle")
            monitor = threading.Thread(target=self.monitor, daemon=True)
            monitor.start()
            if self.abort.wait(self.plan["settle_seconds"]):
                raise RuntimeError(self.reason)
            self.measure()
            if self.abort.is_set():
                raise RuntimeError(self.reason)
        except BaseException as error:
            write_json(OUT / "failure.json", {"time": utc(), "type": type(error).__name__, "error": str(error)})
            raise
        finally:
            self.stop.set()
            if monitor:
                monitor.join(timeout=35)
                if monitor.is_alive():
                    self.fail("Telemetry thread did not stop")
                if not self.monitor_finished.is_set():
                    self.fail("Telemetry completion was not confirmed")
            try:
                self.cleanup()
            except BaseException as error:
                write_json(OUT / "failure.json", {"time": utc(), "type": "cleanup", "error": str(error)})
                raise
            finally:
                result = summarize(self.plan)
                (OUT / "decision.txt").write_text(result["decision"] + "\n")
        if result["decision"].startswith("HOLD"):
            raise RuntimeError(result["decision"])


if __name__ == "__main__":
    experiment = Experiment()
    if sys.argv[1:] == ["run"]:
        experiment.execute()
    elif sys.argv[1:] == ["cleanup"]:
        OUT.mkdir(parents=True, exist_ok=True)
        experiment.cleanup("workflow-final")
    else:
        raise SystemExit("Expected run or cleanup")
