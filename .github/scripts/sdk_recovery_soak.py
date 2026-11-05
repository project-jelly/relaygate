#!/usr/bin/env python3
"""Run the opt-in TLS recovery soak in one process and sample its RSS/FD usage."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time


def resources(pid):
    proc = Path(f"/proc/{pid}")
    if proc.exists():
        status = dict(line.split(":", 1) for line in (proc / "status").read_text().splitlines())
        return int(status["VmRSS"].split()[0]), len(list((proc / "fd").iterdir()))
    rss = int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True))
    files = subprocess.check_output(["lsof", "-a", "-p", str(pid), "-Ff"], text=True)
    fds = sum(line.startswith("f") and line[1:].isdigit() for line in files.splitlines())
    return rss, fds


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--output", type=Path, default=Path("target/sdk-recovery-soak"))
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    if platform.system() not in ("Linux", "Darwin"):
        parser.error("resource sampling supports Linux and macOS")
    if platform.system() == "Darwin" and not shutil.which("lsof"):
        parser.error("macOS sampling requires lsof")
    args.output.mkdir(parents=True, exist_ok=True)
    build = subprocess.check_output([
        "cargo", "test", "--locked", "-p", "relaygate-gateway", "--test",
        "sdk_gateway_contract", "--no-run", "--message-format=json",
    ], text=True)
    artifacts = [json.loads(line) for line in build.splitlines()]
    binary = next(item["executable"] for item in artifacts
                  if item.get("executable") and item["target"]["name"] == "sdk_gateway_contract")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    metadata = {"revision": revision, "platform": platform.platform(), "seconds": args.seconds,
                "binary": binary, "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(), "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"]))}
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with (args.output / "runtime.log").open("w") as log, (args.output / "resources.jsonl").open("w") as samples:
        child = subprocess.Popen([
            binary, "recovery_soak::tls_recovery_soak", "--ignored", "--exact", "--nocapture",
        ], stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, "RELAYGATE_RECOVERY_SOAK_SECS": str(args.seconds)})
        print(f"soak pid={child.pid} seconds={args.seconds} output={args.output}", flush=True)
        started = time.monotonic()
        try:
            while child.poll() is None:
                if time.monotonic() - started > args.seconds + 60:
                    raise TimeoutError("soak exceeded its duration and cleanup budget")
                try:
                    rss, fds = resources(child.pid)
                except (OSError, ValueError, KeyError, subprocess.CalledProcessError):
                    if child.poll() is not None:
                        break
                    raise
                samples.write(json.dumps({"elapsed_s": round(time.monotonic() - started, 2),
                                          "rss_kib": rss, "fds": fds}) + "\n")
                samples.flush()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        result = {"exit_code": child.returncode, "elapsed_s": round(time.monotonic() - started, 2)}
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
        return child.returncode


if __name__ == "__main__":
    raise SystemExit(main())
