"""Fail closed on incomplete image evidence, missing Rust metadata or findings."""

import argparse
import json
from pathlib import Path


def validate(report, require_rust=True):
    if report.get("SchemaVersion") != 2 or not isinstance(report.get("Results"), list):
        raise ValueError("Missing or unsupported Trivy results")
    results = report["Results"]
    if not results:
        raise ValueError("Empty image scan")
    if require_rust and not any(
        result.get("Type") == "rustbinary" and result.get("Packages")
        for result in results
    ):
        raise ValueError("No Rust binary dependency inventory; cargo-auditable metadata is required")
    findings = [
        finding
        for result in results
        for kind in ("Vulnerabilities", "Secrets")
        for finding in result.get(kind, []) or []
        if finding.get("Severity") in ("HIGH", "CRITICAL")
    ]
    if findings:
        raise ValueError(f"{len(findings)} HIGH/CRITICAL vulnerability or secret findings")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    validate(json.loads(args.report.read_text()))


if __name__ == "__main__":
    main()
