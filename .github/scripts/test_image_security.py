import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from image_security import validate


def clean_report():
    return {"SchemaVersion": 2, "Results": [
        {"Type": "debian", "Packages": [{"Name": "libc6"}]},
        {"Type": "rustbinary", "Packages": [{"Name": "rustls", "Version": "0.23.45"}]},
    ]}


class ImageSecurityTests(unittest.TestCase):
    def test_clean_auditable_image_passes(self):
        validate(clean_report())

    def test_os_only_scan_cannot_claim_rust_coverage(self):
        report = clean_report()
        report["Results"].pop()
        with self.assertRaisesRegex(ValueError, "Rust binary"):
            validate(report)

    def test_rust_result_without_inventory_fails(self):
        report = clean_report()
        report["Results"][1]["Packages"] = []
        with self.assertRaises(ValueError):
            validate(report)

    def test_fixed_and_unfixed_findings_block(self):
        for kind in ("Vulnerabilities", "Secrets"):
            for severity in ("HIGH", "CRITICAL"):
                for fixed in ("", "1.2.3"):
                    with self.subTest(kind=kind, severity=severity, fixed=fixed):
                        report = clean_report()
                        report["Results"][0][kind] = [{"Severity": severity, "FixedVersion": fixed}]
                        with self.assertRaisesRegex(ValueError, "findings"):
                            validate(report)

    def test_lower_severity_does_not_block(self):
        report = clean_report()
        report["Results"][0]["Vulnerabilities"] = [{"Severity": "MEDIUM"}]
        validate(report)

    def test_missing_results_and_empty_scan_fail(self):
        for report in ({}, {"SchemaVersion": 2}, {"SchemaVersion": 2, "Results": []}):
            with self.subTest(report=report), self.assertRaises(ValueError):
                validate(report)

    def test_cli_fails_closed_on_scanner_error_or_invalid_report(self):
        script = Path(__file__).with_name("image_security.py")
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "trivy.json"
            for content in (None, "", "not JSON", json.dumps({"error": "database download failed"})):
                with self.subTest(content=content):
                    if content is not None:
                        report.write_text(content)
                    result = subprocess.run([sys.executable, str(script), str(report)], capture_output=True)
                    self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
