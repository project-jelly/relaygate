import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("promote_image.sh")
DIGEST = "sha256:" + "a" * 64


class PromotionTests(unittest.TestCase):
    def run_promotion(self, existing, current="1.2.3"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = root / "docker"
            docker.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
root=pathlib.Path(os.environ['TEST_ROOT'])
if sys.argv[3] == 'create':
    (root/'created').write_text(' '.join(sys.argv[4:]))
    sys.exit(0)
if not (root/'created').exists():
    existing=os.environ['EXISTING']
    if existing in ('missing', 'network', 'credentials'):
        messages = {'missing': 'ERROR: '+os.environ['IMAGE']+':'+os.environ['VERSION']+': not found',
                    'network': 'TLS handshake timeout', 'credentials': 'docker-credential-osxkeychain executable not found'}
        print(messages[existing], file=sys.stderr)
        sys.exit(1)
    print(json.dumps({'digest':existing}))
else:
    print(json.dumps({'digest':os.environ['DIGEST']}))
''')
            docker.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", TEST_ROOT=str(root), EXISTING=existing,
                       IMAGE="ghcr.io/project-jelly/example", DIGEST=DIGEST, VERSION="1.2.3", LATEST_VERSION=current)
            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            created = root / "created"
            return result.returncode, created.read_text() if created.exists() else ""

    def test_new_version_promotes_only_scanned_digest(self):
        code, command = self.run_promotion("missing")
        self.assertEqual(code, 0)
        self.assertIn("@" + DIGEST, command)
        self.assertIn(":latest", command)

    def test_existing_same_digest_can_resume(self):
        self.assertEqual(self.run_promotion(DIGEST)[0], 0)

    def test_existing_different_digest_cannot_be_overwritten(self):
        code, command = self.run_promotion("sha256:" + "b" * 64)
        self.assertNotEqual(code, 0)
        self.assertEqual(command, "")

    def test_registry_failure_is_not_treated_as_missing_tag(self):
        code, command = self.run_promotion("network")
        self.assertNotEqual(code, 0)
        self.assertEqual(command, "")

    def test_generic_not_found_is_not_manifest_absence(self):
        code, command = self.run_promotion("credentials")
        self.assertNotEqual(code, 0)
        self.assertEqual(command, "")

    def test_old_version_does_not_replace_latest(self):
        code, command = self.run_promotion("missing", current="1.2.4")
        self.assertEqual(code, 0)
        self.assertNotIn(":latest", command)


if __name__ == "__main__":
    unittest.main()
