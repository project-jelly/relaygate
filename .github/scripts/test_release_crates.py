import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import call, patch
from urllib.error import HTTPError

import release_crates as release

VERSION = "0.5.5"
PACKAGE = release.PACKAGES[0]


def archive(files=None, sha="old"):
    contents = {
        "Cargo.toml": b"[package]\nname = 'relaygate-destination'\nversion = '0.5.5'\n",
        "Cargo.lock": b"locked dependencies",
        "src/lib.rs": b"pub fn example() {}",
        ".cargo_vcs_info.json": json.dumps({"git": {"sha1": sha}, "path_in_vcs": "crates/relaygate-destination"}).encode(),
    }
    contents.update(files or {})
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as output:
        for name, data in contents.items():
            entry = tarfile.TarInfo(f"{PACKAGE}-{VERSION}/{name}")
            entry.size = len(data)
            output.addfile(entry, io.BytesIO(data))
    return buffer.getvalue()


class ExistingPackageTests(unittest.TestCase):
    def test_only_git_commit_can_differ(self):
        original = release.package_contents(archive(), PACKAGE, VERSION)
        self.assertEqual(original, release.package_contents(archive(sha="new"), PACKAGE, VERSION))
        for name in ("src/lib.rs", "Cargo.toml", "Cargo.lock", "README.md"):
            with self.subTest(name=name):
                self.assertNotEqual(original, release.package_contents(archive({name: b"changed"}), PACKAGE, VERSION))
        dirty = json.dumps({"git": {"sha1": "new", "dirty": True}, "path_in_vcs": "crates/relaygate-destination"}).encode()
        self.assertNotEqual(original, release.package_contents(archive({".cargo_vcs_info.json": dirty}), PACKAGE, VERSION))

    def test_invalid_archive_path_is_rejected_without_extraction(self):
        with self.assertRaisesRegex(ValueError, "Invalid or duplicate"):
            release.package_contents(archive({"../outside": b"bad"}), PACKAGE, VERSION)

    def test_registry_checksum_and_contents_are_verified(self):
        published = archive()
        entry = {"checksum": hashlib.sha256(published).hexdigest()}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            (target / "package").mkdir()
            candidate = target / "package" / f"{PACKAGE}-{VERSION}.crate"
            candidate.write_bytes(archive(sha="new"))
            with patch.object(release, "fetch", return_value=published), patch.object(release.subprocess, "run") as run:
                release.verify_existing(PACKAGE, VERSION, entry, target)
                self.assertEqual(run.call_args.args[0], ["cargo", "package", "--locked", "--no-verify", "-p", PACKAGE])
                candidate.write_bytes(archive({"src/lib.rs": b"different source"}))
                with self.assertRaisesRegex(ValueError, "published contents differ: src/lib.rs"):
                    release.verify_existing(PACKAGE, VERSION, entry, target)
                run.reset_mock()
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    release.verify_existing(PACKAGE, VERSION, {"checksum": "wrong"}, target)
                run.assert_not_called()


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.registry = {}
        self.lookup = patch.object(release, "published_version", side_effect=lambda package, version: self.registry.get(package)).start()
        self.verify = patch.object(release, "verify_existing").start()
        self.wait = patch.object(release, "wait_for_index").start()
        self.run = patch.object(release.subprocess, "run").start()
        self.addCleanup(patch.stopall)

    def test_partial_publish_can_resume_without_republishing(self):
        def upload_then_fail(command, **kwargs):
            self.registry[command[-1]] = {"checksum": "published"}
            raise subprocess.CalledProcessError(1, command)

        self.run.side_effect = upload_then_fail
        with self.assertRaises(subprocess.CalledProcessError):
            release.release(VERSION, publish=True)
        self.assertEqual(list(self.registry), [PACKAGE])

        self.run.reset_mock(side_effect=True)
        pending = release.release(VERSION, publish=True)
        self.assertEqual(pending, list(release.PACKAGES[1:]))
        self.assertEqual(self.verify.call_args.args[:2], (PACKAGE, VERSION))
        self.assertEqual(self.run.call_args_list, [call(["cargo", "publish", "--locked", "-p", name], check=True) for name in pending])
        self.assertEqual(self.wait.call_args_list, [call(name, VERSION) for name in release.PACKAGES])

    def test_mismatch_in_last_package_blocks_all_new_uploads(self):
        self.registry[release.PACKAGES[-1]] = {"checksum": "existing"}
        self.verify.side_effect = ValueError("published contents differ")
        with self.assertRaisesRegex(ValueError, "published contents differ"):
            release.release(VERSION, publish=True)
        self.run.assert_not_called()

    def test_all_published_is_a_verified_no_op(self):
        self.registry.update({name: {"checksum": "existing"} for name in release.PACKAGES})
        self.assertEqual(release.release(VERSION, publish=True), [])
        self.assertEqual(self.verify.call_count, len(release.PACKAGES))
        self.run.assert_not_called()

    def test_default_preflight_never_publishes(self):
        self.assertEqual(release.release(VERSION), list(release.PACKAGES))
        self.run.assert_not_called()

    def test_registry_failure_is_not_treated_as_unpublished(self):
        self.lookup.side_effect = HTTPError("https://crates.io", 503, "Unavailable", {}, None)
        with self.assertRaises(HTTPError):
            release.release(VERSION, publish=True)
        self.run.assert_not_called()


class RegistryTests(unittest.TestCase):
    def test_only_http_404_is_missing(self):
        for code in (404, 403, 429, 500):
            with self.subTest(code=code), patch.object(release, "urlopen", side_effect=HTTPError("https://crates.io", code, "error", {}, None)):
                if code == 404:
                    self.assertIsNone(release.fetch("https://crates.io", allow_missing=True))
                else:
                    with self.assertRaises(HTTPError):
                        release.fetch("https://crates.io", allow_missing=True)

    def test_yanked_or_wrong_version_is_rejected(self):
        for change in ({"yanked": True}, {"num": "0.5.4"}, {"crate": "another"}):
            entry = {"crate": PACKAGE, "num": VERSION, "yanked": False, **change}
            with self.subTest(change=change), patch.object(release, "fetch", return_value=json.dumps({"version": entry}).encode()):
                with self.assertRaisesRegex(ValueError, "unexpected or yanked"):
                    release.published_version(PACKAGE, VERSION)

    def test_index_visibility_wait_and_timeout(self):
        body = json.dumps({"vers": VERSION, "yanked": False}).encode()
        with patch.object(release, "fetch", side_effect=[None, body]), patch.object(release.time, "sleep") as sleep:
            release.wait_for_index(PACKAGE, VERSION)
            sleep.assert_called_once_with(5)
        with patch.object(release, "fetch", return_value=None), patch.object(release.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "did not reach"):
                release.wait_for_index(PACKAGE, VERSION)
        with patch.object(release, "fetch", return_value=json.dumps({"vers": VERSION, "yanked": True}).encode()):
            with self.assertRaisesRegex(ValueError, "yanked"):
                release.wait_for_index(PACKAGE, VERSION)


if __name__ == "__main__":
    unittest.main()
