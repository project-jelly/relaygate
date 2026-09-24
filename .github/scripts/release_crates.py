#!/usr/bin/env python3
"""Resume a crates.io release after verifying every already published package.

The default is a read-only preflight; only --publish uploads missing versions.
Run from a clean repository root after check-crate-packages.sh has passed.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time
import tomllib
from urllib.error import HTTPError
from urllib.request import Request, urlopen

PACKAGES = (
    "relaygate-destination",
    "relaygate-transport",
    "relaygate-protocol",
    "relaygate-token-issuer",
    "relaygate-sdk",
)
USER_AGENT = "relaygate-release (https://github.com/project-jelly/relaygate)"


def fetch(url, allow_missing=False):
    try:
        with urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=30) as response:
            return response.read()
    except HTTPError as error:
        if allow_missing and error.code == 404:
            return None
        raise


def published_version(package, version):
    body = fetch(f"https://crates.io/api/v1/crates/{package}/{version}", allow_missing=True)
    if body is None:
        return None
    entry = json.loads(body)["version"]
    if entry["crate"] != package or entry["num"] != version or entry["yanked"]:
        raise ValueError(f"{package} {version}: unexpected or yanked registry version")
    return entry


def wait_for_index(package, version):
    # All release package names have at least four characters.
    path = f"{package[:2]}/{package[2:4]}/{package}"
    for attempt in range(60):
        body = fetch(f"https://index.crates.io/{path}", allow_missing=True)
        for line in (body or b"").splitlines():
            entry = json.loads(line)
            if entry["vers"] == version:
                if entry["yanked"]:
                    raise ValueError(f"{package} {version}: registry index version is yanked")
                return
        print(f"Waiting for {package} {version} in crates.io index ({attempt + 1}/60)", flush=True)
        time.sleep(5)
    raise RuntimeError(f"{package} {version} did not reach the crates.io index")


def package_contents(data, package, version):
    prefix = f"{package}-{version}/"
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for entry in archive:
            if not entry.isfile() or not entry.name.startswith(prefix):
                raise ValueError(f"Unexpected archive entry: {entry.name}")
            name = entry.name[len(prefix):]
            if not name or any(part in ("", ".", "..") for part in name.split("/")) or name in files:
                raise ValueError(f"Invalid or duplicate archive path: {entry.name}")
            content = archive.extractfile(entry).read()
            if name == ".cargo_vcs_info.json":
                # An unrelated main commit changes this SHA without changing the package.
                vcs = json.loads(content)
                vcs["git"].pop("sha1", None)
                content = json.dumps(vcs, sort_keys=True).encode()
            files[name] = content
    if "Cargo.toml" not in files:
        raise ValueError("Package archive is missing Cargo.toml")
    return files


def verify_existing(package, version, entry, target):
    data = fetch(f"https://static.crates.io/crates/{package}/{package}-{version}.crate")
    if hashlib.sha256(data).hexdigest() != entry["checksum"]:
        raise ValueError(f"{package} {version}: registry archive checksum mismatch")
    subprocess.run(
        ["cargo", "package", "--locked", "--no-verify", "-p", package],
        env={**os.environ, "CARGO_TARGET_DIR": str(target)},
        check=True,
    )
    candidate = (target / "package" / f"{package}-{version}.crate").read_bytes()
    actual = package_contents(data, package, version)
    expected = package_contents(candidate, package, version)
    changed = sorted(name for name in actual.keys() | expected.keys() if actual.get(name) != expected.get(name))
    if changed:
        raise ValueError(f"{package} {version}: published contents differ: {', '.join(changed)}; use a new version")


def release(version, publish=False):
    pending = []
    with tempfile.TemporaryDirectory(prefix="relaygate-release-") as directory:
        for package in PACKAGES:
            entry = published_version(package, version)
            if entry is None:
                pending.append(package)
                continue
            wait_for_index(package, version)
            verify_existing(package, version, entry, Path(directory))
            print(f"Skip {package} {version}: published contents match", flush=True)

    # Complete the entire preflight before making any registry changes.
    print(f"Unpublished {version}: {', '.join(pending) or '(none)'}", flush=True)
    if publish:
        for package in pending:
            subprocess.run(["cargo", "publish", "--locked", "-p", package], check=True)
            wait_for_index(package, version)
    return pending


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true", help="publish missing versions after preflight")
    args = parser.parse_args()
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("Release requires a clean worktree")
    with open("Cargo.toml", "rb") as manifest:
        version = tomllib.load(manifest)["workspace"]["package"]["version"]
    release(version, publish=args.publish)


if __name__ == "__main__":
    main()
