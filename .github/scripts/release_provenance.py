"""Bind a signed SLSA v1 statement to the checked-out source and trusted CI run.

workflow_run's GITHUB_SHA identifies the workflow revision, not necessarily the
source being built. Keep these identities separate in the signed predicate.
"""

import argparse
import json
import os
from pathlib import Path
import re

PREDICATE_TYPE = "https://slsa.dev/provenance/v1"
BUILD_TYPE = "https://project-jelly.github.io/buildtypes/container-release/v1"


def predicate(env):
    source_sha = env["RELEASE_SHA"]
    workflow_sha = env["GITHUB_WORKFLOW_SHA"]
    if not all(re.fullmatch(r"[a-f0-9]{40}", sha) for sha in (source_sha, workflow_sha)):
        raise ValueError("Source and workflow revisions must be full commit SHAs")
    repository = f'{env["GITHUB_SERVER_URL"]}/{env["GITHUB_REPOSITORY"]}'
    workflow_ref = env["GITHUB_WORKFLOW_REF"]
    return {
        "buildDefinition": {
            "buildType": BUILD_TYPE,
            "externalParameters": {
                "source": {"repository": repository, "commit": source_sha},
                "ci": {"runId": env["CI_RUN_ID"], "runAttempt": env["CI_RUN_ATTEMPT"]},
            },
            "internalParameters": {
                "workflow": {"ref": workflow_ref, "commit": workflow_sha},
            },
            "resolvedDependencies": [
                {"uri": f"git+{repository}@{source_sha}", "digest": {"gitCommit": source_sha}},
            ],
        },
        "runDetails": {
            "builder": {"id": f'{env["GITHUB_SERVER_URL"]}/{workflow_ref}'},
            "metadata": {
                "invocationId": f'{repository}/actions/runs/{env["GITHUB_RUN_ID"]}/attempts/{env["GITHUB_RUN_ATTEMPT"]}'
            },
        },
    }


def statement_predicate_type(statement):
    snake_case = statement.get("predicate_type")
    camel_case = statement.get("predicateType")
    if "predicate_type" in statement and "predicateType" in statement and snake_case != camel_case:
        raise ValueError("Conflicting predicate type fields in gh verification output")
    return snake_case if "predicate_type" in statement else camel_case


def verify(results, expected):
    # Only inspect gh's cryptographically verified statements, never raw bundles.
    for result in results:
        statement = result.get("verificationResult", {}).get("statement", {})
        if statement_predicate_type(statement) == PREDICATE_TYPE and statement.get("predicate") == expected:
            return
    raise ValueError("No verified attestation matches the source, CI and build invocation")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("generate", "verify"))
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    expected = predicate(os.environ)
    if args.command == "generate":
        args.path.write_text(json.dumps(expected, indent=2) + "\n")
    else:
        verify(json.loads(args.path.read_text()), expected)


if __name__ == "__main__":
    main()
