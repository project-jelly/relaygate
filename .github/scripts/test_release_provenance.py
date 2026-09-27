import copy
import json
from pathlib import Path
import unittest

from release_provenance import predicate, verify, PREDICATE_TYPE


ENV = {
    "RELEASE_SHA": "a" * 40,
    "GITHUB_WORKFLOW_SHA": "b" * 40,
    "GITHUB_SERVER_URL": "https://github.com",
    "GITHUB_REPOSITORY": "project-jelly/relaygate",
    "GITHUB_WORKFLOW_REF": "project-jelly/relaygate/.github/workflows/release.yml@refs/heads/main",
    "CI_RUN_ID": "123",
    "CI_RUN_ATTEMPT": "1",
    "GITHUB_RUN_ID": "456",
    "GITHUB_RUN_ATTEMPT": "2",
}


class ProvenanceTests(unittest.TestCase):
    def test_workflow_revision_is_not_mistaken_for_source(self):
        result = predicate(ENV)
        self.assertEqual(result["buildDefinition"]["resolvedDependencies"][0]["digest"]["gitCommit"], "a" * 40)
        self.assertEqual(result["buildDefinition"]["internalParameters"]["workflow"]["commit"], "b" * 40)

    def test_valid_verified_result_matches(self):
        expected = predicate(ENV)
        verify([{"verificationResult": {"statement": {"predicateType": PREDICATE_TYPE, "predicate": expected}}}], expected)

    def test_observed_gh_verified_output_shape(self):
        # Structure from a successful gh attestation verify response; values sanitized.
        results = json.loads((Path(__file__).parent / "fixtures" / "gh-verified-attestation.json").read_text())
        expected = results[0]["verificationResult"]["statement"]["predicate"]
        verify(results, expected)
        with self.assertRaises(ValueError):
            verify(results, {"different": "predicate"})

    def test_dual_type_fields_must_agree(self):
        expected = predicate(ENV)
        statement = {"predicate_type": PREDICATE_TYPE, "predicateType": PREDICATE_TYPE, "predicate": expected}
        verify([{"verificationResult": {"statement": statement}}], expected)
        for other in ["https://example.org/conflict", None]:
            statement["predicateType"] = other
            with self.subTest(other=other), self.assertRaises(ValueError):
                verify([{"verificationResult": {"statement": statement}}], expected)

    def test_wrong_source_or_ci_or_workflow_are_rejected(self):
        expected = predicate(ENV)
        for key in ("RELEASE_SHA", "CI_RUN_ID", "CI_RUN_ATTEMPT", "GITHUB_WORKFLOW_SHA", "GITHUB_RUN_ID"):
            changed = dict(ENV)
            changed[key] = "c" * 40
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify([{"verificationResult": {"statement": {"predicateType": PREDICATE_TYPE, "predicate": predicate(changed)}}}], expected)

    def test_unverified_raw_statement_is_rejected(self):
        with self.assertRaises(ValueError):
            verify([{"statement": {"predicateType": PREDICATE_TYPE, "predicate": predicate(ENV)}}], predicate(ENV))

    def test_short_sha_is_rejected(self):
        with self.assertRaises(ValueError):
            predicate(dict(ENV, RELEASE_SHA="abc"))


if __name__ == "__main__":
    unittest.main()
