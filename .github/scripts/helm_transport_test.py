"""Transport and platform annotation rendering contracts (run after Helm setup)."""

import json
from pathlib import Path
import subprocess
import unittest


CHART = Path(__file__).resolve().parents[2] / "deploy/helm/relaygate"


def render(*settings, annotations=None):
    command = ["helm", "template", "relaygate", str(CHART), "--namespace", "relaygate", "--kube-version", "1.32.0"]
    for setting in settings:
        command.extend(["--set", setting])
    for component, values in (annotations or {}).items():
        command.extend(["--set-json", f"{component}.annotations={json.dumps(values)}"])
    return subprocess.run(command, capture_output=True, text=True, check=False)


def workload(output, component):
    marker = f"# Source: relaygate/templates/{component}-statefulset.yaml"
    return output.split(marker, 1)[1].split("\n---", 1)[0]


class TransportTests(unittest.TestCase):
    def successful(self, *settings, **kwargs):
        result = render(*settings, **kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_default_requires_tls_without_platform_restart_policy(self):
        output = self.successful()
        self.assertEqual(output.count("  replicas: 1\n"), 2)
        self.assertNotIn("kind: Certificate", output)
        self.assertNotIn("kind: Issuer", output)
        for component in ("gateway", "route-table"):
            item = workload(output, component)
            self.assertIn('name: RELAYGATE_INTERNAL_TRANSPORT\n              value: "mtls"', item)
            self.assertIn("name: RELAYGATE_INTERNAL_TLS_CERT_PATH", item)
            self.assertIn("mountPath: /etc/relaygate/tls/internal", item)
            self.assertNotIn("secret.reloader.stakater.com/reload:", item)
            for annotation in ("authorization-reload", "edge-tls-reload", "internal-tls-reload"):
                self.assertNotIn(f"relaygate.io/{annotation}:", item)

    def test_plaintext_keeps_edge_tls_and_authorization_but_removes_internal_certificates(self):
        output = self.successful("tls.internal.mode=plaintext")
        for component in ("gateway", "route-table"):
            item = workload(output, component)
            self.assertIn('name: RELAYGATE_INTERNAL_TRANSPORT\n              value: "plaintext"', item)
            self.assertNotIn("RELAYGATE_INTERNAL_GATEWAY_KEYS", item)
            self.assertNotIn("RELAYGATE_INTERNAL_TLS_", item)
            self.assertNotIn("name: internal-tls", item)
            self.assertNotIn("RELAYGATE_INSECURE_TEST_TRANSPORT", item)
        self.assertIn("RELAYGATE_SDK_TLS_CERT_PATH", workload(output, "gateway"))
        self.assertIn("RELAYGATE_AUTH_CONFIG_PATH", workload(output, "gateway"))
        self.assertNotIn("RELAYGATE_CLUSTER_TOKEN", workload(output, "gateway"))
        self.assertNotIn("kind: Certificate", output)

    def test_existing_secrets_mount_only_role_leaf_and_public_trust(self):
        output = self.successful(
            "tls.internal.trustSecret=public-trust",
            "tls.internal.gatewaySecret=gw-leaf",
            "tls.internal.routeTableSecret=rt-leaf",
        )
        self.assertNotIn("cert-manager.io/", output)
        for component, leaf, other in (("gateway", "gw-leaf", "rt-leaf"), ("route-table", "rt-leaf", "gw-leaf")):
            item = workload(output, component)
            self.assertIn('name: "public-trust"', item)
            self.assertIn(f'name: "{leaf}"', item)
            self.assertNotIn(f'name: "{other}"', item)

    def test_workload_annotations_are_independent_of_pod_templates_and_transport(self):
        for mode in ("mtls", "plaintext"):
            baseline = self.successful(f"tls.internal.mode={mode}")
            for values_key, component, other in (("gateway", "gateway", "route-table"), ("routeTable", "route-table", "gateway")):
                with self.subTest(mode=mode, component=component):
                    output = self.successful(
                        f"tls.internal.mode={mode}",
                        annotations={values_key: {
                            "secret.reloader.stakater.com/reload": "edge,trust,leaf",
                            "example.com/operator": "external",
                        }},
                    )
                    metadata, spec = workload(output, component).split("\nspec:", 1)
                    self.assertIn("secret.reloader.stakater.com/reload: edge,trust,leaf", metadata)
                    self.assertIn("example.com/operator: external", metadata)
                    self.assertEqual(spec, workload(baseline, component).split("\nspec:", 1)[1])
                    self.assertEqual(workload(output, other), workload(baseline, other))

    def test_removed_options_and_invalid_annotations_are_rejected(self):
        for setting in (
            "tls.internal.mode=tcp", "tls.edge.autoReload=true", "tls.internal.autoReload=true",
            "credentials.existingSecret=old", "tls.edge.reloadToken=old", "tls.internal.reloadToken=old",
            "tls.internal.source=certManager", "tls.internal.certManager.issuerRef.name=old",
            "tls.internal.existingSecret=old", "tls.edge.certificateKey=old",
            "tls.internal.gatewayPrivateKeyKey=old", "tls.internal.trustSecret=",
            "tls.internal.gatewaySecret=", "tls.internal.routeTableSecret=",
        ):
            with self.subTest(setting=setting):
                self.assertNotEqual(render(setting).returncode, 0)
        for component in ("gateway", "routeTable"):
            with self.subTest(component=component):
                self.assertNotEqual(render(annotations={component: {"example.com/invalid": True}}).returncode, 0)

    def test_extra_env_cannot_bypass_transport_selection(self):
        for component in ("gateway", "routeTable"):
            for name in ("RELAYGATE_INTERNAL_TRANSPORT", "RELAYGATE_SDK_TRANSPORT", "RELAYGATE_INTERNAL_GATEWAY_KEYS", "RELAYGATE_INSECURE_TEST_TRANSPORT", "RELAYGATE_RT_TRUSTED_LOCAL"):
                with self.subTest(component=component, name=name):
                    result = render(f"{component}.extraEnv[0].name={name}", f"{component}.extraEnv[0].value=blocked")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("cannot override chart-managed variable", result.stderr)


class ImageReferenceTests(unittest.TestCase):
    def test_tags_and_digest_pins_render_for_each_component(self):
        default = render()
        self.assertEqual(default.returncode, 0, default.stderr)
        for component, repository in (
            ("gateway", "relaygate-gateway"),
            ("route-table", "relaygate-route-table"),
        ):
            self.assertRegex(workload(default.stdout, component), rf'image: "ghcr\.io/project-jelly/{repository}:[^"@]+"')

        gateway_digest = "a" * 64
        route_digest = "b" * 64
        pinned = render(
            f"gateway.image.tag=0.5.13@sha256:{gateway_digest}",
            f"routeTable.image.tag=0.5.11@sha256:{route_digest}",
        )
        self.assertEqual(pinned.returncode, 0, pinned.stderr)
        self.assertIn(
            f'image: "ghcr.io/project-jelly/relaygate-gateway:0.5.13@sha256:{gateway_digest}"',
            workload(pinned.stdout, "gateway"),
        )
        self.assertIn(
            f'image: "ghcr.io/project-jelly/relaygate-route-table:0.5.11@sha256:{route_digest}"',
            workload(pinned.stdout, "route-table"),
        )

    def test_rejects_invalid_image_digests(self):
        for component in ("gateway", "routeTable"):
            for suffix in ("@sha256:abcd", "@sha256:" + "A" * 64, "@sha512:" + "a" * 64, "@sha256:" + "g" * 64):
                with self.subTest(component=component, suffix=suffix):
                    result = render(f"{component}.image.tag=1.2.3{suffix}")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(f"{component}.image.tag", result.stderr)


if __name__ == "__main__":
    unittest.main()
