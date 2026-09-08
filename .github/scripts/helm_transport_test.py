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
        for component in ("gateway", "route-table"):
            item = workload(output, component)
            self.assertIn('name: RELAYGATE_INTERNAL_TRANSPORT\n              value: "mtls"', item)
            self.assertIn("name: RELAYGATE_INTERNAL_TLS_CERT_PATH", item)
            self.assertIn("mountPath: /etc/relaygate/tls/internal", item)
            self.assertNotIn("secret.reloader.stakater.com/reload:", item)
            for annotation in ("credentials-reload", "edge-tls-reload", "internal-tls-reload"):
                self.assertNotIn(f"relaygate.io/{annotation}:", item)

    def test_plaintext_keeps_edge_tls_and_token_but_removes_internal_certificates(self):
        output = self.successful("tls.internal.mode=plaintext")
        for component in ("gateway", "route-table"):
            item = workload(output, component)
            self.assertIn('name: RELAYGATE_INTERNAL_TRANSPORT\n              value: "plaintext"', item)
            self.assertNotIn("RELAYGATE_INTERNAL_GATEWAY_KEYS", item)
            self.assertNotIn("RELAYGATE_INTERNAL_TLS_", item)
            self.assertNotIn("name: internal-tls", item)
            self.assertNotIn("RELAYGATE_INSECURE_TEST_TRANSPORT", item)
        self.assertIn("RELAYGATE_SDK_TLS_CERT_PATH", workload(output, "gateway"))
        self.assertIn("RELAYGATE_CLUSTER_TOKEN", workload(output, "gateway"))
        self.assertNotIn("kind: Certificate", output)

    def test_cert_manager_mounts_only_role_leaf_and_public_trust(self):
        output = self.successful(
            "tls.internal.source=certManager",
            "tls.internal.certManager.issuerRef.name=internal-ca",
            "tls.internal.certManager.issuerRef.kind=Issuer",
            "tls.internal.certManager.trustSecret.name=public-trust",
        )
        self.assertEqual(output.count("kind: Certificate\n"), 2)
        for component, leaf in (("gateway", "gw"), ("route-table", "rt")):
            item = workload(output, component)
            self.assertNotIn('secretName: "relaygate-internal-tls"', item)
            self.assertIn('name: "public-trust"', item)
            self.assertIn(f'name: "relaygate-{leaf}-internal-tls"', item)
        self.assertEqual(output.count("rotationPolicy: Always"), 2)

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
            "credentials.reloadToken=old", "tls.edge.reloadToken=old", "tls.internal.reloadToken=old",
        ):
            with self.subTest(setting=setting):
                self.assertNotEqual(render(setting).returncode, 0)
        for component in ("gateway", "routeTable"):
            with self.subTest(component=component):
                self.assertNotEqual(render(annotations={component: {"example.com/invalid": True}}).returncode, 0)
        self.assertNotEqual(render("tls.internal.mode=plaintext", "tls.internal.source=certManager").returncode, 0)

    def test_extra_env_cannot_bypass_transport_selection(self):
        for component in ("gateway", "routeTable"):
            for name in ("RELAYGATE_INTERNAL_TRANSPORT", "RELAYGATE_INTERNAL_GATEWAY_KEYS", "RELAYGATE_INSECURE_TEST_TRANSPORT", "RELAYGATE_RT_TRUSTED_LOCAL"):
                with self.subTest(component=component, name=name):
                    result = render(f"{component}.extraEnv[0].name={name}", f"{component}.extraEnv[0].value=blocked")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("cannot override chart-managed variable", result.stderr)


if __name__ == "__main__":
    unittest.main()
