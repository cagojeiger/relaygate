"""Internal transport and certificate reload rendering contract (run after Helm setup)."""

from pathlib import Path
import subprocess
import unittest


CHART = Path(__file__).resolve().parents[2] / "deploy/helm/relaygate"


def render(*settings):
    command = ["helm", "template", "relaygate", str(CHART), "--namespace", "relaygate"]
    for setting in settings:
        command.extend(["--set", setting])
    return subprocess.run(command, capture_output=True, text=True, check=False)


def workload(output, component):
    marker = f"# Source: relaygate/templates/{component}-statefulset.yaml"
    return output.split(marker, 1)[1].split("\n---", 1)[0]


class InternalTransportTests(unittest.TestCase):
    def successful(self, *settings):
        result = render(*settings)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_default_requires_tls_and_has_no_automatic_restart_opt_in(self):
        output = self.successful()
        for component in ("gateway", "route-table"):
            item = workload(output, component)
            self.assertIn('name: RELAYGATE_INTERNAL_TRANSPORT\n              value: "mtls"', item)
            self.assertIn("name: RELAYGATE_INTERNAL_TLS_CERT_PATH", item)
            self.assertIn("mountPath: /etc/relaygate/tls/internal", item)
            self.assertNotIn("secret.reloader.stakater.com/reload:", item)

    def test_plaintext_keeps_edge_tls_and_keys_but_removes_internal_certificates(self):
        output = self.successful("tls.internal.mode=plaintext")
        for component in ("gateway", "route-table"):
            item = workload(output, component)
            self.assertIn('name: RELAYGATE_INTERNAL_TRANSPORT\n              value: "plaintext"', item)
            self.assertIn("name: RELAYGATE_INTERNAL_GATEWAY_KEYS", item)
            self.assertNotIn("RELAYGATE_INTERNAL_TLS_", item)
            self.assertNotIn("name: internal-tls", item)
            self.assertNotIn("RELAYGATE_INSECURE_TEST_TRANSPORT", item)
        self.assertIn("RELAYGATE_SDK_TLS_CERT_PATH", workload(output, "gateway"))
        self.assertIn("RELAYGATE_CLUSTER_TOKEN", workload(output, "gateway"))
        self.assertNotIn("kind: Certificate", output)

    def test_cert_manager_reload_watches_only_role_leaf_and_public_trust(self):
        output = self.successful(
            "tls.internal.source=certManager", "tls.internal.autoReload=true",
            "tls.internal.certManager.issuerRef.name=internal-ca",
            "tls.internal.certManager.issuerRef.kind=Issuer",
            "tls.internal.certManager.trustSecret.name=public-trust",
        )
        self.assertEqual(output.count("kind: Certificate\n"), 2)
        for component, leaf in (("gateway", "gw"), ("route-table", "rt")):
            item = workload(output, component)
            metadata = item.split("\nspec:", 1)[0]
            self.assertIn(
                f'secret.reloader.stakater.com/reload: "public-trust,relaygate-{leaf}-internal-tls"',
                metadata,
            )
            self.assertNotIn("secretName: \"relaygate-internal-tls\"", item)
            self.assertIn('name: "public-trust"', item)
        self.assertEqual(output.count("rotationPolicy: Always"), 2)

    def test_existing_secret_reload_and_invalid_combinations(self):
        output = self.successful("tls.internal.autoReload=true")
        self.assertEqual(output.count('secret.reloader.stakater.com/reload: "relaygate-internal-tls"'), 2)
        for settings in (
            ("tls.internal.mode=tcp",),
            ("tls.internal.mode=plaintext", "tls.internal.autoReload=true"),
            ("tls.internal.mode=plaintext", "tls.internal.source=certManager"),
        ):
            with self.subTest(settings=settings):
                self.assertNotEqual(render(*settings).returncode, 0)

    def test_extra_env_cannot_bypass_transport_selection(self):
        for component in ("gateway", "routeTable"):
            for name in ("RELAYGATE_INTERNAL_TRANSPORT", "RELAYGATE_INSECURE_TEST_TRANSPORT", "RELAYGATE_RT_TRUSTED_LOCAL"):
                with self.subTest(component=component, name=name):
                    result = render(f"{component}.extraEnv[0].name={name}", f"{component}.extraEnv[0].value=blocked")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("cannot override chart-managed variable", result.stderr)


if __name__ == "__main__":
    unittest.main()
