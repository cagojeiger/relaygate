import unittest

from change_scope import classify


class ClassifyTests(unittest.TestCase):
    def test_test_only_change_is_not_runtime(self):
        paths = [
            "crates/relaygate-server/tests/process.rs",
            "crates/relaygate-server/tests/process/admission.rs",
            "crates/relaygate-gateway/src/state/delivery_tests.rs",
            "crates/relaygate-gateway/src/state/tests.rs",
            "crates/relaygate-gateway/src/state/control_admission_tests/budget.rs",
            "crates/relaygate-gateway-peer/src/runtime_tests/duplicate_cleanup.rs",
            "crates/relaygate-gateway-peer/src/liveness_runtime_tests/observation.rs",
            "docs/test/001-executable-coverage.toml",
            "README.md",
        ]
        self.assertEqual(classify(paths), (False, False))

    def test_gateway_source_is_runtime_but_not_published(self):
        paths = ["crates/relaygate-gateway/src/state.rs"]
        self.assertEqual(classify(paths), (True, False))

    def test_sdk_source_is_runtime_and_published(self):
        paths = ["crates/relaygate-sdk/src/listener.rs"]
        self.assertEqual(classify(paths), (True, True))

    def test_published_crate_test_is_published_but_not_runtime(self):
        paths = ["crates/relaygate-sdk/src/pipe/tests.rs"]
        self.assertEqual(classify(paths), (False, True))

    def test_manifest_and_packaging_inputs_are_published(self):
        for path in (
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            "LICENSE",
            "tests/package-consumer/Cargo.toml",
            ".github/scripts/check-crate-packages.sh",
        ):
            with self.subTest(path=path):
                self.assertTrue(classify([path])[1])

    def test_chart_docker_and_workflow_changes_are_runtime(self):
        for path in (
            "deploy/helm/relaygate/values.yaml",
            "deploy/docker/Dockerfile",
            ".github/workflows/ci.yml",
            "monitoring/grafana/dashboards/gateway.json",
            "tests/kind/run.sh",
            "examples/echo-probe/src/main.rs",
        ):
            with self.subTest(path=path):
                self.assertTrue(classify([path])[0])

    def test_empty_change_is_neither(self):
        self.assertEqual(classify([]), (False, False))


if __name__ == "__main__":
    unittest.main()
