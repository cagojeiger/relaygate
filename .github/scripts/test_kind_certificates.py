from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
HARNESS = r'''
set -euo pipefail
source "$1/tests/kind/certificates.sh"
export ARTIFACTS=$2 CERTIFICATES=$2
sequence=$3
printf '0' >"$ARTIFACTS/calls"
timeout() {
  [[ "$1" == 3 && "$2" == openssl && "$3" == s_client ]]
  [[ "$*" == *"-verify_return_error"* && "$*" == *"-CAfile"* ]]
  [[ "$*" == *"-servername relaygate-gateway.internal"* ]]
  [[ "$*" == *"-alpn relaygate/2"* ]]
  local count outcome
  count=$(<"$ARTIFACTS/calls")
  count=$((count + 1))
  printf '%s' "$count" >"$ARTIFACTS/calls"
  outcome=${sequence:$((count - 1)):1}
  case "$outcome" in
    n) printf 'serial=NEW\n' ;;
    o) printf 'serial=OLD\n' ;;
    v) printf 'serial=NEW\n'; echo 'certificate verify failed' >&2; return 1 ;;
    *) echo 'connection failed' >&2; return 124 ;;
  esac
}
openssl() {
  [[ "$*" == 'x509 -noout -serial' ]]
  local certificate
  IFS= read -r certificate || return 1
  printf '%s\n' "$certificate"
}
sleep() { printf 'sleep\n' >>"$ARTIFACTS/sleeps"; }
wait_for_served_certificate 127.0.0.1:28420 serial=NEW 3
'''


class ServedCertificateTests(unittest.TestCase):
    def check_sequence(self, sequence, expected_calls, success):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["bash", "-c", HARNESS, "test", str(ROOT), directory, sequence],
                text=True, capture_output=True, timeout=5,
            )
            artifacts = Path(directory)
            self.assertEqual(result.returncode == 0, success, result.stderr)
            self.assertEqual(int((artifacts / "calls").read_text()), expected_calls)
            sleeps = artifacts / "sleeps"
            self.assertEqual(
                len(sleeps.read_text().splitlines()) if sleeps.exists() else 0,
                expected_calls - 1,
            )
            served = artifacts / "certificate-edge-served.txt"
            if success:
                self.assertEqual(served.read_text(), "127.0.0.1:28420 serial=NEW\n")
            else:
                self.assertFalse(served.exists())
                self.assertIn("did not serve the expected verified", result.stderr)
                self.assertIn("address=127.0.0.1:28420 attempt=3", result.stderr)
            return result.stderr

    def test_immediate_expected_certificate(self):
        self.check_sequence("n", 1, True)

    def test_transient_failure_and_old_serial_converge(self):
        self.check_sequence("fon", 3, True)

    def test_persistent_connection_failure_is_bounded(self):
        self.assertIn("connection failed", self.check_sequence("fff", 3, False))

    def test_persistent_old_serial_fails(self):
        self.assertIn("observed=serial=OLD", self.check_sequence("ooo", 3, False))

    def test_matching_serial_does_not_hide_tls_validation_failure(self):
        self.assertIn("certificate verify failed", self.check_sequence("vvv", 3, False))

    def test_failure_after_old_serial_does_not_reuse_old_output(self):
        self.check_sequence("ofn", 3, True)
