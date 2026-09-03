import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

from helm_release import CHART, check_version, package_contents, require_bump, stage_package, version


class VersionTests(unittest.TestCase):
    def test_stable_version(self):
        self.assertEqual(version("name: relaygate\nversion: 0.1.0\n"), "0.1.0")

    def test_invalid_version(self):
        for value in ('"0.1.0"', "01.1.0", "1.0", "1.0.0-rc.1", "../x", "1.0.0+build"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                version(f"version: {value}\n")
        with self.assertRaises(ValueError):
            version("version: 1.0.0\nversion: 2.0.0\n")

    def test_version_bump_policy(self):
        require_bump("0.1.0", "0.1.0", False)
        require_bump("0.9.0", "0.10.0", True)
        for old, new, changed in (("0.1.0", "0.1.0", True), ("0.2.0", "0.1.0", True),
                                  ("0.2.0", "0.1.0", False)):
            with self.subTest(old=old, new=new, changed=changed), self.assertRaises(ValueError):
                require_bump(old, new, changed)


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.site = self.root / "site"
        self.candidate = self.root / "relaygate-0.1.0.tgz"

    def package(self, payload=b"one", mtime=1, extra=None):
        files = {"relaygate/Chart.yaml": b"name: relaygate\nversion: 0.1.0\n",
                 "relaygate/values.yaml": payload}
        files.update(extra or {})
        with tarfile.open(self.candidate, "w:gz") as archive:
            for name, data in files.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                entry.mtime = mtime
                entry.mode = 0o644
                archive.addfile(entry, io.BytesIO(data))

    def test_retry_retains_original_bytes_and_checksum(self):
        self.package()
        digest = stage_package(self.candidate, self.site)
        original = (self.site / self.candidate.name).read_bytes()
        self.package(mtime=999)
        self.assertNotEqual(original, self.candidate.read_bytes())
        self.assertEqual(stage_package(self.candidate, self.site), digest)
        self.assertEqual((self.site / self.candidate.name).read_bytes(), original)

    def test_same_version_changed_content_is_rejected(self):
        self.package()
        stage_package(self.candidate, self.site)
        original = (self.site / self.candidate.name).read_bytes()
        self.package(payload=b"two")
        with self.assertRaisesRegex(ValueError, "immutable"):
            stage_package(self.candidate, self.site)
        self.assertEqual((self.site / self.candidate.name).read_bytes(), original)

    def test_corrupt_checksum_is_rejected(self):
        self.package()
        stage_package(self.candidate, self.site)
        (self.site / f"{self.candidate.name}.sha256").write_text("corrupt\n")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            stage_package(self.candidate, self.site)

    def test_filename_must_match_version(self):
        self.candidate = self.root / "relaygate-9.0.0.tgz"
        self.package()
        with self.assertRaisesRegex(ValueError, "filename"):
            stage_package(self.candidate, self.site)

    def test_retry_repairs_missing_checksum(self):
        self.package()
        digest = stage_package(self.candidate, self.site)
        (self.site / f"{self.candidate.name}.sha256").unlink()
        self.assertEqual(stage_package(self.candidate, self.site), digest)

    def test_archive_cannot_escape_chart(self):
        self.package(extra={"relaygate/../outside": b"no"})
        with self.assertRaisesRegex(ValueError, "archive path"):
            package_contents(self.candidate)


class GitVersionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        previous_directory = Path.cwd()
        self.addCleanup(os.chdir, previous_directory)
        os.chdir(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.name", "test")
        self.git("config", "user.email", "test@example.invalid")
        self.git("commit", "--allow-empty", "-qm", "initial")
        self.initial = self.git("rev-parse", "HEAD")
        Path(CHART).mkdir(parents=True)
        self.chart = Path(CHART, "Chart.yaml")
        self.chart.write_text("version: 0.1.0\n")
        self.commit()
        self.base = self.git("rev-parse", "HEAD")

    def git(self, *args):
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")

    def test_initial_chart_and_unchanged_chart(self):
        self.assertEqual(check_version(self.initial), "0.1.0")
        Path("README.md").write_text("outside chart\n")
        self.commit()
        self.assertEqual(check_version(self.base), "0.1.0")

    def test_chart_change_requires_bump(self):
        Path(CHART, "values.yaml").write_text("changed: true\n")
        self.commit()
        with self.assertRaises(ValueError):
            check_version(self.base)
        self.chart.write_text("version: 0.1.1\n")
        self.commit()
        self.assertEqual(check_version(self.base), "0.1.1")

    def test_invalid_base_is_not_initial_release(self):
        with self.assertRaises(subprocess.CalledProcessError):
            check_version("missing-ref")


if __name__ == "__main__":
    unittest.main()
