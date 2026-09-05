"""Helm release version and immutable package checks; no network access."""

import argparse
import hashlib
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile

CHART = "deploy/helm/relaygate"


def version(document):
    matches = re.findall(r"^version: ([0-9]+\.[0-9]+\.[0-9]+)$", document, re.MULTILINE)
    if len(matches) != 1 or any(str(int(n)) != n for n in matches[0].split(".")):
        raise ValueError("Chart.yaml version must be an unquoted stable X.Y.Z without leading zeros")
    return matches[0]


def require_non_decreasing(previous, current):
    old = tuple(map(int, previous.split(".")))
    new = tuple(map(int, current.split(".")))
    if new < old:
        raise ValueError("Chart.yaml version must not decrease")


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def check_version(base):
    current = version(Path(CHART, "Chart.yaml").read_text())
    if base and set(base) != {"0"}:
        # An invalid ref fails; only an absent chart permits the initial version.
        if git("ls-tree", base, "--", CHART):
            previous = version(git("show", f"{base}:{CHART}/Chart.yaml"))
            require_non_decreasing(previous, current)
    return current


def package_contents(path):
    files = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            parts = PurePosixPath(member.name).parts
            if not parts or parts[0] != "relaygate" or ".." in parts:
                raise ValueError("unexpected chart archive path")
            if member.isdir():
                continue
            if not member.isfile() or member.name in files:
                raise ValueError("chart archive must contain unique regular files")
            files[member.name] = (member.mode, archive.extractfile(member).read())
    if "relaygate/Chart.yaml" not in files:
        raise ValueError("chart archive is missing Chart.yaml")
    return files


def stage_package(candidate, site):
    contents = package_contents(candidate)
    chart_version = version(contents["relaygate/Chart.yaml"][1].decode())
    if candidate.name != f"relaygate-{chart_version}.tgz":
        raise ValueError("package filename does not match Chart.yaml")
    site.mkdir(parents=True, exist_ok=True)
    destination = site / candidate.name
    checksum = site / f"{candidate.name}.sha256"
    if destination.exists():
        # Helm archives carry timestamps; compare contents but retain original bytes.
        if package_contents(destination) != contents:
            raise ValueError("published chart version is immutable; bump Chart.yaml version")
    else:
        if checksum.exists():
            raise ValueError("checksum exists without its chart package")
        shutil.copyfile(candidate, destination)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    expected = f"{digest}  {destination.name}\n"
    if checksum.exists() and checksum.read_text() != expected:
        raise ValueError("published chart checksum mismatch")
    checksum.write_text(expected)
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check-version")
    check.add_argument("--base")
    stage = commands.add_parser("stage")
    stage.add_argument("candidate", type=Path)
    stage.add_argument("site", type=Path)
    args = parser.parse_args()
    if args.command == "check-version":
        print(check_version(args.base))
    else:
        print(stage_package(args.candidate, args.site))


if __name__ == "__main__":
    main()
