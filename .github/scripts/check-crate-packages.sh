#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: check-crate-packages.sh [--allow-dirty]

Build RelayGate crate archives locally, inspect normalized manifests, and test
an isolated registry-style consumer through [patch.crates-io]. No crate is
published.

Set RELAYGATE_PACKAGE_ALLOW_DIRTY=1 or pass --allow-dirty for local validation
from an uncommitted worktree. CI should use the clean default.
USAGE
}

allow_dirty="${RELAYGATE_PACKAGE_ALLOW_DIRTY:-}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --allow-dirty)
      allow_dirty=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [ -z "$allow_dirty" ] && [ -n "$(git status --porcelain)" ]; then
  echo "worktree has uncommitted, staged or untracked changes; pass --allow-dirty for local checks" >&2
  exit 1
fi

packages=(
  relaygate-destination
  relaygate-protocol
  relaygate-transport
  relaygate-sdk
  relaygate-token-issuer
)
root="$(git rev-parse --show-toplevel)"
work="$(mktemp -d "${TMPDIR:-/tmp}/relaygate-package-consumer.XXXXXX")"
cleanup() {
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

consumer="$work/consumer"
extracted="$work/extracted"
cargo_target="$work/cargo-target"
export CARGO_TARGET_DIR="$cargo_target"

mkdir -p "$consumer" "$extracted"

version="$(
  python3 - <<'PY'
import json
import subprocess

packages = [
    "relaygate-destination",
    "relaygate-protocol",
    "relaygate-transport",
    "relaygate-sdk",
    "relaygate-token-issuer",
]
metadata = json.loads(
    subprocess.check_output(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version=1"],
        text=True,
    )
)
versions = {
    package["name"]: package["version"]
    for package in metadata["packages"]
    if package["name"] in packages
}
missing = [package for package in packages if package not in versions]
if missing:
    raise SystemExit(f"missing package metadata for: {', '.join(missing)}")
unique = sorted(set(versions.values()))
if len(unique) != 1:
    details = ", ".join(f"{name}={versions[name]}" for name in packages)
    raise SystemExit(f"package versions must match: {details}")
print(unique[0])
PY
)"

python3 - "$root/tests/package-consumer/Cargo.toml" "$version" <<'PY'
import sys
import tomllib

manifest_path = sys.argv[1]
expected = sys.argv[2]
packages = [
    "relaygate-destination",
    "relaygate-protocol",
    "relaygate-transport",
    "relaygate-sdk",
    "relaygate-token-issuer",
]
with open(manifest_path, "rb") as manifest:
    dependencies = tomllib.load(manifest)["dependencies"]
bad = [
    f"{package}={dependencies.get(package)!r}"
    for package in packages
    if dependencies.get(package) != f"={expected}"
]
if bad:
    raise SystemExit(
        "tests/package-consumer dependency versions must exactly match "
        f"workspace package version {expected}: {', '.join(bad)}"
    )
PY

doc_args=(doc --no-deps --locked)
for package in "${packages[@]}"; do
  doc_args+=(-p "$package")
done
RUSTDOCFLAGS="-Dwarnings" cargo "${doc_args[@]}"

package_args=(package --locked --no-verify)
if [ -n "$allow_dirty" ]; then
  package_args+=(--allow-dirty)
fi

for package in "${packages[@]}"; do
  source_manifest="$root/crates/$package/Cargo.toml"
  if ! grep -Eq '^publish[[:space:]]*=[[:space:]]*false[[:space:]]*$' "$source_manifest"; then
    echo "$source_manifest must keep publish = false until the final release decision" >&2
    exit 1
  fi
  package_specific_args=()
  case "$package" in
    relaygate-protocol|relaygate-token-issuer)
      package_specific_args+=(--config "patch.crates-io.relaygate-destination.path=\"$root/crates/relaygate-destination\"")
      ;;
    relaygate-sdk)
      package_specific_args+=(--config "patch.crates-io.relaygate-destination.path=\"$root/crates/relaygate-destination\"")
      package_specific_args+=(--config "patch.crates-io.relaygate-protocol.path=\"$root/crates/relaygate-protocol\"")
      package_specific_args+=(--config "patch.crates-io.relaygate-transport.path=\"$root/crates/relaygate-transport\"")
      ;;
  esac
  cargo "${package_args[@]}" "${package_specific_args[@]}" -p "$package"
  archive="$cargo_target/package/${package}-${version}.crate"
  if [ ! -f "$archive" ]; then
    echo "missing package archive for $package $version: $archive" >&2
    exit 1
  fi
  tar -xzf "$archive" -C "$extracted"
  manifest="$extracted/${package}-${version}/Cargo.toml"
  if awk '
    /^\[/ {
      in_dependency_section = ($0 ~ /^\[(target\..*\.)?(dependencies|dev-dependencies|build-dependencies)(\.|])/)
    }
    in_dependency_section && /path[[:space:]]*=/ {
      print FILENAME ":" FNR ":" $0
      found = 1
    }
    END {
      exit found ? 0 : 1
    }
  ' "$manifest"; then
    echo "normalized manifest for $package still contains a path dependency" >&2
    exit 1
  fi
  if [ ! -f "$extracted/${package}-${version}/LICENSE" ]; then
    echo "package archive for $package does not include LICENSE" >&2
    exit 1
  fi
  if [ ! -f "$extracted/${package}-${version}/README.md" ]; then
    echo "package archive for $package does not include README.md" >&2
    exit 1
  fi
  if ! grep -Eq '^publish[[:space:]]*=[[:space:]]*false[[:space:]]*$' "$manifest"; then
    echo "normalized manifest for $package must keep publish = false" >&2
    exit 1
  fi
done

cp -R "$root/tests/package-consumer/." "$consumer/"
{
  echo
  echo "[patch.crates-io]"
  for package in "${packages[@]}"; do
    echo "$package = { path = \"../extracted/${package}-${version}\" }"
  done
} >> "$consumer/Cargo.toml"

cargo generate-lockfile --manifest-path "$consumer/Cargo.toml"
metadata="$work/consumer-metadata.json"
cargo metadata --manifest-path "$consumer/Cargo.toml" --locked --format-version=1 > "$metadata"
python3 - "$metadata" "$extracted" "$version" <<'PY'
import json
import pathlib
import sys

metadata_path = pathlib.Path(sys.argv[1])
extracted = pathlib.Path(sys.argv[2]).resolve()
version = sys.argv[3]
packages = [
    "relaygate-destination",
    "relaygate-protocol",
    "relaygate-transport",
    "relaygate-sdk",
    "relaygate-token-issuer",
]
metadata = json.loads(metadata_path.read_text())
by_name = {package["name"]: package for package in metadata["packages"]}
for name in packages:
    package = by_name.get(name)
    if package is None:
        raise SystemExit(f"consumer metadata is missing {name}")
    if package["version"] != version:
        raise SystemExit(f"{name} resolved version {package['version']}, expected {version}")
    if package.get("source") is not None:
        raise SystemExit(f"{name} resolved from registry source {package['source']!r}")
    expected_manifest = (extracted / f"{name}-{version}" / "Cargo.toml").resolve()
    actual_manifest = pathlib.Path(package["manifest_path"]).resolve()
    if actual_manifest != expected_manifest:
        raise SystemExit(
            f"{name} resolved manifest {actual_manifest}, expected extracted package {expected_manifest}"
        )
PY
cargo test --manifest-path "$consumer/Cargo.toml" --locked
cargo doc --manifest-path "$consumer/Cargo.toml" --locked --no-deps
