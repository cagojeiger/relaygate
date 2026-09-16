#!/usr/bin/env python3
"""Classify the files a change touches so CI can skip work that cannot be affected.

Prints GitHub Actions outputs:

  runtime=true|false    any file outside tests/docs changed, so the built
                        binaries, images or charts may differ
  published=true|false  any published crate, workspace manifest or packaging
                        input changed, so crate package archives must be
                        re-verified

Unknown base (first push, empty or zero sha) reports true for both.
"""

import argparse
import fnmatch
import subprocess
import sys

ZERO_SHA = "0" * 40

# Files that can only change test or documentation behaviour.
# Matched with fnmatch, where `*` also crosses `/` (unlike a shell glob), so
# `docs/*` covers every depth under docs/. Do not port these to shell globs.
NON_RUNTIME_PATTERNS = (
    "docs/*",
    "*.md",
    "crates/*/tests/*",
    "crates/*/src/*/tests/*",
    "crates/*/src/*_tests.rs",
    "crates/*/src/*/*_tests.rs",
    "crates/*/src/*_tests/*",
    "crates/*/src/*/*_tests/*",
    "crates/*/src/tests.rs",
    "crates/*/src/*/tests.rs",
    "tests/package-consumer/*",
)

# Inputs to `cargo package` for the published crates.
PUBLISHED_PATTERNS = (
    "crates/relaygate-destination/*",
    "crates/relaygate-protocol/*",
    "crates/relaygate-transport/*",
    "crates/relaygate-sdk/*",
    "crates/relaygate-token-issuer/*",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    # Every published crate sets license-file.workspace, which resolves to
    # the root LICENSE, so it is a `cargo package` input.
    "LICENSE",
    "tests/package-consumer/*",
    ".github/scripts/check-crate-packages.sh",
    ".github/workflows/ci.yml",
)


def matches(path, patterns):
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def classify(paths):
    """Return (runtime, published) for the changed paths."""
    runtime = any(not matches(path, NON_RUNTIME_PATTERNS) for path in paths)
    published = any(matches(path, PUBLISHED_PATTERNS) for path in paths)
    return runtime, published


def changed_paths(base):
    if not base or base == ZERO_SHA:
        return None
    try:
        output = subprocess.check_output(
            ["git", "diff", "--name-only", f"{base}...HEAD"], text=True
        )
    except subprocess.CalledProcessError:
        return None
    return [line for line in output.splitlines() if line]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="")
    args = parser.parse_args()
    paths = changed_paths(args.base)
    if paths is None:
        runtime, published = True, True
    else:
        runtime, published = classify(paths)
    sys.stdout.write(f"runtime={str(runtime).lower()}\n")
    sys.stdout.write(f"published={str(published).lower()}\n")


if __name__ == "__main__":
    main()
