# Releasing RelayGate

RelayGate has three release units that share one version:

1. The root server and OCI image
2. The nested Go module at `sdk/go`
3. The Rust crate at `sdk/rust/relaygate-sdk`

Examples are repository-local evidence and are not released independently.

## Prerequisites

- The release change is ready to merge into `main` and all CI jobs pass.
- `VERSION`, `sdk/rust/relaygate-sdk/Cargo.toml`, and `CHANGELOG.md` contain the same release version.
- The Actions secret `CARGO_REGISTRY_TOKEN` can publish `relaygate-sdk` to crates.io.
- Existing root or Go SDK tags, if any, point to the exact release commit.

## Releasing

For `0.1.0`, merge a release change that sets the version files together:

```text
VERSION                                      0.1.0
sdk/rust/relaygate-sdk/Cargo.toml            version = "0.1.0"
CHANGELOG.md                                 ## 0.1.0 - YYYY-MM-DD
```

The `VERSION` change on `main` starts the release workflow. It publishes:

- `ghcr.io/cagojeiger/relaygate:0.1.0`
- `ghcr.io/cagojeiger/relaygate:latest`
- `relaygate-sdk` version `0.1.0` on crates.io
- `v0.1.0` on the release commit
- `sdk/go/v0.1.0` on the same release commit
- A GitHub Release generated from the tag

The workflow creates both Git tags only after the server image and Rust crate succeed. A rerun accepts an existing tag only when it still resolves to the exact release commit. After the release, best-effort GHCR retention keeps the newest 20 stable SemVer image versions; `latest` remains an alias of the newest one.

## Verification

```bash
gh release view v0.1.0
docker pull ghcr.io/cagojeiger/relaygate:0.1.0
go list -m github.com/cagojeiger/relaygate/sdk/go@v0.1.0
cargo info relaygate-sdk@0.1.0
```

Do not move an existing release tag. Correct a released version with a new patch version.
