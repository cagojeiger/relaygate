# Releasing RelayGate

RelayGate has three release units that share one version:

1. The root server and OCI image
2. The nested Go module at `sdk/go`
3. The Rust crate at `sdk/rust/relaygate-sdk`

Examples are repository-local evidence and are not released independently.

## Prerequisites

- The release commit is merged into `main` and all CI jobs pass.
- `sdk/rust/relaygate-sdk/Cargo.toml` and `CHANGELOG.md` contain the release version.
- The Actions secret `CARGO_REGISTRY_TOKEN` can publish `relaygate-sdk` to crates.io.
- The root release tag does not already exist.

## Tagging

For `0.1.0`, create and push one annotated root tag on the `main` release commit:

```bash
git switch main
git pull --ff-only origin main
git tag -a v0.1.0 -m "RelayGate v0.1.0"
git push origin v0.1.0
```

The root tag publishes:

- `ghcr.io/cagojeiger/relaygate:v0.1.0`
- `ghcr.io/cagojeiger/relaygate:latest`
- `relaygate-sdk` version `0.1.0` on crates.io
- `sdk/go/v0.1.0` on the same release commit
- A GitHub Release generated from the tag

The release workflow creates the nested Go module tag only after the server image and Rust crate succeed. A rerun accepts that tag only when it still resolves to the exact release commit.

## Verification

```bash
gh release view v0.1.0
docker pull ghcr.io/cagojeiger/relaygate:v0.1.0
go list -m github.com/cagojeiger/relaygate/sdk/go@v0.1.0
cargo info relaygate-sdk@0.1.0
```

Do not move an existing release tag. Correct a released version with a new patch version.
