# RelayGate 릴리스

RelayGate에는 같은 version을 공유하는 release unit 세 개가 있다.

1. Root server와 OCI image
2. `sdk/go`의 nested Go module
3. `sdk/rust/relaygate-sdk`의 Rust crate

Example은 repository-local evidence이며 독립 release하지 않는다.

## 사전 조건

- 릴리스 commit이 `main`에 병합되고 모든 CI 작업이 통과해야 한다.
- `sdk/rust/relaygate-sdk/Cargo.toml`과 `CHANGELOG.md`에 release version이 있어야 한다.
- Actions 비밀 값 `CARGO_REGISTRY_TOKEN`이 crates.io에 `relaygate-sdk`를 게시할 수 있어야 한다.
- 최상위 릴리스 태그가 존재하지 않아야 한다.

## 태그 생성

`0.1.0`은 `main` release commit에 annotated root tag 하나를 만들어 push한다.

```bash
git switch main
git pull --ff-only origin main
git tag -a v0.1.0 -m "RelayGate v0.1.0"
git push origin v0.1.0
```

Root tag는 다음을 publish한다.

- `ghcr.io/cagojeiger/relaygate:v0.1.0`
- `ghcr.io/cagojeiger/relaygate:latest`
- crates.io의 `relaygate-sdk` version `0.1.0`
- 같은 release commit의 `sdk/go/v0.1.0`
- Tag에서 생성한 GitHub Release

릴리스 작업 흐름은 서버 이미지와 Rust crate가 성공한 뒤에만 하위 Go 모듈 태그를 생성한다. 재실행은 해당 태그가 exact 릴리스 commit을 가리킬 때만 허용한다.

## 검증

```bash
gh release view v0.1.0
docker pull ghcr.io/cagojeiger/relaygate:v0.1.0
go list -m github.com/cagojeiger/relaygate/sdk/go@v0.1.0
cargo info relaygate-sdk@0.1.0
```

기존 릴리스 태그를 이동하지 않는다. 이미 릴리스한 버전의 수정은 새 patch 버전으로 배포한다.
