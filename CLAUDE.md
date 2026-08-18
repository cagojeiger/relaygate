# CLAUDE.md

## 작업 원칙

- 변경 전 관련 코드와 ADR/SPEC을 읽고 가정을 밝힌다.
- 현재 요구에 필요한 최소 구현을 선택하고 speculative abstraction을 만들지 않는다.
- 기존 worktree 변경을 보존하고, 수정 범위를 벗어난 파일을 정리하지 않는다.
- 의미가 달라지는 해석이 여럿이면 임의로 결정하지 않는다.

## 프로젝트 경계

- Production Controller와 Gateway runtime은 Go가 소유한다.
- 하나의 server binary/image는 `controller`와 `gateway` 두 role을 제공한다.
- `controller`는 persistent Raft voter, current-state FSM, control gRPC와 admin만 소유한다.
- `gateway`는 public/peer Relay와 control client를 소유하며 Raft/store를 열지 않는다.
- Public Go/Rust SDK는 하나의 protobuf contract를 공유하며 server/Raft type을 노출하지 않는다.
- Go SDK는 `sdk/go` 독립 module이고 root workspace 없이 build/test되어야 한다.
- Relay는 gRPC, Raft transport는 내부 protocol, REST는 read-only observation surface다.
- `ClientId`는 인증으로 정해지는 strict namespace다. Client/API key는 external client config만 관리한다.

## 문서

- 장기 설계 결정은 `docs/adr/`, 상태와 동작 계약은 `docs/spec/`, 검증 계획은 `docs/test/`에 둔다.
- State/event 의미를 바꾸면 `SPEC 004`의 canonical table과 `TEST 001`의 대응 test를 함께 갱신한다.
- Accepted ADR의 의미를 바꿀 때는 기존 문장을 조용히 고치지 말고 새 결정을 기록한다.

## 검증

Go runtime 변경:

```text
gofmt -w <touched .go files>
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

Go SDK 변경:

```text
cd sdk/go
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

Rust SDK 변경:

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
