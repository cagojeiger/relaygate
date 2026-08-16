# RelayGate

RelayGate는 Raft로 조정되는 Gateway cluster에서 일시적인 양방향 Pipe를 연결하고 중계한다.
연결과 payload는 복구하지 않는다.

## 구성

| 구성 | 책임 |
| --- | --- |
| Go Gateway runtime | Process lifecycle, config, gRPC/REST, Gateway relay, Raft integration |
| Go SDK / Rust SDK | 각 언어에 자연스러운 public API |
| protobuf | 두 SDK가 공유하는 wire contract |

- Relay data path는 protobuf gRPC를 사용한다.
- Raft transport는 server 내부 protocol이다.
- REST는 read-only 운영 관찰용이며 relay나 client/key CRUD를 제공하지 않는다.
- 인증된 연결은 하나의 `ClientId`에 격리되고, client와 API key는 external client config가 관리한다.

설계 원칙은 `docs/adr/`, 상태·동작 계약은 `docs/spec/`, 필수 검증 계획은 `docs/test/`에 있다.
장애와 복구 경계는 [SPEC 003](docs/spec/003-failure-and-recovery-model.md), 닫힌 상태 전이표는
[SPEC 004](docs/spec/004-state-transition-model.md), v0 필수 테스트는
[TEST 001](docs/test/001-core-correctness-test-plan.md)에서 정의한다. Cross-Gateway owner hop의 장기 결정은
[ADR 008](docs/adr/008-cross-gateway-hop-and-replay.md)이 소유한다.

## 현재 구현

현재 Go runtime은 다음 경계까지 구현한다.

- HashiCorp Raft의 단일 voter 또는 정적 3-voter bootstrap
- BoltDB log/stable store와 file snapshot
- `GatewaySlot`과 `BindingSlot` generation/ref CAS, tombstone과 distinct-key 상한
- Current leader의 term/quorum 확인으로 생성하는 `AuthorityId`, exact Gateway generation fencing과 timeout classification
- 내부 gRPC control stream: `Hello → authoritative current-instance bindings → exact FullSnapshot → serial BindingMutation`
- 각 process의 독립적인 `GatewayId`/`GatewayInstanceId`, endpoint 순회와 leader 변경 뒤 자동 full-snapshot 재검증
- External client config의 SHA-256 verifier, `SIGHUP` atomic reload와 메모리 `ClientSession`
- Public `Relay.Connect`의 first-message 인증 deadline, bounded client session과 authenticated `BindListener/UnbindListener`
- 별도 local binding runtime의 `Registering/Live/Retiring/Retired`, session ownership과 process-wide capacity
- Install 응답 유실 뒤 authoritative reconcile + exact CAS replay, unbind/session/key 제거의 즉시 local ineligibility
- In-flight correlation 전용 `request_id`, exact literal `endpoint`와 required `target_id`를 쓰는 same/cross-Gateway `Open`
- `A ∧ L ∧ Q ∧ C ∧ V ∧ O` attempt admission, owner O reservation과 Listener accept Open LP
- `Hello`의 internal relay address를 current authority session memory에만 둔 remote-owner discovery
- 별도 internal gRPC bind/advertise listener, exact serialized context/absolute expiry와 bounded replay fence
- Process-wide bounded async Open, duplicate in-flight rejection, `CancelOpen`과 exact caller-owned `ClosePipe`
- Bounded attempt/Pipe/terminal table, caller `Opened/Failed/Unknown`과 session/reload terminal 전파
- Same/cross-Gateway Pipe의 60 KiB 이하 opaque payload frame, activation, 방향별 FIFO와 bounded backpressure
- JSON structured log, quorum-confirmed read-only `/status`, local health/readiness와 Prometheus metrics
- Snapshot 뒤 process restart에서 control state 복구

Cross-Gateway owner forwarding은 아래 승인 계약과 [TEST 001의 H01–H12](docs/test/001-core-correctness-test-plan.md)를
따른다. Unit/integration test와 isolated 3-node H12 smoke가 pass했고 CI에 같은 workflow가 있다. Arbitrary fault
matrix, peer auth/mTLS와 전체 H gate는 아직 증명하지 않는다. Wildcard/priority, target 생략 선택, `OpenAll`,
동적 multi-node join과 public Go/Rust SDK도 별도 evidence가 필요하다.

## 승인된 Cross-Gateway 계약

```text
Caller ─ public Relay ─ Ingress ═ dedicated internal bidi stream ═ Owner ─ public Relay ─ Listener
                              one volatile hop / logical remote Pipe
```

- Owner Gateway는 `Hello`에 internal relay advertise address를 싣는다. Current authority는 이를 exact current
  control-session memory에만 두고 Raft, snapshot, database나 directory에 저장하지 않는다.
- Internal relay는 public Relay, control gRPC와 Raft transport에서 분리된 bind/advertise listener다. Remote
  attempt는 dedicated bidi stream 하나를 쓰고 Accepted 뒤 같은 stream이 그 Pipe의 유일한 Gateway hop이다.
- Forwarded context는 epoch/authority provenance, ingress Gateway/instance/control session, exact auth/binding/owner
  address와 absolute `expires_at`을 묶는다. `ClockSkewBound < relay.open_timeout`을 증명할 수 있어야 한다.
- Owner는 exact `O`에서 local attempt reservation과 successful `AttemptId` cache insert를 원자화한 뒤에만
  Listener에게 offer한다. Entry는 Listener reject 뒤에도 expiry까지 남는다. Listener accept를 `AcceptedO`로
  기록할 때 `PipeId`를 만들고 Open이 선형화된다. 이 retention은 Owner process lifetime 안의 volatile guarantee다.
- O reservation 뒤 hop retry/reconnect, Pipe resume/attach와 payload replay는 없다. O guard failure는 consume하지
  않아 expiry 전 재평가할 수 있다. Open LP 미통과가 증명될 때만 stable failure이고 통과 가능 또는 이후
  response/hop loss는 `Unknown`이다. Ingress/Owner segment는 각각 volatile하다.

현재 Raft transport와 internal control gRPC, 그리고 internal owner relay를 활성화하는 초기 slice는 TLS나 peer
authentication 없이 **신뢰된 local/dev network만을 전제**한다. 기준 config는 모든 listener를 loopback에 bind한다. Compose만 container network
통신을 위해 bind address를 명시적으로 `0.0.0.0`으로 override하고 host port는 loopback에만 publish한다.
이 상태로 shared 또는 untrusted network에 배포하지 않으며, 운영 배포 전 transport trust boundary와
Gateway identity 인증을 별도로 확정한다. Bearer key를 받는 public Relay listener는 TLS가 구현될 때까지
non-loopback bind를 거부한다.

Forwarded context의 ingress/owner field는 provenance를 구조적으로 묶지만 plaintext Owner가 actual stream peer,
current ingress control session 또는 자기 advertised address의 currentness를 검증하는 cryptographic proof는 아니다.
Ingress만 authority response의 ingress tuple과 자기 live session 일치를 send 전에 확인한다.

```bash
go run ./cmd/relaygate -config ./configs/relaygate.yaml
curl http://127.0.0.1:9090/healthz/ready
curl http://127.0.0.1:9090/status
```

설정은 `configs/relaygate.yaml` 하나가 기준이다. 공통 정책은 YAML에 두고, 배포마다 달라지는
`RELAYGATE_RAFT_NODE_ID`, `RELAYGATE_RAFT_BIND_ADDRESS`,
`RELAYGATE_RAFT_ADVERTISE_ADDRESS`, `RELAYGATE_RAFT_DATA_DIR`,
`RELAYGATE_RAFT_BOOTSTRAP`, `RELAYGATE_RAFT_BOOTSTRAP_VOTERS`,
`RELAYGATE_CONTROL_CLUSTER_EPOCH`, `RELAYGATE_CONTROL_BIND_ADDRESS`,
`RELAYGATE_INTERNAL_RELAY_BIND_ADDRESS`, `RELAYGATE_INTERNAL_RELAY_ADVERTISE_ADDRESS`,
`RELAYGATE_GATEWAY_ID`, `RELAYGATE_GATEWAY_CONTROL_ENDPOINTS`,
`RELAYGATE_ADMIN_BIND_ADDRESS`를 환경변수로 덮어쓴다.
Bootstrap voter와 control endpoint 목록은 JSON 배열이며 `Raft NodeId`와 `GatewayId`는 같은 값으로
간주하지 않는다.

같은 canonical YAML의 `clients`가 external credential source다. Raw key는 저장하지 않고
`sha256:<digest>`만 둔다. 기본 local-development verifier의 test key는
`relaygate-local-development-key`이며 production에서 반드시 교체한다. `SIGHUP`은 전체 config를 검증한 뒤
clients만 atomic하게 교체하고, 제거된 credential의 local session을 종료한다.
`relay.authentication_timeout`은 인증을 보내지 않는 stream을 종료하고,
`relay.max_client_sessions`는 process 전체의 active session 수를 제한한다. 같은 값은 connection별 gRPC
동시 stream 상한에도 사용하지만 전역 상한의 source of truth는 session manager다.
`relay.max_listener_bindings`는 process 전체의 Registering/Live/cleanup 대기 listener 정의 수를 원자적으로
제한하며 범위는 1–512다. 이는 connection 수가 아니다. 초과 시 기존 binding을 evict하지 않고 새 Bind만 거부한다.
`relay.open_timeout`은 admission·offer, Listener confirmation, payload queue backpressure와 terminal control
delivery의 각 bounded wait를 제한하고 Forwarded OpenContext의 validity를 정한다. Cross-Gateway 배포는 authority와
Gateway wall clock의 알려진 skew가 이 값보다 작아야 한다.
`relay.max_pipes`는 process 전체의 Opening/Accepted Pipe와 listener termination cleanup 대기까지 묶은
admission 상한이며 범위는 1–100,000이다. Open worker semaphore, successful O reservation replay cache와 terminal
history도 같은 크기로 제한한다.
Open worker는 stream 수와 곱해 상한을 넘지 않으며 초과 시 기존 Pipe를 evict하지 않고 새 Open만 거부한다. `request_id`는
live stream의 in-flight correlation에만 쓰고 replay/resume하지 않는다. `CancelOpen` ACK는 signal 전달 여부, `ClosePipe` ACK는 exact
session ownership 여부만 나타낸다.
Payload는 메시지 경계를 보존하며 frame당 최대 60 KiB다. 방향별 순서만 보존하고 전달 성공은 local gRPC stream
write 완료까지만 뜻하며 peer application 관찰이나 ACK가 아니다. Stream별 payload queue는 32 frame, process 전체
outbound queued/in-flight payload는 `min(relay.max_pipes, 1024)` frame으로 제한한다. 한계가 `relay.open_timeout`
안에 해소되지 않으면 frame을 조용히 버리지 않고 해당 Pipe를 terminal로 만든다. Local gRPC write가 이미
시작됐다면 destination stream을 실패시키고
그 write 결과를 join한 뒤 반환해 실패 뒤 late write를 막는다. Control/terminal message는 별도 우선 lane을 사용한다.
각 authenticated stream은 unbuffered FIFO Pipe worker 하나로 payload와 `ClosePipe`를 순서화해 receive loop가
outbound failure를 계속 관찰한다. 이 worker 수는 `relay.max_client_sessions`로 제한된다. Stream 종료는 Open
worker를 cancel/join하지만 Pipe worker는 cancel만 하고 handler 반환을 막지 않아 gRPC transport cancellation이
in-flight write를 풀 수 있게 한다.
한 정의의 `endpoint_pattern`은 최대 1024 bytes, `target_id`는 최대 128 bytes로 제한해 최대 snapshot도 internal
gRPC 1 MiB envelope 안에 유지한다.

Docker Compose는 고정된 3-voter smoke cluster를 실행한다. Raft transport는 Compose network 안에만 있고,
각 노드의 internal control gRPC는 `7101`–`7103`, read-only Admin HTTP는 `9091`–`9093`에 노출된다. Internal
owner relay `7300`은 Compose private network에만 두고 host에 publish하지 않는다. Cross-Gateway 증거는 caller와
listener가 다른 Gateway public Relay를 사용해 [H12](docs/test/001-core-correctness-test-plan.md)를 통과한 artifact로만
판정한다.

```bash
docker compose up --build
curl http://127.0.0.1:9091/status
curl http://127.0.0.1:9092/status
curl http://127.0.0.1:9093/status
```

`/status`는 현재 quorum을 확인한 leader에서만 `200`과 `authority_id`를 반환한다. Follower 또는 quorum을
확인할 수 없는 노드는 `503`과 `presence.state=NoAuthority`를 반환한다. `authority_id`와 `presence`가
quorum-confirmed observation이며 Raft/Gateway diagnostic field는 응답 시점의 best-effort local status다.
HTTP 또는 control RPC 호출 취소는 해당 호출만 끝내며 current authority나 다른 control session을 fence하지
않는다.
