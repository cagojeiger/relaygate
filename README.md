# RelayGate

RelayGate는 인증된 caller와 현재 연결 가능한 Listener를 short-lived bidirectional Pipe로 연결한다. Payload 저장, response replay, Pipe resume, durable delivery는 제공하지 않는다.

## 구조

```text
Go/Rust SDK -- public Relay gRPC --> Gateway
                                      |
                                      | control gRPC
                                      v
                  +---------------- Controller quorum ----------------+
                  | durable HashiCorp Raft + current-state FSM         |
                  | control authority + read-only admin                |
                  +----------------------------------------------------+
                                      |
                                      | live control session의 owner relay address
                                      v
                                Owner Gateway
```

Server binary는 두 startup role을 제공한다.

| Role | 소유 | 소유하지 않음 |
| --- | --- | --- |
| `controller` | Durable embedded HashiCorp Raft voter/store, authoritative current-only `GatewaySession`/exact route FSM, control gRPC, read-only admin | Public Relay, peer Relay, SDK session, Pipe payload |
| `gateway` | Public Relay, peer Relay, control client, auth/session/binding/Pipe runtime | Raft node/store, control server, authoritative FSM |

Raft는 `raft.data_dir` 아래에 term, vote, log, membership, stable state, snapshot, `NodeId`, `ClusterEpoch`, current Gateway session, exact route를 저장한다. FSM과 snapshot은 current `GatewaySession`과 exact route만 저장한다. Delete, withdraw, session replacement, Gateway removal은 실제 삭제이며 application tombstone, route generation, payload, Pipe state, credential state, 별도 route history를 남기지 않는다. Raft protocol log는 snapshot compaction 전까지 존재한다.

성공한 snapshot은 과거 logical Raft log를 compact하고 설정된 snapshot 수를 보존한다. Bolt file은 즉시 축소되지 않고 이미 할당된 page를 disk high-water mark로 재사용할 수 있으므로 physical volume 크기와 current FSM cardinality를 별도로 관찰한다.

Controller leader-local volatile state는 current `AuthorityId`, control session, revalidated Gateway mirror, owner relay address다. Leadership이 바뀌면 이 계층을 초기화하고 Gateway가 reconnect/full snapshot으로 live Gateway memory와 committed current FSM을 다시 맞춘다.

Gateway-local volatile state는 authenticated SDK session, local Listener binding, Open attempt fence, Pipe segment, buffer, payload다. Gateway restart는 이를 폐기하며 SDK가 reconnect하고 current Listener declaration을 fresh Bind한다.

## Repository

```text
cmd/relaygate/                         process entrypoint
internal/
├── app/{relaygate,config,admin}       composition, config, observation
├── raft/{state,node,membership}       durable Raft, current FSM, local operator socket
├── gateway/
│   ├── access/{auth,session,runtime}  authentication과 reload retirement
│   ├── control/{model,authority,client,server,transport}
│   ├── routing/{binding,opening}      Open context, Listener, Pipe lifecycle
│   └── relay/{public,peer}            SDK ingress와 Gateway hop
└── gen/{control,gateway,operator,relay}/v1
                                        generated protobuf
proto/relaygate/                       canonical wire schema
sdk/go                                독립 public Go module
sdk/rust/relaygate-sdk                독립 public Rust crate
examples/echo/                         격리된 echo example
docs/{adr,spec,test}/                  결정, 계약, 검증 근거
```

## 복구 모델

정상 복구는 동일 Raft cohort와 durable Controller store를 유지한다.

| 상황 | 결과 |
| --- | --- |
| 같은 volume과 `NodeId`로 Controller process 재시작 | Durable Raft store와 snapshot을 다시 열며 bootstrap은 필요 없다 |
| Quorum이 생존한 same-epoch leader 장애 | 새 leader가 형성되고 빈 authority state에서 Gateway가 reconnect/full snapshot한다 |
| Controller store 유실 | 지워진 `NodeId`를 재사용하지 않고 새 `NodeId` replacement를 add/catch-up한 뒤 잃은 member를 제거한다 |
| Quorum 유실 | Quorum 복구 전까지 새 control/admission을 fail closed한다 |
| Disaster reset | 과거 controller/control/gateway path를 명시적으로 fence한 뒤 별도 epoch/cohort를 시작하며 자동 recovery가 아니다 |

`raft.bootstrap=true`는 빈 Controller store의 최초 cluster formation을 위한 one-shot이며 production recovery 수단이 아니다. 잃은 member identity를 되살리는 데 사용하면 안 된다.

Membership 변경은 REST endpoint나 추가 TCP port를 사용하지 않는다. Current leader Controller 안에서 같은 binary를 실행해 protected Unix socket에 접근한다.

```bash
relaygate membership list -config /etc/relaygate/relaygate.yaml
relaygate membership add -node-id controller-4 -raft-address controller-4:27400 -config /etc/relaygate/relaygate.yaml
# controller-4가 catch-up하고 ready가 된 뒤 잃은 ID를 제거한다.
relaygate membership remove -node-id controller-3 -config /etc/relaygate/relaygate.yaml
```

Lost-store replacement는 fresh `NodeId`, empty data directory, `bootstrap=false`로 먼저 시작한다. 동일 Add/Remove retry는 CLI response loss에도 안전하게 current membership을 반환하며 application work를 replay하지 않는다.

Production Controller에는 durable volume/PVC가 필요하다. Local Compose는 Controller별 named volume을 사용한다. `emptyDir`은 cluster loss를 허용하는 disposable development 환경에서만 사용할 수 있고 HA가 아니다.

## Pipe와 SDK 계약

- `ClientId`는 인증으로 정해지는 strict namespace다.
- Public Go/Rust SDK는 `relay.proto`만 공유하며 server/control/Raft type은 private이다.
- Open admission은 active client session, current authority, quorum, exact directory route, revalidated owner session, owner reservation을 모두 요구한다.
- Ingress Gateway는 exact owner Gateway identity/address마다 gRPC/HTTP2 connection 하나를 공유하고 remote Pipe마다 독립 peer stream 하나를 연다.
- Owner identity/address 변경 시 새 connection을 사용하며 이전 connection은 기존 Pipe stream이 모두 종료된 뒤 닫는다.
- Idle peer connection은 `min(max_pipes, 64)` bounded LRU cache로 제한해 과거 Gateway churn이 socket 누적으로 이어지지 않게 한다.
- 한 peer stream의 timeout/cancel은 해당 Pipe만 종료하고 shared connection의 sibling stream은 유지한다. Connection-level 장애는 그 connection의 Pipe 전체를 종료한다.
- Internal redial, payload retry, resume, replay는 없다.
- `Send` 성공은 peer SDK가 exact `PayloadId`를 bounded receive queue에 넣고 correlated receipt가 돌아왔다는 뜻이다. Peer application processing이나 durable commit을 뜻하지 않으며 local transport handoff 뒤 receipt loss는 `Unknown`이다.
- Multiplexed public Relay stream은 control/terminal과 payload를 별도 bounded lane으로 보내 queued payload pressure를 우회한다.
- Pipe별 peer stream은 bounded lane 하나에서 send를 직렬화한다. Blocked send timeout/cancel은 해당 Pipe와 stream을 종료하지만 frame을 몰래 drop/retry/replay하지 않는다.
- 권장 SDK entry point는 `ManagedClient`다. Session reconnect와 current Listener fresh Bind만 수행한다. Raw `Client`는 session-owned advanced API다. 어느 쪽도 Open queue, outcome replay, Pipe resume, payload replay를 하지 않는다.
- Bind/Unbind validation, capacity, conflict, availability failure는 operation-local이다. Authentication, session, protocol, transport failure는 Relay stream을 종료한다.
- Payload rejection은 SDK의 exact Pipe view를 종료한다. Server는 exact owned Pipe만 terminalize하며 payload를 retry/replay하지 않는다.

## 설치

Release `0.1.0`은 server image와 일치하는 Go/Rust SDK로 구성된다.

```bash
docker pull ghcr.io/cagojeiger/relaygate:v0.1.0
go get github.com/cagojeiger/relaygate/sdk/go@v0.1.0
cargo add relaygate-sdk@0.1.0
```

세 artifact는 같은 public Relay protobuf contract를 사용한다. Release 절차는 [RELEASING.md](RELEASING.md)에 있다.

## Local 설정

```bash
# 폐기 가능한 single-node data directory를 처음 만들 때만 사용한다.
RELAYGATE_RAFT_BOOTSTRAP=true go run ./cmd/relaygate -config ./configs/relaygate.yaml
curl http://127.0.0.1:27490/healthz/ready
curl http://127.0.0.1:27490/status
```

Initial store가 만들어진 뒤에는 file 기본값인 `bootstrap: false`로 시작한다. Production member store를 삭제하거나 교체한 뒤 one-shot bootstrap input을 재사용하면 안 된다.

주요 port:

| Port | 용도 |
| --- | --- |
| `27400` | Controller Raft TCP |
| `27410` | Controller control gRPC |
| `27420` | Gateway public Relay gRPC |
| `27430` | Gateway internal owner relay gRPC |
| `27490` | Read-only Admin HTTP |

Compose는 named volume을 가진 Controller 세 개와 durable store가 없는 Gateway 두 개를 시작한다. 최초 cluster formation에서 command-scoped bootstrap input을 한 번만 설정한다.

```bash
RELAYGATE_BOOTSTRAP_ONCE=true docker compose up -d --build
# controller-1만 steady-state 기본값(bootstrap=false)으로 다시 만든다.
docker compose up -d --no-build --no-deps --force-recreate controller-1
curl http://127.0.0.1:27591/status
curl http://127.0.0.1:27594/status
docker compose down --remove-orphans
```

이후에는 `docker compose up -d`만 사용한다. `RELAYGATE_BOOTSTRAP_ONCE`는 recovery 수단이 아니며 Controller volume이 지워진 뒤 재사용하면 안 된다.

Smoke harness는 multi-role local 구성을 검증하고 실행 뒤 생성한 project resource를 제거한다.

```bash
./scripts/compose-smoke.sh
```

## Security와 production 경계

현재 internal control, peer, Raft transport와 Admin REST는 trusted local/dev network를 가정한다. Production에는 다음 근거가 추가로 필요하다.

- Internal peer/control identity authentication 또는 mTLS
- Raft transport mTLS policy
- `ClockSkewBound < relay.open_timeout` 근거
- Controller replacement, quorum restoration, explicit disaster reset fencing의 production runbook 근거

계약: [SPEC 001](docs/spec/001-system-model.md), [SPEC 003](docs/spec/003-failure-and-recovery-model.md), [SPEC 004](docs/spec/004-state-transition-model.md). 검증: [TEST 001](docs/test/001-core-correctness-test-plan.md), [TEST 002](docs/test/002-failure-evidence-matrix.md). Runtime 결정: [ADR 002](docs/adr/002-current-state-cluster-and-recovery.md), [ADR 005](docs/adr/005-runtime-and-release-boundary.md).
