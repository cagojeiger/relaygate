# SPEC 010: 전송과 admission 보호 계약

SDK edge·내부 전송 보안, SDK socket·제어 요청 admission 보호와 runtime 환경변수를 소유합니다. 이 경계의 metric은
[SPEC 008](008-runtime-observability-contract.md), operation token 검증은 [SPEC 009](009-operation-jwt-authorization-contract.md)가 소유합니다.

## 전송

```text
SDK <-> GW : TLS/TCP + server authentication + credential-free HELLO
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake
```

위 구성이 기본값입니다. `RELAYGATE_INTERNAL_TRANSPORT=plaintext`는 내부 두 구간만 평문 TCP로 실행하고
SDK TLS를 유지합니다. SDK edge는 독립적으로 `RELAYGATE_SDK_TRANSPORT=tls|plaintext`를 사용합니다(`tls`
기본). SDK Gateway endpoint의 `tcp://`는 plaintext에 대응하며 access token과 payload를 암호화하지 않습니다. Unknown mode,
제거된 test flag(`RELAYGATE_INSECURE_TEST_TRANSPORT`, `RELAYGATE_RT_TRUSTED_LOCAL`)가 설정되면 시작 실패입니다. Readiness도 같은 mode를 사용합니다.

| ID | 계약 |
| --- | --- |
| `SEC-001` | TLS endpoint에서 SDK는 CA trust source, server name, SNI와 `relaygate/3` ALPN을 검증한 뒤 credential-free HELLO를 보낸다. |
| `SEC-002` | HELLO는 application identity·token·Destination을 포함하지 않고 WELCOME은 새 SessionId만 부여한다. |
| `SEC-003` | internal mTLS는 신뢰 CA·Gateway client DNS SAN·서버 DNS SAN·`relaygate/3` ALPN을 검증한다. Logical identity는 incarnation/owner fencing에 사용한다. |
| `SEC-004` | TLS failure는 평문 fallback 없는 terminal connection failure이며 새 retry는 새 TLS connection으로 시작한다. |
| `SEC-005` | Gateway SDK TLS와 internal mTLS는 서버 certificate/key path를 요구한다. 공인 TLS SDK client는 기본 roots를 사용한다. |
| `SEC-006` | internal mode는 `mtls` 기본값 또는 명시적 `plaintext`다. Plaintext는 인증과 전송 암호화가 없는 격리 테스트 mode다. |
| `SEC-007` | hop TLS는 transport peer를 보호하고 application E2E/peer auth는 Pipe 위 application protocol이 담당한다. |
| `SEC-008` | public Relay API는 transport-independent이고 endpoint는 TLS/TCP 기본 또는 명시적 TCP다. |
| `SEC-009` | SDK edge와 internal mTLS는 독립 Secret·trust domain이다. |
| `SEC-010` | internal plaintext는 SDK edge TLS와 operation authorization을 비활성화하지 않는다. |
| `SEC-011` | external L4는 byte stream passthrough이고 Gateway가 SDK TLS를 종료한다. |
| `SEC-012` | SDK accept는 전체 transport slot과 별도 handshake slot을 TLS 전에 확보한다. Handshake 상한 도달 시 새 socket을 닫고 기존 session을 유지한다. |
| `SEC-013` | HELLO payload 상한은 0 bytes다. HELLO 읽기와 WELCOME/거절 쓰기는 합쳐 5초 이내 끝내고 성공 뒤 일반 frame 한도로 전환하며 이미 읽은 다음 frame을 보존한다. |
| `SEC-014` | SDK 신규 socket은 slot·TLS 처리 전 GW-local rate budget을 통과한다. 초기 burst와 초당 refill을 제한하고 부족하면 새 socket만 종료한다. Clone은 같은 예산을 공유한다. |
| `SEC-015` | SDK PUBLISH/DIAL은 session별·GW 전체의 공유 제어 예산을 통과한 뒤 authorization과 state operation을 수행한다. 초과 요청은 `RESOURCE_EXHAUSTED`, DIAL은 `NOT_OBSERVED`다. |

## SDK handshake 보호

```text
accept -> rate budget -> transport + handshake slot -> TLS(5s) -> HELLO/response(5s)
                                                        |-- success -> slot 반환 -> session
                                                        `-- failure -> state + slot 반환
```

| 설정 | 기본값·의미 |
| --- | --- |
| `RELAYGATE_MAX_PENDING_HANDSHAKES` | 256; 실제 한도는 `min(설정값, MAX_SESSIONS)`. 0은 시작 실패 |
| `RELAYGATE_MAX_SESSIONS` | 10,000; handshake와 admitted session을 포함한 전체 transport 수 |
| `RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND` | 256; 초당 신규 admission budget refill. 0은 시작 실패 |
| `RELAYGATE_SDK_CONNECTION_BURST` | 256; 초기·유휴 후 budget 최대 보유량. 0은 시작 실패 |
| HELLO payload | 0 bytes |

256은 보호 상한이며 처리량 보장 수치가 아닙니다. 기존 Relay의 managed reconnect는 SDK backoff로 분산합니다.
초기 `Relay::connect`는 단일 시도이므로 socket admission 거절 뒤 재시도는 application이 결정합니다. Readiness
조회는 budget을 소비하지 않고 기존 session은 유지합니다. 소비한 rate budget은 실패·종료에도 반환하지 않고
시간으로만 보충합니다. 임의의 t초 구간에서 통과 수는 `burst + rate * t` 이하입니다. 이 제한은 GW-local이고
GW 재시작은 burst를 초기화하며 replica 증가는 cluster 총 예산을 늘립니다.

## SDK 제어 요청 보호

```text
PUBLISH / DIAL -> session budget -> Gateway budget -> authorization -> state operation
DATA / PING / OFFER 응답 / UNPUBLISH / CANCEL / FIN / CLOSE / RESET -> 기존 처리
```

| 설정 | 기본값 |
| --- | --- |
| `RELAYGATE_CONTROL_RATE_PER_SECOND` / `RELAYGATE_CONTROL_BURST` | GW 전체 4,096/s · burst 4,096 |
| `RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND` / `RELAYGATE_SESSION_CONTROL_BURST` | session별 256/s · burst 256 |

두 operation은 같은 bucket을 공유합니다. Session budget이 없는 요청은 GW budget을 소비하지 않습니다. 소비한
budget은 결과·연결 종료와 무관하게 시간으로만 보충합니다. Session 종료는 해당 bucket을 제거하고 GW bucket은
유지합니다. 거절은 authorization·registry 변경·RT Resolve·peer OPEN 전에 결정합니다. DIAL ConnectionId fence는
거절 후에도 유지합니다. 기존 Binding·Pipe와 정리 메시지는 이 제한을 사용하지 않습니다.

## Runtime 환경변수

`relaygate-server`가 읽는 환경변수의 canonical 목록입니다. 동작 계약은 각 절과 SPEC이 소유하고 이 절은 이름,
기본값과 시작 검증만 모읍니다. 기본값은 `crates/relaygate-server/src/config/`와 각 crate의 `DEFAULT_*` 상수를
따릅니다.

공통 규칙:

- 정수와 `_MS` 값은 양의 정수이며 0이나 parse 실패는 listener를 열기 전에 시작 실패입니다.
- `_MS` 값은 monotonic deadline으로 표현할 수 있어야 합니다.
- 제거된 `RELAYGATE_CLUSTER_TOKEN`, `RELAYGATE_NEXT_CLUSTER_TOKEN`이 설정되면 시작 실패입니다([ADR 016](../adr/016-per-operation-jwt-authorization.md)).

### 공통 process

| 변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_LOG` | `info` | tracing filter. 없으면 `RUST_LOG`/기본 filter |
| `RELAYGATE_LOG_FORMAT` | `text` | `text` 또는 `json` |
| `RELAYGATE_METRICS_BIND_ADDR` | 없음(비활성) | Prometheus exporter socket address |
| `RELAYGATE_METRICS_INTERVAL_MS` | 5000 | Gateway gauge 갱신 주기(RouteTable은 검증만). `RELAYGATE_METRICS_BIND_ADDR` 없이 설정하면 시작 실패 |

### 전송 mode

규칙은 [전송](#전송)과 [ADR 014](../adr/014-explicit-internal-transport-mode.md)를 따릅니다.

| 변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_SDK_TRANSPORT` | `tls` | SDK edge `tls` 또는 `plaintext` |
| `RELAYGATE_INTERNAL_TRANSPORT` | `mtls` | GW↔GW·RT `mtls` 또는 `plaintext` |
| `RELAYGATE_SDK_TLS_CERT_PATH` / `RELAYGATE_SDK_TLS_KEY_PATH` | 없음 | SDK TLS 사용 시 필수 |
| `RELAYGATE_SDK_TLS_SERVER_NAME` | 없음 | TLS `check` 명령에서 필수 |
| `RELAYGATE_SDK_TLS_CA_PATH` | Web PKI roots | `check` 명령의 사설 CA |
| `RELAYGATE_INTERNAL_TLS_CA_PATH` / `RELAYGATE_INTERNAL_TLS_CERT_PATH` / `RELAYGATE_INTERNAL_TLS_KEY_PATH` | 없음 | 내부 mTLS 사용 시 필수 |
| `RELAYGATE_PEER_TLS_SERVER_NAME` | 없음 | distributed Gateway 또는 RouteTable의 mTLS에서 필수. Gateway는 peer 검증 이름, RouteTable은 접속을 허용할 Gateway client 인증서 이름 |
| `RELAYGATE_RT_TLS_SERVER_NAME` | 없음 | distributed Gateway mTLS에서 필수 |
| `RELAYGATE_INSECURE_TEST_TRANSPORT` / `RELAYGATE_RT_TRUSTED_LOCAL` | – | 0.4에서 제거. 설정되어 있으면 시작 실패 |

### Gateway

| 변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_BIND_ADDR` | `0.0.0.0:27420` | SDK listener |
| `RELAYGATE_AUTH_CONFIG_PATH` | 없음(필수) | Namespace issuer 설정([SPEC 009](009-operation-jwt-authorization-contract.md)) |
| `RELAYGATE_WRITER_QUEUE_CAPACITY` | 128 | session별 SDK writer queue |
| `RELAYGATE_MAX_FRAME_LEN` | 1 MiB | SDK frame 최대 길이 |
| `RELAYGATE_MAX_SESSIONS` | 10,000 | [SDK handshake 보호](#sdk-handshake-보호) |
| `RELAYGATE_MAX_PENDING_HANDSHAKES` | 256 | 동일 |
| `RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND` / `RELAYGATE_SDK_CONNECTION_BURST` | 256 / 256 | 동일 |
| `RELAYGATE_CONTROL_RATE_PER_SECOND` / `RELAYGATE_CONTROL_BURST` | 4,096 / 4,096 | [SDK 제어 요청 보호](#sdk-제어-요청-보호) |
| `RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND` / `RELAYGATE_SESSION_CONTROL_BURST` | 256 / 256 | 동일 |
| `RELAYGATE_MAX_BINDINGS` | 100,000 | GW 전체 live Binding |
| `RELAYGATE_MAX_PENDING_OFFERS` | 10,000 | 응답 대기 OFFER |
| `RELAYGATE_MAX_REMOTE_DIAL_ATTEMPTS` | 128 | 동시에 진행 중인 remote dial attempt |
| `RELAYGATE_MAX_LIVE_PIPES` | 100,000 | GW 전체 live Pipe |
| `RELAYGATE_OFFER_TIMEOUT_MS` | 5000 | OFFER deadline(`DIAL-008`) |
| `RELAYGATE_DRAIN_TIMEOUT_MS` | 120000 | graceful drain 상한([ADR 010](../adr/010-bounded-gateway-drain-and-reconnect-jitter.md)) |
| `RELAYGATE_SDK_HEARTBEAT_IDLE_MS` / `RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS` | 60000 / 20000 | SDK session liveness([ADR 008](../adr/008-transport-liveness-and-idle-retirement.md)) |
| `RELAYGATE_STATS_INTERVAL_MS` | 없음(비활성) | 주기적 state stats log |

### Distributed Gateway

`RELAYGATE_RT_SHARD_DIRECTORY_PATH`, `RELAYGATE_GATEWAY_NAME`, `RELAYGATE_GATEWAY_LOCATOR`,
`RELAYGATE_PEER_BIND_ADDR` 중 하나라도 있으면 distributed mode이며 아래 필수 값을
모두 요구합니다.

| 변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_RT_SHARD_DIRECTORY_PATH` | 없음(필수) | ShardDirectory JSON |
| `RELAYGATE_GATEWAY_NAME` | 없음(필수) | logical Gateway 이름 |
| `RELAYGATE_GATEWAY_LOCATOR` | 없음(필수) | peer가 접속할 주소 |
| `RELAYGATE_PEER_BIND_ADDR` | `0.0.0.0:27421` | peer listener |
| `RELAYGATE_PEER_HEARTBEAT_IDLE_MS` / `RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS` | 60000 / 20000 | peer transport liveness |
| `RELAYGATE_PEER_IDLE_TIMEOUT_MS` | 300000 | stream 없는 peer transport retirement |

### RouteTable

| 변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_RT_BIND_ADDR` | `127.0.0.1:27430` | RT listener |
| `RELAYGATE_RT_SHARD_DIRECTORY_PATH` | 없음(필수) | ShardDirectory JSON |
| `RELAYGATE_RT_SHARD_ID` | `rt-0` | 이 process가 소유하는 shard |
| `RELAYGATE_RT_LEASE_TTL_MS` | 30000 | registration lease([SPEC 004](004-route-table-contract.md)) |
| `RELAYGATE_RT_REQUEST_QUEUE_CAPACITY` | 128 | shard actor request queue |
| `RELAYGATE_RT_WRITER_QUEUE_CAPACITY` | 32 | connection별 writer queue |
| `RELAYGATE_RT_MAX_CONNECTIONS` | 1,024 | 동시 Gateway connection |
| `RELAYGATE_RT_MAX_FRAME_LEN` | 1 MiB | RT frame 최대 길이 |
| `RELAYGATE_RT_HANDSHAKE_TIMEOUT_MS` | 3000 | connection handshake deadline |
