# SPEC 002: SDK와 Pipe 계약

## API

| 호출 | 결과 |
| --- | --- |
| `Relay::connect(Config)` | active Relay |
| `Relay::listen(RouteAddress, AccessTokenSource)` | active Listener |
| `Relay::dial(RouteAddress, AccessTokenSource)` | established Pipe |
| `Listener::accept()` | distinct incoming Pipe |
| `Listener::close()` | 해당 Listener 종료 |
| `Relay::close()` | 전체 SDK runtime 종료 |

`Config`는 Gateway transport, timeout, heartbeat, reconnect와 bounded queue 값을 가집니다. Application이
config source를 읽어 SDK에 전달합니다. `HELLO/WELCOME`에는 credential이 없습니다.

`host:port`와 `tls://host:port`는 기본 공인 CA, endpoint 이름 검증, SNI와 `relaygate/3` ALPN을 자동
적용합니다. `tcp://host:port`는 제공자가 명시한 평문 연결입니다. IPv6는 `[::1]:port` 형식을 사용합니다.
사설 CA는 `with_ca_certificate`로 지정하며 평문 endpoint에는 CA 설정을 허용하지 않습니다. TLS 검증 실패 후
평문 재접속은 없습니다. Agent 풀과 작업 분배 정책은 application이 소유합니다.

## AccessTokenSource

```text
static token -----------------------> PUBLISH 또는 DIAL
async callback(action, RouteAddress) -> PUBLISH 또는 DIAL
```

| ID | 계약 |
| --- | --- |
| `SDK-001` | `Relay::connect`는 지정 transport와 credential-free `HELLO/WELCOME` 완료 뒤 반환한다. |
| `SDK-002` | network loss, heartbeat timeout과 bounded writer failure는 current session을 끝낸다. |
| `SDK-003` | retryable session loss는 bounded exponential backoff와 runtime별 jitter로 재연결한다. |
| `SDK-004` | 새 session은 이미 반환된 live Listener를 새 AccessToken으로 자동 republish한다. |
| `SDK-005` | recovery는 새 session·Binding·dial을 만들며 existing Pipe, committed dial, pending initial listen과 payload는 terminal이다. |
| `SDK-006` | terminal config·protocol 오류는 `BLOCKED`로 수렴하며 config를 바꾼 새 runtime으로 회복한다. |
| `SDK-007` | explicit close는 같은 runtime의 terminal `CLOSED`로 수렴한다. |
| `SDK-014` | AccessToken은 비어 있지 않은 최대 4,096 bytes이고 Debug 출력은 값을 redaction한다. |
| `SDK-015` | dynamic AccessTokenSource는 `AccessAction`과 exact RouteAddress를 받아 application-owned future를 실행한다. |
| `SDK-016` | Listener는 AccessTokenSource를 보관하고 initial publish와 republish마다 다시 호출한다. |
| `SDK-017` | dial은 API 호출당 AccessTokenSource를 정확히 한 번 resolve하며 committed operation을 SDK가 replay하지 않는다. |
| `SDK-018` | SDK는 token cache, singleflight, refresh token, private key, token issuer를 소유하지 않는다. |

Token source 실패와 deadline은 해당 operation의 `UNAVAILABLE` 또는 `DEADLINE_EXCEEDED/NOT_OBSERVED`입니다.
Initial listen은 오류로 반환합니다. 이미 반환된 Listener의 republish token source 실패는 `SUSPENDED`로 두고
bounded delay 뒤 다시 공급을 요청합니다. Gateway의 `UNAUTHENTICATED`·`PERMISSION_DENIED`는 Listener를
`BLOCKED`로 만듭니다. Application은 새로운 token source 또는 새 Relay/Listener를 구성해 회복합니다.

## RelaySession

```mermaid
stateDiagram-v2
    [*] --> CONNECTING
    CONNECTING --> ACTIVE: TLS + HELLO/WELCOME
    ACTIVE --> RECONNECTING: retryable session loss
    RECONNECTING --> ACTIVE: reconnect + republish
    CONNECTING --> BLOCKED: terminal config/protocol
    ACTIVE --> BLOCKED: terminal config/protocol
    RECONNECTING --> BLOCKED: terminal config/protocol
    CONNECTING --> CLOSED: Relay.close
    ACTIVE --> CLOSED: Relay.close
    RECONNECTING --> CLOSED: Relay.close
    BLOCKED --> CLOSED: Relay.close
```

## Listener

```mermaid
stateDiagram-v2
    [*] --> REGISTERING
    REGISTERING --> ACTIVE: Binding confirmed
    REGISTERING --> BLOCKED: terminal failure
    REGISTERING --> CLOSED: close
    ACTIVE --> SUSPENDED: session/token-source loss
    SUSPENDED --> ACTIVE: republished
    ACTIVE --> BLOCKED: terminal authorization failure
    SUSPENDED --> BLOCKED: terminal authorization failure
    ACTIVE --> CLOSED: close
    SUSPENDED --> CLOSED: close
    BLOCKED --> CLOSED: close
```

| ID | 계약 |
| --- | --- |
| `SDK-008` | `listen`은 Gateway가 Binding을 확인한 뒤 Listener를 반환한다. |
| `SDK-009` | incoming OFFER는 Listener별 bounded queue admission 뒤 성공한다. |
| `SDK-010` | `accept`는 distinct Pipe를 정확히 한 번 반환한다. |
| `SDK-011` | session 종료는 old unaccepted Pipe를 제거한다. |
| `SDK-012` | Listener close는 신규 수신과 unaccepted Pipe를 끝내고 returned Pipe는 독립 유지한다. |
| `SDK-013` | 같은 Relay의 동일 RouteAddress 중복 listen은 `ALREADY_EXISTS`다. |

## Pipe

```mermaid
stateDiagram-v2
    [*] --> OPENING
    OPENING --> OPEN
    OPEN --> HALF_CLOSED: FIN
    HALF_CLOSED --> CLOSED: opposite FIN / CLOSE
    OPEN --> CLOSED: CLOSE / RESET
    HALF_CLOSED --> CLOSED: RESET
```

| ID | 계약 |
| --- | --- |
| `PIPE-001` | Pipe는 full-duplex opaque byte stream이다. |
| `PIPE-002` | `FIN`은 한 방향 write half-close이고 반대 방향은 계속 사용한다. |
| `PIPE-003` | `CLOSE`는 정상 종료, `RESET`은 오류 종료다. |
| `PIPE-004` | frame, queue와 buffer는 모두 bounded다. |
| `PIPE-005` | Pipe terminal cleanup은 해당 Pipe에 한정되고 sibling Pipe, Listener와 Binding은 유지된다. |
| `PIPE-006` | Pipe I/O success는 RelayGate byte path의 성공이며 application acknowledgement는 application protocol이 정의한다. |
