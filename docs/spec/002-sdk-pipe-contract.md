# SPEC 002: SDK와 Pipe 계약

## API

```text
Relay::connect(Config)          -> Relay
Relay::listen(DestinationId)    -> Listener
Relay::dial(DestinationId)      -> Pipe
Listener::accept()              -> Pipe
Listener::close()               -> Listener 종료
Relay::close()                  -> 전체 runtime 종료
```

`Config`는 Gateway 주소, `ClusterToken`, TLS CA/server name, timeout, heartbeat, reconnect와 bounded
queue 값을 가진다. SDK는 환경 변수를 읽지 않는다.

## session 복구

```text
CONNECTING ── WELCOME ──► ACTIVE
    ▲                       │
    └── jitter/backoff ◄── RECONNECTING
                            │ terminal config/admission
                            v
                          BLOCKED
                            │ close
                            v
                          CLOSED
```

- **`SDK-001`**: `Relay::connect`는 TCP, TLS, `HELLO/WELCOME`이 완료된 뒤 반환한다.
- **`SDK-002`**: network loss, heartbeat timeout 또는 bounded writer 실패는 current session 전체를 끝낸다.
- **`SDK-003`**: retryable session loss는 bounded exponential backoff와 runtime별 jitter로 재연결한다.
- **`SDK-004`**: 이미 반환된 non-closed Listener만 새 session에 자동 publish한다.
- **`SDK-005`**: 기존 Pipe, committed dial, pending initial listen과 payload는 replay·resume하지 않는다.
- **`SDK-006`**: terminal admission/config 오류는 자동 hot retry하지 않는다.
- **`SDK-007`**: 명시적 close 뒤에는 재연결하지 않는다.

## Listener와 accept

```text
REGISTERING -> ACTIVE -> SUSPENDED -> ACTIVE
                  │          │
                  ├-------> BLOCKED
                  └-------> CLOSED
```

- **`SDK-008`**: `listen`은 Gateway가 Binding을 확인한 뒤 Listener를 반환한다.
- **`SDK-009`**: incoming OFFER는 Listener별 bounded queue에 admission된 뒤에만 성공한다.
- **`SDK-010`**: `accept`는 distinct Pipe를 정확히 한 번 반환한다.
- **`SDK-011`**: session이 끝난 뒤 old unaccepted Pipe는 반환하지 않는다.
- **`SDK-012`**: Listener close는 신규 수신과 unaccepted Pipe를 끝내지만 이미 반환된 Pipe는 독립적이다.
- **`SDK-013`**: 같은 Relay의 동일 Destination 중복 listen은 `ALREADY_EXISTS`다.

## Pipe

```text
OPENING -> OPEN -> HALF_CLOSED -> CLOSED
             └──── RESET ───────► CLOSED
```

- **`PIPE-001`**: Pipe는 full-duplex opaque byte stream이다.
- **`PIPE-002`**: `FIN`은 한 방향 write half-close이며 반대 방향은 계속 사용할 수 있다.
- **`PIPE-003`**: `CLOSE`는 정상 종료, `RESET`은 오류 종료다.
- **`PIPE-004`**: 모든 frame, queue와 buffer에는 상한이 있다.
- **`PIPE-005`**: 한 Pipe 종료는 sibling Pipe, Listener와 Binding을 제거하지 않는다.
- **`PIPE-006`**: Pipe I/O 성공은 application이 payload를 처리했다는 acknowledgement가 아니다.
