# SPEC 008: 전송 보안과 관측 계약

## 전송과 admission

```text
SDK <-> GW : TLS/TCP + server authentication + ClusterToken
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake
```

- **`SEC-001`**: SDK는 CA와 별도 server name을 검증한 뒤에만 ClusterToken을 보낸다.
- **`SEC-002`**: Gateway는 current token 하나와 optional next token 하나만 허용한다.
- **`SEC-003`**: token 불일치는 SessionId/Binding/Pipe 없이 `UNAUTHENTICATED`로 끝난다.
- **`SEC-004`**: 내부 transport는 certificate와 기존 logical identity를 모두 검증한다.
- **`SEC-005`**: TLS 실패는 평문 fallback을 하지 않는다.
- **`SEC-006`**: production server config는 certificate/key 경로를 요구한다.
- **`SEC-007`**: insecure transport는 명시적인 test-only config에서만 허용하며 Helm에는 노출하지 않는다.
- **`SEC-008`**: hop TLS는 application E2E 보호나 Pipe peer 인증이 아니다.
- **`SEC-009`**: public Relay API는 Gateway transport 종류와 독립적이며 0.2 구현은 명시적인 TLS/TCP 하나다.
- **`SEC-010`**: SDK-facing TLS와 내부 mTLS는 별도 Secret과 trust domain으로 운영할 수 있다.
- **`SEC-011`**: 외부 L4는 byte stream을 passthrough하며 Gateway가 SDK TLS를 종단한다.

## 로그

필수 lifecycle event:

```text
session admitted/rejected/removed
listener active/suspended/blocked/closed
dial result + code + observation
peer/RT connect, handshake, loss, recovery
drain start/deadline/complete
TLS handshake rejection
bounded queue/capacity rejection
```

DATA hot path에는 per-frame info 로그를 남기지 않습니다. 로그에는 credential, certificate private key,
payload와 무제한 error body를 넣지 않습니다.

## metric

필수 저카디널리티 범주:

```text
RED: session/publish/dial/peer/RT request result와 duration
USE: current session/binding/pending offer/live Pipe/peer stream/RT mapping과 capacity rejection
recovery: reconnect, route dependency transition, lease expiry, drain
```

DestinationId, SessionId, BindingId, PipeId, Gateway 주소, credential과 자유형 error를 label로 쓰지 않습니다.
identity가 필요한 분석은 bounded lifecycle log를 사용합니다.

## probe

| probe | 보장 | 보장하지 않음 |
| --- | --- | --- |
| startup/readiness `check` | TLS + ClusterToken HELLO/WELCOME | RT, Destination, Pipe, payload 성공 |
| liveness TCP | process가 socket을 수락할 수 있음 | control/data plane 정상 |
| topology test | local/one-hop/dial/Pipe byte 일치 | application 업무 성공 |

- **`OBS-001`**: snapshot gauge는 event counter가 아니라 현재 값으로 갱신한다.
- **`OBS-002`**: cleanup 뒤 gauge는 baseline으로 돌아와야 한다.
- **`OBS-003`**: secret marker는 로그, metric과 error에 나타나지 않아야 한다.
- **`OBS-004`**: Helm annotation은 scrape endpoint만 노출하며 SDK/peer/RT port를 외부 공개하지 않는다.
