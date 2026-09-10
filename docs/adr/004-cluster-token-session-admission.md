# ADR 004: ClusterToken은 SDK session admission을 제한한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted, implemented |
| credential | current 1개 + next 0..1개 |
| 권한 단위 | RelayGate trust domain |

## 결정

```text
Configured transport ready (TLS by default)
   └── HELLO(ClusterToken)
         ├── current | next ──► SessionId ──► listen · dial
         └── mismatch       ──► UNAUTHENTICATED
```

| 관심사 | 소유자·의미 |
| --- | --- |
| session admission | Gateway가 ClusterToken으로 trust-domain membership 확인 |
| Destination access | admitted session은 모든 Destination에 listen·dial |
| Destination 주소 | 공개 routing identifier |
| peer identity·authorization | Pipe 위 application protocol |
| token 공급 | application config와 Kubernetes Secret |
| token 보관 | operator의 external secret system |

SDK는 application이 전달한 token을 지정 transport(TLS 기본) 성립 뒤 제시합니다.
명시적 TCP endpoint에서는 token도 평문이며 제공자가 전송 보호를 책임집니다. Gateway는 timing-safe 비교를 사용하고 token을
Debug, log, metric, error, RT, Binding과 Pipe state에서 redaction합니다.

## 회전

```text
Gateway accepts current + next
        -> SDK moves to next
        -> next becomes current
        -> Gateway rollout
```

이미 admitted된 session은 현재 transport 수명 동안 유지됩니다. 즉시 폐기는 Gateway rollout과 SDK
reconnect로 수행합니다.

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [SPEC 008](../spec/008-runtime-observability-contract.md)
