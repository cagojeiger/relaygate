# ADR 002: 하나의 Relay 세션이 송신과 수신을 함께 수행한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted, implemented |
| SDK runtime | `Relay` |
| session role | Pipe마다 dialer와 listener로 결정 |

## 결정

```text
Relay runtime 1 ── current RelaySession 0..1
Relay::listen(Destination, token source) ──► Listener
Relay::dial(Destination, token source)   ──► Pipe
Listener::accept()           ──► Pipe

Destination * ◄── Binding ──► * RelaySession
dial 1회 ──► Binding 1개 ──► bidirectional Pipe 1개
```

| 규칙 | 결과 |
| --- | --- |
| 하나의 Relay·Destination | pending/active Listener 최대 하나 |
| 여러 Relay·Destination | live Binding 0..N |
| self Binding | dial candidate에서 제외 |
| session loss | live Listener를 새 SessionId·BindingId로 재등록 |
| existing Pipe·committed dial | terminal 결과로 유지, 새 operation이 복구 담당 |

`Listener`는 Destination 수신 의도를 소유하는 handle이고 별도 transport session이 아닙니다. bounded
incoming queue admission이 Pipe 수신 경계를 만들며 `accept()`는 각 Pipe를 한 번 반환합니다.

## 참고

- [RFC 4254](../rfc/rfc-4254-ssh-channel.md)
- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [SPEC 001](../spec/001-terminology-and-object-model.md)
- [SPEC 002](../spec/002-sdk-pipe-contract.md)
