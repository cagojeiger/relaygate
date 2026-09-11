# ADR 018: RelayGate는 operation grant 뒤 Destination을 1:1 Pipe로 연결한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted; supersedes [ADR 001](001-relaygate-responsibility-boundary.md) |
| 입력 | `listen(Destination, AccessTokenSource)`, `dial(Destination, AccessTokenSource)`, opaque bytes |
| 출력 | terminal result 또는 full-duplex Pipe 1개 |

## 결정

```text
Application A                                      Application B
token issuance · peer auth                    token issuance · peer auth
payload · acknowledgement                     payload · acknowledgement
       │                                                  │
       └── SDK ═══════ opaque bidirectional Pipe ═════ SDK ┘
                       RelayGate boundary
```

| 주체 | 책임 |
| --- | --- |
| RelayGate | operation JWT grant 검증, live Destination publication·조회, local/one-hop Pipe, bounded lifecycle·오류 관측 |
| Application | Destination 생성·보관, token 발급·갱신, Pipe 상대 인증·인가, payload framing·의미·acknowledgement·업무 재시도, 필요한 E2E 보호 |
| Platform | 외부 L4 진입점, 인증서·Secret 공급, workload 배포·재시작 |

```text
Destination -> live Binding 0..N
dial 1회     -> eligible Binding 1개 -> Pipe 1개
Pipe         -> ordered opaque bytes in both directions
```

Namespace grant는 RelayGate의 `PUBLISH`·`DIAL` admission을 제한합니다. RelayGate의 완료 경계는 Pipe
수립과 byte-path I/O 결과이며, Pipe 상대 identity와 application payload 처리 결과는 application protocol이
확정합니다.

## 참고

- [ADR 015](015-hierarchical-destination.md)
- [ADR 016](016-per-operation-jwt-authorization.md)
- [SPEC 001](../spec/001-terminology-and-object-model.md)
- [SPEC 002](../spec/002-sdk-pipe-contract.md)
