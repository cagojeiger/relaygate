# ADR 001: RelayGate는 Destination을 1:1 Pipe로 연결하는 범위까지 책임진다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted |
| 입력 | `listen(RouteAddress, token source)`, `dial(RouteAddress, token source)`, opaque bytes |
| 출력 | terminal result 또는 full-duplex Pipe 1개 |

## 결정

```text
Application A                                      Application B
peer auth · payload · ack                     peer auth · payload · ack
       │                                                  │
       └── SDK ═══════ opaque bidirectional Pipe ═════ SDK ┘
                       RelayGate boundary
```

| 주체 | 책임 |
| --- | --- |
| RelayGate | operation JWT grant 검증, live RouteAddress publication·조회, local/one-hop Pipe, bounded lifecycle·오류 관측 |
| Application | RouteAddress 생성·보관, token 발급·갱신, Pipe 상대 인증·인가, payload framing·의미·acknowledgement·업무 재시도, 필요한 E2E 보호 |
| Platform | 외부 L4 진입점, 인증서·Secret 공급, workload 배포·재시작 |

Namespace grant는 RelayGate의 publish/dial admission을 제한합니다. Pipe 상대 identity와 payload 권한은
application이 Pipe 위 protocol로 적용합니다.

```text
RouteAddress -> live Binding 0..N
dial 1회     -> eligible Binding 1개 -> Pipe 1개
Pipe         -> ordered opaque bytes in both directions
```

RelayGate의 완료 경계는 Pipe 수립과 byte-path I/O 결과입니다. Application protocol의 acknowledgement가
상대 application의 payload 처리 결과를 확정합니다.

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [RFC 3439](../rfc/rfc-3439-simplicity-principle.md)
- [SPEC 001](../spec/001-terminology-and-object-model.md)
- [SPEC 002](../spec/002-sdk-pipe-contract.md)
