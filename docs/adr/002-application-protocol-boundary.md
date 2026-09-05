# ADR 002: application 의미와 보안은 endpoint가 소유한다

| 항목 | 값 |
| --- | --- |
| 상태 | Superseded by ADR 011 |
| 전제 | [ADR 001](001-relayed-pipe-responsibility-boundary.md) |

## 맥락

Destination별 ClientKey 결정은 ADR 011이 대체하며 application protocol 경계는 유지한다.

RelayGate가 payload 의미와 end-to-end 정책까지 소유하면 모든 application 요구가 relay core에 결합된다.

## 결정

```text
RelayGate   = Pipe relay + ClientId binding 등록 권한 확인
Application = framing + peer 인증·인가 + service 선택
            + aggregation + idempotency + 업무 retry
Deployment  = Gateway service identity + channel integrity
            + Gateway/RT component identity
```

`ClientKey`는 Listener가 해당 `ClientId`에 binding을 등록할 권한만 증명한다. Pipe 위의 peer 인증·인가와 payload 의미는 Connector와 Listener application이 정의한다.

Gateway는 process 시작 시 external configuration에서 `ClientId -> ClientKey` map을 로드한다. configured `ClientId`마다 `ClientKey`는 하나이며 process 수명 동안 바뀌지 않는다. Listener는 최초 등록과 재연결 뒤 등록마다 같은 key를 제시하고 Gateway는 binding을 만들기 전에 이를 검증한다.

배포 환경은 SDK가 접속하는 Gateway service의 identity와 channel integrity를 보장하고, Gateway 간 및 Gateway-RT 간 내부 channel에서는 component identity를 인증된 transport context에 결합해야 한다. protocol message가 주장하는 `GatewayId`만으로 component를 신뢰하지 않는다. 이 infrastructure trust는 Pipe 위 application peer 인증·인가와 별개의 책임이다.

## 결과

- Gateway와 SDK는 payload를 해석하지 않는다.
- 여러 Listener에 분산된 상태나 결과의 통합은 application 책임이다.
- RelayGate의 rate, connection과 buffer 제한은 자원 보호이지 application 권한 모델이 아니다.
- RelayGate는 `ClientKey`를 발급하거나 영속화하지 않으며 runtime hot reload, 동시 key rotation과 active binding의 즉시 폐기를 제공하지 않는다.
- key 변경은 새 Gateway process configuration과 새 `listen(ClientId, ClientKey)` operation으로 적용한다.
- 내부 transport identity와 integrity가 없는 배포는 RelayGate의 `GatewayId`와 RT registration 신뢰 전제를 만족하지 않는다.

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [RFC 3439](../rfc/rfc-3439-simplicity-principle.md)
