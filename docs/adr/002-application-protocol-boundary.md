# ADR 002: application 의미와 보안은 endpoint가 소유한다

| 항목 | 값 |
| --- | --- |
| 상태 | Proposed |
| 전제 | [ADR 001](001-relayed-pipe-responsibility-boundary.md) |

## 맥락

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

배포 환경은 SDK가 접속하는 Gateway service의 identity와 channel integrity를 보장하고, Gateway 간 및 Gateway-RT 간 내부 channel에서는 component identity를 인증된 transport context에 결합해야 한다. protocol message가 주장하는 `GatewayId`만으로 component를 신뢰하지 않는다. 이 infrastructure trust는 Pipe 위 application peer 인증·인가와 별개의 책임이다.

## 결과

- Gateway와 SDK는 payload를 해석하지 않는다.
- 여러 Listener에 분산된 상태나 결과의 통합은 application 책임이다.
- RelayGate의 rate, connection과 buffer 제한은 자원 보호이지 application 권한 모델이 아니다.
- `ClientKey` 폐기는 신규 binding과 신규 Pipe admission을 막지만 이미 admission을 마친 Pipe의 application 권한을 소급해 판단하지 않는다.
- 내부 transport identity와 integrity가 없는 배포는 RelayGate의 `GatewayId`와 RT publication 신뢰 전제를 만족하지 않는다.

## 이 ADR에서 정하지 않는 것

- `ClientKey` 형식, 배포, rotation과 폐기 절차
- 등록 권한 실패의 오류 코드
- application handshake와 payload protocol
- application의 분산 조정 방식
- TLS, mTLS, service mesh와 certificate 배포 방식

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [RFC 3439](../rfc/rfc-3439-simplicity-principle.md)
