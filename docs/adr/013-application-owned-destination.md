# ADR 013: DestinationId는 application이 소유하는 UUIDv4 주소다

| 항목 | 값 |
| --- | --- |
| 상태 | 채택, 구현됨 |
| 전제 | [ADR 010](010-symmetric-relay-session.md) |

## 결정

```text
생성·영구 보관 : Application
형식 검증      : SDK + Gateway
현재 위치      : Gateway Binding + memory-only RouteTable mapping
중앙 발급·예약 : 없음
```

`DestinationId`는 UUIDv4 형식의 opaque 라우팅 주소다. application이 생성하고 재시작 뒤에도 같은
주소가 필요하면 자신의 config나 저장소에 보관한다. RelayGate는 Destination 생성 API, 영구 registry,
소유권 record와 전역 중복 검사를 제공하지 않는다.

Destination은 live Listener가 있을 때만 BindingSet으로 존재한다. 마지막 Binding이 사라지거나 RT가
재시작하면 mapping도 사라진다. 같은 UUID를 여러 Relay가 listen하면 하나의 Destination에 여러 live
Binding이 있는 N:M 모델로 처리한다. 우연한 충돌과 의도적인 공유를 RelayGate가 구분하지 않는다.

## 결과

- RT state는 현재 live Binding 수에 비례하고 address history가 누적되지 않는다.
- Gateway와 RT 재배포가 application의 stable Destination을 바꾸지 않는다.
- UUID는 추측 난이도를 높일 뿐 인증, 권한과 비밀을 제공하지 않는다.
- 강한 주소 소유권과 중앙 발급이 필요해지면 RelayGate core가 아닌 별도 control plane 결정이 필요하다.

## 참고

- [ADR 004](004-current-state-routing-topology.md)
- [ADR 005](005-soft-state-registration-lifecycle.md)
- [SPEC 003](../spec/003-destination-binding-contract.md)
