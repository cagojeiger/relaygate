# ADR 006: Client 격리와 credential

## Context

같은 Endpoint를 사용하는 client 사이에는 우회할 수 없는 namespace와 하나의 credential source가 필요하다.
이 최종 문서는 기존 ADR 006과 ADR 007의 client 격리와 credential 검증 결정을 통합해 대체한다. 이전
결정문은 Git history에 남는다.

## Decision

`ClientId`는 인증으로 결정되는 strict namespace다. Binding, route와 Pipe operation은 그 namespace 안에서만
해석하며 cross-client lookup을 허용하지 않는다.

Client와 API key의 source of truth는 external config다.

- API key는 operator가 생성한 high-entropy bearer secret으로 한정한다.
- 하나의 `ClientId`는 rotation을 위해 여러 immutable `ApiKeyId`를 가질 수 있다.
- Config에는 raw key 대신 `sha256:<64 lowercase hex>` verifier만 둔다.
- Gateway는 exact `(ClientId, ApiKeyId)` verifier를 constant-time으로 비교한다.
- RelayGate는 credential을 database나 Raft에 저장하지 않고 CRUD API도 제공하지 않는다.
- Public `Relay.Connect`의 첫 메시지만 raw key를 포함하며, 이후 identity는 authenticated session에서 얻는다.
- Invalid startup config는 fail closed다. Reload의 invalid candidate는 거부하고 current snapshot은 유지한다.
  Valid candidate만 atomic하게 적용하며 제거된 credential의 session을 종료한다.

Non-loopback Public Relay는 TLS가 제공될 때만 허용한다.

## Consequences

- Client별 route 격리와 중단 없는 key rotation을 함께 지원한다.
- Credential lifecycle은 external config가 소유한다.
- Raw key는 log, 상태, Raft 또는 config에 기록하지 않는다.
