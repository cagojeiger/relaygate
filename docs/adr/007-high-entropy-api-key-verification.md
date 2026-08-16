# ADR 007: High-entropy API key 검증

## Context

External client config는 raw API key를 저장하지 않으면서 여러 key rotation과 exact `ApiKeyId` 인증을
지원해야 한다. Password와 API key의 생성·검증 조건은 다르며 불필요한 credential dependency를 늘리지
않아야 한다.

## Decision

API key는 operator가 충분한 entropy로 생성한 bearer secret으로 한정한다. Config에는 raw key 대신
`sha256:<64 lowercase hex>` verifier만 저장한다. Gateway는 presented key의 SHA-256을 계산해 exact
`(ClientId, ApiKeyId)` verifier와 constant-time 비교한다.

하나의 verifier를 여러 credential에 공유할 수 없다. Process lifetime 중 같은 `ApiKeyId`의 verifier 변경은
reload 전체를 거부하며, process restart를 가로지르는 immutability는 external config management가 소유한다.

Public `Relay.Connect` stream의 첫 메시지만 raw key를 포함할 수 있다. 성공하면 stream lifetime에 묶인
`ClientSessionId`를 만들고 이후 operation의 `ClientId`는 session에서 암묵적으로 결정한다. Raw key는
로그, 상태, Raft와 config에 기록하지 않는다.

첫 인증 메시지는 설정된 짧은 deadline 안에 도착해야 한다. Active client session 수는 process 단위로
제한하며, 인증 전 stream도 gRPC connection별 동시 stream 상한과 deadline으로 유한하게 유지한다.

Bearer transport TLS가 구현되기 전까지 Relay listener는 loopback bind만 허용한다.

## Consequences

- 표준 라이브러리만으로 deterministic verifier와 rotation을 지원한다.
- 낮은 entropy의 password나 사람이 고른 secret에는 이 방식을 사용할 수 없다.
- Network deployment는 TLS contract가 추가되기 전까지 fail closed한다.
- 인증을 보내지 않는 connection은 무기한 runtime 자원을 점유할 수 없다.

## 관련 문서

- [SPEC 002: Client Configuration and Presence](../spec/002-client-configuration-and-presence.md)
- [SPEC 004: State Transition Model](../spec/004-state-transition-model.md)
- [TEST 001: Core Correctness Test Plan](../test/001-core-correctness-test-plan.md)
