# ADR 011: 정적 ClusterToken으로 SDK 세션 admission만 제한한다

| 항목 | 값 |
| --- | --- |
| 상태 | 채택, 구현됨 |
| 변경 대상 | ADR 002의 Destination별 ClientKey |

## 결정

```text
ClusterTokenSet = current 1개 + next 0..1개
RelayGate       = Session admission + admitted Session의 listen/dial
Application     = Pipe 상대 인증·인가 + payload 보호·해석
```

Gateway는 시작 config에서 `current`와 선택적인 `next` ClusterToken을 읽는다. SDK application은
ClusterToken 하나를 `RelayConfig`에 명시적으로 넣는다. TLS가 성립한 뒤 0.2 `HELLO`에서 토큰을
제시하며, 둘 중 어느 값과도 일치하지 않으면 Gateway는 `UNAUTHENTICATED`로 연결을 닫고
SessionId, Binding과 Pipe를 만들지 않는다.

ClusterToken은 SDK가 같은 trust domain에 속한다는 admission bearer secret이다. 사용자나 장비의
신원, Destination 소유권 또는 Destination별 권한을 뜻하지 않는다. 승인된 Session은 모든
Destination에 listen/dial할 수 있다. `AccessKeyId`, Destination별 `ClientKey`, `PublishKey`, password와
사전 Destination 허용 목록은 두지 않는다.

SDK는 process 환경을 암묵적으로 읽지 않는다. application이 자신의 config source에서 token을 읽어
`RelayConfig`에 전달한다. Gateway 배포는 기존 Kubernetes Secret을 process config에 연결한다.

회전은 Gateway가 current/next를 함께 허용하고 SDK를 next로 옮긴 뒤 next를 current로 승격하는
겹침 방식이다. config는 process 시작 시 고정하며 hot reload와 이미 승인된 Session의 즉시 폐기는
제공하지 않는다.

SDK와 Gateway는 token 값을 Debug, 로그, metric과 error에 포함하지 않고 RT, Binding과 Pipe state에
저장하지 않는다. 비교는 길이 정보까지 포함해 timing-safe 방식으로 수행한다.

## 결과

- Destination은 공개 라우팅 주소이고 비밀이나 소유권 증명이 아니다.
- token 유출은 trust domain admission 유출이며 Destination 하나의 유출로 축소되지 않는다.
- Pipe 상대 인증과 민감한 payload의 E2E 보호는 application protocol이 수행한다.
- 동적 발급, 외부 검증 서비스, principal과 Destination ACL은 0.2 범위가 아니다.
- admission 이후에도 session, Listener, attempt, Pipe와 buffer에는 유한한 상한을 둔다.

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [ADR 002](002-application-protocol-boundary.md)
- [SPEC 008](../spec/008-runtime-observability-contract.md)
