# ADR 006: 클라이언트 격리와 인증 정보

## 배경

같은 Endpoint를 사용하는 Client도 우회할 수 없는 namespace와 단일 credential source가 필요하다. 이 문서는 과거 ADR 006과 ADR 007의 client isolation 및 credential verification 결정을 통합해 대체한다. 이전 기록은 Git history에 남는다.

## 결정

`ClientId`는 인증으로 결정되는 strict namespace다. Binding, route, Pipe operation은 이 namespace 안에서만 해석하며 cross-client lookup을 허용하지 않는다.

Client와 API key의 source of truth는 external config다.

- API key는 operator가 생성한 high-entropy bearer secret만 허용한다.
- 하나의 `ClientId`는 rotation을 위해 여러 immutable `ApiKeyId`를 가질 수 있다.
- Config는 raw key가 아니라 `sha256:<64 lowercase hex>` verifier만 저장한다.
- Gateway는 exact `(ClientId, ApiKeyId)` verifier를 constant time으로 비교한다.
- RelayGate는 인증 정보를 database나 Raft에 저장하지 않으며 CRUD API도 제공하지 않는다.
- Public `Relay.Connect`의 첫 message만 raw key를 포함한다. 이후 identity는 authenticated session에서 가져온다.
- 잘못된 startup config는 fail closed한다. Reload candidate가 잘못되면 거부하고 현재 snapshot을 유지한다. Valid candidate만 atomic apply하며 제거된 credential의 session은 종료한다.

Non-loopback Public Relay는 TLS가 제공될 때만 허용한다.

## 결과

- Client별 route isolation과 중단 없는 key rotation을 함께 지원한다.
- 인증 정보 수명주기는 외부 설정이 소유한다.
- Raw key를 log, state, Raft, config에 기록하지 않는다.
