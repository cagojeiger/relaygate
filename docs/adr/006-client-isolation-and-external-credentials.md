# ADR 006: Client 격리와 외부 credential source of truth

## Context

같은 Endpoint를 쓰는 client 사이에는 우회할 수 없는 인증 경계와 하나의 credential source가 필요하다.

## Decision

`ClientId`는 인증으로 정해지는 **엄격하고 암묵적인 namespace**다. Binding, route와 Pipe operation은
그 안에서만 수행하며 cross-client lookup이나 routing은 허용하지 않는다.

Client와 credential의 source of truth는 external client config다. 하나의 `ClientId`는 rotation을 위해
여러 immutable `ApiKeyId`를 가질 수 있다. RelayGate는 이를 database/Raft에 복제하거나 CRUD API로
관리하지 않는다.

Startup은 fail closed하며 `SIGHUP` reload는 전체 검증 뒤 atomic하게 적용한다. 제거된 credential의
session과 연결 상태는 종료한다. 세부 계약은 [SPEC 002](../spec/002-client-configuration-and-presence.md)를
따른다.

## Consequences

- Endpoint, target, binding과 Pipe 같은 client-facing identifier는 인증된 `ClientId` 안에서 해석된다.
- 여러 key로 중단 없는 rotation과 명시적인 권한 회수가 가능하다.
- Client/key lifecycle은 외부 config가 소유한다.
