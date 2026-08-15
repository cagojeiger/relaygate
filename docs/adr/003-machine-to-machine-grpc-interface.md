# ADR 003: Machine-to-machine gRPC 인터페이스

## Context

RelayGate는 CLI, daemon과 backend service를 위한 장기 양방향 연결이다. Relay와 운영 관찰은 서로
다른 계약과 권한이 필요하다.

## Decision

Public relay interface는 **protobuf 기반 gRPC over HTTP/2**다. SDK-to-Gateway와
Gateway-to-Gateway relay가 같은 data-plane contract를 사용하며 Go/Rust SDK는 하나의 schema를
공유한다.

Raft transport는 구현 기술과 무관하게 public relay/SDK 호환성 범위 밖의 내부 protocol이다.

REST는 **read-only observation API**로만 제공한다. Relay, payload, client/key CRUD는 허용하지
않으며 browser relay가 필요하면 application backend가 SDK adapter가 된다.

## Consequences

- Go/Rust SDK가 하나의 wire contract를 공유한다.
- gRPC가 streaming, deadline과 cancellation을 담당한다.
- REST와 Raft transport는 public relay contract를 확장하지 않는다.
