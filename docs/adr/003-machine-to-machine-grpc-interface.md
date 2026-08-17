# ADR 003: Protocol 경계

## Context

SDK traffic, Gateway 내부 통신, Raft와 운영 관찰은 수명과 trust가 다르다.

## Decision

| 경계 | Protocol | 공개 여부 |
| --- | --- | --- |
| SDK ↔ Gateway | `relay.proto` gRPC/HTTP2 | Public Go/Rust SDK contract |
| Gateway ↔ authority | `control.proto` gRPC/HTTP2 | Internal |
| Ingress ↔ owner Gateway | `gateway.proto` gRPC/HTTP2 | Internal, one stream per remote Pipe |
| Raft voter ↔ voter | HashiCorp Raft TCP transport | Internal implementation |
| 운영 관찰 | Read-only HTTP/JSON | Local/dev observation only |

각 protobuf와 listener는 분리한다. Public SDK에는 control, peer, generated server와 Raft type을 노출하지
않는다. REST는 relay나 client/key mutation을 제공하지 않는다.

## Consequences

- Go/Rust SDK는 하나의 public wire contract만 공유한다.
- Internal protocol은 public compatibility 약속 없이 바꿀 수 있다.
- TLS와 peer authentication은 각 trust boundary에서 별도로 결정한다.
