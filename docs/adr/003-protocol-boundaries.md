# ADR 003: Protocol 경계

## Context

SDK traffic, Gateway 내부 통신, Raft와 운영 관찰은 공개 범위와 trust가 다르다.

## Decision

| 경계 | Protocol | 공개 범위 |
| --- | --- | --- |
| SDK ↔ Gateway | `relay.proto` gRPC/HTTP2 | Public Go/Rust SDK contract |
| Gateway ↔ authority | `control.proto` gRPC/HTTP2 | Internal |
| Ingress ↔ owner Gateway | `gateway.proto` gRPC/HTTP2 | Internal |
| Raft voter ↔ voter | HashiCorp Raft TCP transport | Internal implementation |
| Controller-local membership | `membership.proto` gRPC over Unix socket | Local operator only |
| 운영 관찰 | Read-only HTTP/JSON | Observation only |

각 listener와 protobuf는 분리한다. Public SDK에는 control, peer, generated server와 Raft type을 노출하지
않는다. REST는 relay나 client/key mutation을 제공하지 않는다. 멤버십 변경은 라이브 controller의
Raft 데이터 디렉터리에 묶인 권한 제한 Unix socket에서만 받으며 추가 TCP/REST 포트를 열지 않는다.

Public Relay는 TLS termination 뒤에서 제공한다. Internal control, peer와 Raft transport의 mTLS는 deployment
boundary에서 추가하며 application protocol과 분리한다.

## Consequences

- Go/Rust SDK는 하나의 public wire contract만 공유한다.
- Internal protocol은 public compatibility 약속 없이 바꿀 수 있다.
- Transport 보안은 각 trust boundary에서 독립적으로 강화할 수 있다.
