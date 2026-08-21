# ADR 003: Protocol 경계

## 배경

SDK traffic, Gateway internal communication, Raft, 운영 관찰은 노출 범위와 신뢰 범위가 서로 다르다.

## 결정

| 경계 | Protocol | 노출 범위 |
| --- | --- | --- |
| SDK ↔ Gateway | `relay.proto` gRPC/HTTP2 | Public Go/Rust SDK contract |
| Gateway ↔ authority | `control.proto` gRPC/HTTP2 | Internal |
| Ingress ↔ owner Gateway | `gateway.proto` gRPC/HTTP2 | Internal |
| Raft voter ↔ voter | HashiCorp Raft TCP transport | Internal implementation |
| Controller-local membership | Unix socket 위 `membership.proto` gRPC | Local operator only |
| 운영 관찰 | Read-only HTTP/JSON | Observation only |

Protocol listener와 protobuf contract는 분리한다. Public SDK는 control, peer, generated server, Raft type을 노출하지 않는다. REST는 relay나 client/key mutation을 제공하지 않는다. Membership 변경은 live Controller의 Raft data directory에 묶인 permission-restricted Unix socket에서만 받으며 추가 TCP/REST port를 열지 않는다.

Public Relay는 TLS termination 뒤에서 제공한다. Internal control, peer, Raft transport의 mTLS는 application protocol과 분리된 deployment boundary에서 추가한다.

## 결과

- Go/Rust SDK는 public wire contract 하나만 공유한다.
- Internal protocol은 public compatibility promise 없이 변경할 수 있다.
- 각 trust boundary의 transport security를 독립적으로 강화할 수 있다.
