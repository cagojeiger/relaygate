# ADR 006: 프로토콜 경계

## 배경

SDK traffic, Gateway internal communication, Raft, 운영 관찰은 노출 범위와 신뢰 범위가 서로 다르다.

## 결정

| 경계 | Protocol | 노출 범위 |
| --- | --- | --- |
| SDK ↔ Gateway | `relay.proto` gRPC/HTTP2 | 공개 Go/Rust SDK 계약 |
| Gateway ↔ 권한 주체 | `control.proto` gRPC/HTTP2 | 내부 |
| 진입 ↔ 소유 Gateway | `gateway.proto` gRPC/HTTP2 | 내부 |
| Raft 투표자 ↔ 투표자 | HashiCorp Raft TCP 전송 | 내부 구현 |
| Controller 로컬 구성원 | Unix socket 위 `membership.proto` gRPC | 로컬 운영자 전용 |
| 운영 관찰 | 읽기 전용 HTTP/JSON | 관찰 전용 |

프로토콜 listener와 protobuf 계약은 분리한다. 공개 SDK는 제어, Peer, 생성된 서버, Raft type을 노출하지 않는다. REST는 Relay나 Client/key 변경을 제공하지 않는다. 구성원 변경은 실행 중인 Controller의 Raft data directory에 묶인 권한 제한 Unix socket에서만 받으며 추가 TCP/REST port를 열지 않는다.

Public Relay는 TLS termination 뒤에서 제공한다. Internal control, peer, Raft transport의 mTLS는 application protocol과 분리된 deployment boundary에서 추가한다.

## 결과

- Go/Rust SDK는 public wire contract 하나만 공유한다.
- 내부 프로토콜은 공개 호환성 약속 없이 변경할 수 있다.
- 각 trust boundary의 transport security를 독립적으로 강화할 수 있다.
