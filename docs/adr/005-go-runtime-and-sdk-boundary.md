# ADR 005: Go runtime과 public SDK 경계

## Context

Server 구현과 public client contract가 섞이면 SDK가 runtime/Raft 구현에 종속된다.

## Decision

Production Gateway와 Raft integration은 **Go runtime**으로 구현한다. Process/config lifecycle,
gRPC/REST server, Gateway relay, Raft library/storage/transport도 이 runtime이 소유한다.

Public Go/Rust SDK는 하나의 protobuf schema를 공유하고 각 언어에 자연스러운 API만 노출한다.
Server, generated server와 Raft implementation type은 SDK 경계를 넘지 않는다.

Public protobuf는 relay data path만 정의한다. Raft transport/storage는 호환성 약속 밖이다.

## Consequences

- Server/control plane의 기준 언어는 Go다.
- Go/Rust SDK는 같은 wire semantics를 따른다.
- Server/Raft 구현은 public SDK를 바꾸지 않고 교체할 수 있다.
