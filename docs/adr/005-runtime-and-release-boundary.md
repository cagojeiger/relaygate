# ADR 005: 실행 역할과 배포 경계

## 배경

서버 제어 영역의 영속성, 무상태 Relay 용량, 공개 SDK 호환성은 서로 다른 축으로 변한다. 실행 역할은 Raft·제어 내부 구현을 SDK 밖에 두고 Gateway 수평 확장이 Controller quorum을 바꾸지 않게 해야 한다.

## 결정

RelayGate 서버는 하나의 Go 실행 파일·이미지와 두 시작 역할로 배포한다.

| 역할 | 구성 요소 |
| --- | --- |
| `controller` | 영속 HashiCorp Raft 투표자·저장소, 현재 상태 전용 FSM, 제어 권한·서버, 읽기 전용 관리 API |
| `gateway` | 제어 클라이언트, 공개 Relay, 내부 Peer Relay, 인증·세션·바인딩·Pipe 실행 상태, 읽기 전용 관리 API |

`controller`는 public/peer Relay를 실행하지 않는다. `gateway`는 Raft, durable store, control server, authoritative FSM을 열지 않는다. Role은 startup 때 고정되며 reload로 바꿀 수 없다.

Release unit은 다음 세 개다.

1. Root Go module server image
2. `github.com/cagojeiger/relaygate/sdk/go` Go module
3. `relaygate-sdk` Rust crate

Go/Rust SDK는 `proto/relaygate/relay/v1/relay.proto`만 공유한다. Generated server/control/Raft type은 private이다. Root runtime, Go SDK, Go example은 서로 독립된 module이며 repository workspace file 없이 build/test되어야 한다.

## 결과

- 운영 Controller와 Gateway 실행 환경은 Go가 소유한다.
- Controller quorum과 Relay 처리량은 독립적으로 확장된다.
- 공개 SDK는 서버, 제어, Raft 구현에 의존하지 않는다.
- 동일 image를 환경별로 승격하고 deployment가 `controller` 또는 `gateway`를 선택한다.
