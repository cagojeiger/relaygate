# 변경 기록

RelayGate의 주요 변경을 이 문서에 기록한다.

## 미배포

- Cross-Gateway Pipe가 exact owner Gateway identity/address별 gRPC/HTTP2 connection을 공유하고 Pipe별 독립 stream을 사용하도록 변경했다.
- Canonical 문서의 기본 언어를 한국어로 통일했다.

## 0.1.0 - 2026-08-18

- Go runtime을 durable `controller`와 stateless `gateway` role로 분리했다.
- Embedded Raft FSM에는 current Gateway session과 exact route만 저장한다.
- Protected Unix socket을 통한 leader-only local Raft membership operation을 추가했다.
- Managed session reconnect와 current Listener rebind를 지원하는 bounded public Go/Rust SDK를 추가했다.
- Queue, resume, payload replay 없이 same-Gateway/cross-Gateway bidirectional Pipe를 추가했다.
- 세 Controller/두 Gateway Compose 장애, SDK conformance, echo example 근거를 추가했다.
