# 변경 기록

RelayGate의 주요 변경을 이 문서에 기록한다.

## 미배포

- Cross-Gateway Pipe가 exact owner Gateway identity/address별 gRPC/HTTP2 connection을 공유하고 Pipe별 독립 stream을 사용하도록 변경했다.
- 정규 문서의 기본 언어를 한국어로 통일했다.

## 0.1.0 - 2026-08-18

- Go 실행 환경을 영속 `controller`와 무상태 `gateway` 역할로 분리했다.
- 내장 Raft FSM에는 현재 Gateway 세션과 exact route만 저장한다.
- 보호된 Unix socket을 통한 리더 전용 로컬 Raft 구성원 작업을 추가했다.
- 관리형 세션 재연결과 현재 Listener 재바인딩을 지원하는 상한이 있는 공개 Go/Rust SDK를 추가했다.
- Queue, resume, payload replay 없이 same-Gateway/cross-Gateway bidirectional Pipe를 추가했다.
- 세 Controller/두 Gateway Compose 장애, SDK conformance, echo example 근거를 추가했다.
