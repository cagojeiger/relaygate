# 결정 기록 색인

장기 설계 결정은 제품 경계에서 구체적인 데이터 경로로 내려가는 의존 순서대로 기록한다. 이 색인의 연속 번호를 기준으로 삼으며 이후 번호는 바꾸거나 재사용하지 않는다.

Accepted 결정의 의미를 바꿀 때는 기존 문장을 고치지 않고 새 번호로 기록한다. 대체 관계는 새 문서의 배경 절에 적는다.

## 목록

| 번호 | 제목 | 상태 |
| --- | --- | --- |
| [001](001-relaygate-role-and-responsibility-boundary.md) | RelayGate의 역할 | Accepted |
| [002](002-runtime-and-release-boundary.md) | 실행 역할과 배포 경계 | Accepted |
| [003](003-current-state-cluster-and-recovery.md) | 영속 Controller 집합과 현재 상태 복구 | Accepted |
| [004](004-control-state-authority-split.md) | 제어 상태를 영속 `C`와 리더 로컬 `V`로 나눈다 | Accepted |
| [005](005-leader-driven-expiry.md) | 만료는 확인된 리더가 명령으로 제안한다 | Accepted |
| [006](006-protocol-boundaries.md) | 프로토콜 경계 | Accepted, 전송 보안 항목은 008이 대체 |
| [007](007-client-isolation-and-credentials.md) | 클라이언트 격리와 인증 정보 | Accepted, 전송 보안 항목은 008이 대체 |
| [008](008-public-relay-transport-security.md) | 공개 Relay는 TLS가 구현될 때까지 loopback만 bind한다 | Accepted |
| [009](009-cross-gateway-pipe.md) | Gateway 간 Pipe | Accepted |
| [010](010-payload-delivery-receipts.md) | 종단 간 payload 전달 확인 | Accepted |
| [011](011-sdk-session-supervision.md) | SDK 세션 감독 | Accepted |

## 주제별 찾기

| 주제 | 문서 |
| --- | --- |
| 제품 경계와 하지 않는 것 | 001 |
| 실행 역할과 릴리스 단위 | 002 |
| Raft 집합, 복구, 구성원 변경 | 003 |
| 제어 상태의 두 계층과 합의 비용 | 004 |
| Grace 만료와 결정성 | 005 |
| 프로토콜과 노출 범위 | 006, 008 |
| 인증과 클라이언트 격리 | 007, 008 |
| Gateway 간 Pipe와 연결 공유 | 009 |
| Payload 전달 확인 | 010 |
| SDK 세션과 재연결 | 011 |

동작 계약은 `docs/spec/`, 검증 계획은 `docs/test/`에 있다.
