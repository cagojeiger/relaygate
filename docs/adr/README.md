# 결정 기록 색인

장기 설계 결정은 이 디렉터리에 번호순으로 기록한다. 번호는 한 번 부여하면 바꾸지 않는다. 커밋 메시지와 리뷰에서 번호로 참조하기 때문이다. 결정이 폐기되거나 통합되면 번호를 재사용하지 않고 결번으로 남긴다.

Accepted 결정의 의미를 바꿀 때는 기존 문장을 고치지 않고 새 번호로 기록한다. 대체 관계는 새 문서의 배경 절에 적는다.

## 목록

| 번호 | 제목 | 상태 |
| --- | --- | --- |
| [001](001-relaygate-role-and-responsibility-boundary.md) | RelayGate의 역할 | Accepted |
| [002](002-current-state-cluster-and-recovery.md) | 영속 Controller 집합과 현재 상태 복구 | Accepted |
| [003](003-protocol-boundaries.md) | 프로토콜 경계 | Accepted, 전송 보안 항목은 016이 대체 |
| 004 | — | 결번, 처분 미기록 |
| [005](005-runtime-and-release-boundary.md) | 실행 역할과 배포 경계 | Accepted |
| [006](006-client-isolation-and-credentials.md) | 클라이언트 격리와 인증 정보 | Accepted, 전송 보안 항목은 016이 대체 |
| 007 | — | 결번, 006에 통합 |
| [008](008-cross-gateway-pipe.md) | Gateway 간 Pipe | Accepted |
| 009 | — | 결번, 처분 미기록 |
| 010 | — | 결번, 처분 미기록 |
| [011](011-sdk-session-supervision.md) | SDK 세션 감독 | Accepted |
| 012 | — | 결번, 처분 미기록 |
| [013](013-payload-delivery-receipts.md) | 종단 간 payload 전달 확인 | Accepted |
| [014](014-control-state-authority-split.md) | 제어 상태를 영속 `C`와 리더 로컬 `V`로 나눈다 | Accepted |
| [015](015-leader-driven-expiry.md) | 만료는 확인된 리더가 명령으로 제안한다 | Accepted |
| [016](016-public-relay-transport-security.md) | 공개 Relay는 TLS가 구현될 때까지 loopback만 bind한다 | Accepted |

## 결번

004, 009, 010, 012는 처분 기록이 없다. 007은 006이 통합했다고 006의 배경 절에 기록되어 있다. 처분이 확인되지 않은 번호는 재사용하지 않는다.

## 주제별 찾기

| 주제 | 문서 |
| --- | --- |
| 제품 경계와 하지 않는 것 | 001 |
| Raft 집합, 복구, 구성원 변경 | 002 |
| 제어 상태의 두 계층과 합의 비용 | 014 |
| Grace 만료와 결정성 | 015 |
| 프로토콜과 노출 범위 | 003, 016 |
| 실행 역할과 릴리스 단위 | 005 |
| 인증과 클라이언트 격리 | 006, 016 |
| Gateway 간 Pipe와 연결 공유 | 008 |
| SDK 세션과 재연결 | 011 |
| Payload 전달 확인 | 013 |

동작 계약은 `docs/spec/`, 검증 계획은 `docs/test/`에 있다.
