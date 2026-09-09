# TEST 001: requirement와 실행 증거 대응표

| Test ID | Requirement | 검증 |
| --- | --- | --- |
| `T-MODEL-01` | `TERM-001`, `TERM-002`, `TERM-003`, `TERM-004`, `TERM-005`, `TERM-006`, `TERM-007`, `TERM-008`, `TERM-009`, `TERM-010` | 대칭 Relay, UUID identity, N:M Binding과 1:1 Pipe cardinality |
| `T-SDK-01` | `SDK-001`, `SDK-002`, `SDK-003`, `SDK-004`, `SDK-005`, `SDK-006`, `SDK-007` | 초기 연결, heartbeat, reconnect/republish, old Pipe 종료와 no replay |
| `T-SDK-02` | `SDK-008`, `SDK-009`, `SDK-010`, `SDK-011`, `SDK-012`, `SDK-013` | listen/accept/close, bounded incoming queue, 중복 Listener |
| `T-PIPE-01` | `PIPE-001`, `PIPE-002`, `PIPE-003`, `PIPE-004`, `PIPE-005`, `PIPE-006` | full-duplex, FIN/CLOSE/RESET, backpressure와 sibling 격리 |
| `T-BIND-01` | `BIND-001`, `BIND-002`, `BIND-003`, `BIND-004`, `BIND-005`, `BIND-006`, `BIND-007`, `BIND-008`, `BIND-009`, `BIND-010` | application UUID 보존, live-only Binding index와 session cleanup |
| `T-RT-01` | `RT-001`, `RT-002`, `RT-003`, `RT-004`, `RT-005`, `RT-006`, `RT-007`, `RT-008`, `RT-009`, `RT-010`, `RT-011`, `RT-012`, `RT-013` | shard authority, lease/revision, expiry/restart/재수렴, bounded memory |
| `T-DIAL-01` | `DIAL-001`, `DIAL-002`, `DIAL-003`, `DIAL-004`, `DIAL-005`, `DIAL-006`, `DIAL-007`, `DIAL-008`, `DIAL-009`, `DIAL-010`, `DIAL-011`, `DIAL-012` | local/remote dial, self exclusion, 단일 선택, timeout/cancel/observation과 bounded admission |
| `T-PEER-01` | `PEER-001`, `PEER-002`, `PEER-003`, `PEER-004`, `PEER-005`, `PEER-006`, `PEER-007`, `PEER-008`, `PEER-009`, `PEER-010`, `PEER-011`, `PEER-012` | one-hop multiplexing, direction arbitration, heartbeat/idle/terminal cleanup |
| `T-STATE-01` | `STATE-001`, `STATE-002`, `STATE-003`, `STATE-004`, `STATE-005`, `STATE-006`, `STATE-007`, `STATE-008` | terminal no-resurrection, owner-scoped cleanup, RT 독립, admission 격리와 idempotent convergence |
| `T-SEC-01` | `SEC-001`, `SEC-002`, `SEC-003`, `SEC-004`, `SEC-005`, `SEC-006`, `SEC-007`, `SEC-008`, `SEC-009`, `SEC-010`, `SEC-011` | SDK TLS/TCP, server name/ALPN, 내부 mTLS의 Gateway 역할 SAN·신뢰 CA 거절, plaintext 무인증, token, Secret 분리, L4 passthrough, unknown/혼용 mode startup rejection |
| `T-OBS-01` | `OBS-001`, `OBS-002`, `OBS-003`, `OBS-004`, `OBS-005`, `OBS-006`, `OBS-007`, `OBS-008`, `OBS-009`, `OBS-010`, `OBS-011`, `OBS-012`, `OBS-013` | health·RED/USE·latency·cleanup·redaction; PromQL과 DATA probe 증거는 [TEST 006](006-local-observability-test-plan.md) |
| `T-SEC-02` | `SEC-012`, `SEC-013` | TLS 전 handshake 상한, HELLO buffer 한도, stalled read/write deadline, slot 회수, pipelined frame 보존, 기존 session 격리 |
| `T-SEC-03` | `SEC-014` | burst·fractional refill·유휴 상한, 혼합 시간 간격의 구간별 rate 상한, clone 간 예산 공유, TLS 전 rate 거절·기존 session 유지·회복, env 검증과 거절 metric |
| `T-SEC-04` | `SEC-015`, `STATE-008` | PUBLISH/DIAL 예산 소비·비환급, session 격리·GW 한도, RT 전 거절·ID fence, 종료·취소·늦은 ACCEPT 순서, 거절 응답 bounded 대기·deadline·취소·closed·sibling 격리, 작은 transport/queue의 512 DIAL burst 회수, SDK 회복과 probe 재시도 |

모든 requirement는 위 표의 그룹 하나와 [실행 증거 인덱스](001-executable-coverage.toml)에 연결됩니다.
인덱스는 Rust test 존재를, [TEST 004](004-rt2-gw3-closed-loop-test-plan.md)는 L4 passthrough와
rolling/fault runtime acceptance를 검증합니다. Rust test 존재율은 전체 관측 계약의 의미 검증률이 아니며
PromQL fixture와 Compose DATA probe의 성공 증거를 함께 확인합니다.

## Admission 검증 경계

| 계층 | 검증 내용 | 증거 범위 |
| --- | --- | --- |
| TokenBucket unit | 12개 rate/burst 조합 × 240개 시각에서 관측 비소비·burst 상한·구간별 허용량 | 결정적 혼합 스케줄 |
| GatewayState unit | session 거절 시 GW 예산 보존, GW 거절·후속 실패 시 비환급, 중복 PUBLISH·DIAL과 drain | 주입한 동일 시각·refill 경계 |
| GatewayState unit | 거절·CANCEL·session 종료·늦은 ACCEPT의 24개 순서 × PUBLISH/DIAL | 직렬 상태 전이 48개 조합, sibling Pipe 양방향 전송·최종 cleanup |
| Metric unit | PUBLISH/DIAL × session/GW 거절 counter와 결과 code | 4개 저 cardinality series의 정확한 증가량 |
| Probe unit | 재시도 분류, 단일 deadline, 대기·진행 중 작업의 취소와 해제 | 가상 시간, background retry 없음; public SDK 자동 retry 계약과 구분 |
| SDK–GW integration | 실제 오류 반환·Pipe 보존·refill 회복·재연결 후 republish | 소켓을 사용하는 기존 통합 테스트 |
| Kind 인증서 probe | 재발급 후 새 serial 제공까지 제한 횟수로 확인, TLS 검증 실패·이전 serial·연결 실패의 최종 거절 | Hygiene의 mock 회귀 테스트 6개와 cert-manager Kind 실검증 |

사건 순서 검증은 위 네 사건의 직렬 조합을 대상으로 합니다. 네트워크·executor 전체의 모든
스케줄이나 무제한 공격 트래픽에 대한 증명 범위와는 구분합니다.
