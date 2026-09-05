# TEST 001: requirement와 실행 증거 대응표

| Test ID | Requirement | 검증 |
| --- | --- | --- |
| `T-MODEL-01` | `TERM-001`, `TERM-002`, `TERM-003`, `TERM-004`, `TERM-005`, `TERM-006`, `TERM-007`, `TERM-008`, `TERM-009`, `TERM-010` | 대칭 Relay, UUID identity, N:M Binding과 1:1 Pipe cardinality |
| `T-SDK-01` | `SDK-001`, `SDK-002`, `SDK-003`, `SDK-004`, `SDK-005`, `SDK-006`, `SDK-007` | 초기 연결, heartbeat, reconnect/republish, old Pipe 종료와 no replay |
| `T-SDK-02` | `SDK-008`, `SDK-009`, `SDK-010`, `SDK-011`, `SDK-012`, `SDK-013` | listen/accept/close, bounded incoming queue, 중복 Listener |
| `T-PIPE-01` | `PIPE-001`, `PIPE-002`, `PIPE-003`, `PIPE-004`, `PIPE-005`, `PIPE-006` | full-duplex, FIN/CLOSE/RESET, backpressure와 sibling 격리 |
| `T-BIND-01` | `BIND-001`, `BIND-002`, `BIND-003`, `BIND-004`, `BIND-005`, `BIND-006`, `BIND-007`, `BIND-008`, `BIND-009`, `BIND-010` | application UUID 보존, live-only Binding index와 session cleanup |
| `T-RT-01` | `RT-001`, `RT-002`, `RT-003`, `RT-004`, `RT-005`, `RT-006`, `RT-007`, `RT-008`, `RT-009`, `RT-010`, `RT-011`, `RT-012`, `RT-013` | shard authority, lease/revision, expiry/restart/재수렴, bounded memory |
| `T-DIAL-01` | `DIAL-001`, `DIAL-002`, `DIAL-003`, `DIAL-004`, `DIAL-005`, `DIAL-006`, `DIAL-007`, `DIAL-008`, `DIAL-009`, `DIAL-010` | local/remote dial, self exclusion, 단일 선택, timeout/cancel/observation |
| `T-PEER-01` | `PEER-001`, `PEER-002`, `PEER-003`, `PEER-004`, `PEER-005`, `PEER-006`, `PEER-007`, `PEER-008`, `PEER-009`, `PEER-010`, `PEER-011`, `PEER-012` | one-hop multiplexing, direction arbitration, heartbeat/idle/terminal cleanup |
| `T-STATE-01` | `STATE-001`, `STATE-002`, `STATE-003`, `STATE-004`, `STATE-005`, `STATE-006` | terminal no-resurrection, owner-scoped cleanup, RT 독립과 idempotent convergence |
| `T-SEC-01` | `SEC-001`, `SEC-002`, `SEC-003`, `SEC-004`, `SEC-005`, `SEC-006`, `SEC-007`, `SEC-008`, `SEC-009`, `SEC-010`, `SEC-011` | 명시적 TLS/TCP, TLS server name, mTLS, token, Secret 분리와 L4 passthrough |
| `T-OBS-01` | `OBS-001`, `OBS-002`, `OBS-003`, `OBS-004` | RED/USE metric, cleanup baseline과 secret 비노출 |

모든 in-scope requirement는 위 표와 [실행 증거 인덱스](001-executable-coverage.toml)에 정확히 한
그룹으로 연결됩니다. Helm/kind acceptance는 Rust test 이름이 아닌 외부 runtime 증거이므로
[TEST 004](004-rt2-gw3-closed-loop-test-plan.md)에서 별도로 닫습니다.
