# TEST 004: RT 2 / Gateway 3 kind acceptance

## topology

```text
                           ┌── direct Gateway별 진입: topology/fault 격리
host Rust SDK ── TLS/TCP ──┤
                           └── Envoy L4 passthrough ──► Gateway SDK Service

                 GW-0 ═════ GW-1 ═════ GW-2
                   \          |          /
                    RT-0      hash       RT-1
```

| 항목 | 구성 |
| --- | --- |
| cluster | 격리된 name, local image, 일회성 certificate/Secret |
| SDK | host Rust process |
| direct entry | local/one-hop과 fault scope 검증 |
| Envoy entry | external L4 passthrough 검증 |
| evidence | `target/kind-acceptance` artifact |
| cleanup | 실행이 생성한 cluster와 certificate 제거 |

`tests/kind/run.sh`와 GitHub Actions `Kind Acceptance`가 같은 acceptance harness를 실행합니다.

## acceptance

| ID | 시나리오 | 통과 조건 |
| --- | --- | --- |
| `KIND-01` | TLS/admission | 올바른 CA/name/token/ALPN만 연결되고 wrong CA/name/token/ALPN은 state 없이 실패 |
| `KIND-02` | symmetric chat | 각 Relay가 listen과 dial을 함께 수행하고 다중 사용자 1:1 byte 교환 |
| `KIND-03` | local/one-hop | 모든 local 경로와 directed remote Gateway 경로 성공, RT는 payload 경로에 없음 |
| `KIND-04` | N:M | 같은 Destination Binding 여러 개 중 dial마다 하나만 선택, fan-out 없음 |
| `KIND-05` | Gateway restart | old Pipe 종료, SDK reconnect/Listener republish, fresh dial 성공 |
| `KIND-06` | RT shard loss | unavailable shard의 remote dial만 격리되고 local Pipe와 다른 shard는 유지, shard 복귀 뒤 mapping 재수렴 |
| `KIND-07` | cleanup | SDK 종료 뒤 session/binding/attempt/Pipe/peer stream gauge가 baseline 복귀 |
| `KIND-08` | secret | ClusterToken, internal component credential, TLS private key와 payload marker가 로그·metric·error에 없음 |
| `KIND-09` | L4/TLS passthrough | Envoy가 TLS를 종단하지 않고 SDK의 CA/name 검증과 Pipe byte 왕복이 성공 |
| `KIND-10` | RT rolling restart | shard를 하나씩 교체하는 동안 established Pipe가 진행되고 재등록 뒤 모든 route가 복구 |
| `KIND-11` | Gateway rolling restart | Gateway를 하나씩 교체할 때 SDK가 jitter로 재연결·republish하고 fresh dial이 복구 |
| `KIND-12` | reconnect storm | 100개 RelaySession의 동시 단절 뒤 재연결이 bounded하며 최종 Listener와 dial이 복구 |
| `KIND-13` | bounded soak | 최소 60초·64 worker Pipe 왕복에 오류가 없고 종료 뒤 current gauge가 baseline 복귀 |
| `KIND-14` | cert-manager 재발급 | role별 leaf 재발급 → Reloader Pod 교체 → Listener 재등록·fresh dial 복구 |

## stop condition

모든 ID는 command output, pod state, metric snapshot과 log search evidence를 남깁니다. rolling/storm/soak의
완료 조건은 fresh dial 성공과 cleanup baseline 수렴입니다.
