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

kind에는 RT와 Gateway만 배포하고 SDK는 host에서 실행합니다. 격리된 cluster name, local image와
일회성 certificate/Secret을 사용하며 종료 시 해당 cluster만 삭제합니다. Gateway별 직접 진입은
local/one-hop과 장애 범위를 결정적으로 검증하고, Envoy 경로는 외부 L4 passthrough만 별도로 검증합니다.

`tests/kind/run.sh`가 아래 acceptance를 한 번에 실행합니다. GitHub Actions의 `Kind Acceptance`는
runtime 관련 PR과 수동 실행에서 같은 harness를 사용하고, 성공 여부와 무관하게
`target/kind-acceptance` 증거를 업로드합니다.

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

## stop condition

모든 ID에 command output, pod state, metric snapshot과 로그 검색 결과가 있어야 합니다. rolling/storm/soak
뒤에는 새 dial과 cleanup baseline을 함께 확인합니다. Helm render 성공, pod Ready 또는 단일 echo만으로
완료하지 않습니다.
