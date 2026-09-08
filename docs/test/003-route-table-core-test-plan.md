# TEST 003: RouteTable core 검증

| 범주 | 필수 증거 |
| --- | --- |
| directory | exact artifact bytes generation, ordered authority, invalid schema 거절 |
| registration | Register/Update/KeepAlive/Deregister closed lifecycle |
| revision | monotonic, atomic, idempotent, stale lease 격리 |
| expiry | sibling 보존, expired operation no resurrection |
| memory | keepalive 횟수가 아니라 live lease/Binding 수에 비례 |
| restart | empty start 후 새 Gateway snapshot만으로 복구 |
| transport | 인증, frame/queue/connection 상한, shutdown deadline |

Shard 하나의 중단 범위는 해당 authority shard의 remote Resolve입니다. 다른 shard와 Gateway local
Binding은 독립적으로 유지됩니다.
