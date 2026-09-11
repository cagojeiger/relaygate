# TEST 003: RouteTable core 검증

| 범주 | 필수 증거 |
| --- | --- |
| address | Namespace/DestinationName validation, canonical key, exact Namespace 격리 |
| directory | format version 2, exact artifact bytes generation, RouteAddress authority, invalid schema 거절 |
| registration | Register/Update/KeepAlive/Deregister closed lifecycle |
| revision | monotonic, atomic, idempotent, stale lease 격리 |
| expiry | sibling 보존, expired operation no resurrection |
| memory | keepalive 횟수가 아니라 live lease/Binding 수에 비례 |
| restart | empty start 후 새 Gateway snapshot만으로 복구 |
| transport | owner/generation handshake, frame/queue/connection 상한, shutdown deadline |
| credential boundary | AccessToken·JWT claim은 RT DTO·state에 없음 |

Shard 하나의 중단 범위는 해당 authority shard의 remote exact Resolve입니다. 다른 shard와 Gateway local
Binding은 독립적으로 유지됩니다.
