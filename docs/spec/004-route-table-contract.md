# SPEC 004: RouteTable 계약

```text
authority = ShardDirectory[sha256(RouteAddress.canonical_key) mod M]

RouteTableShard
  registrations[(GatewayId, RelaySessionId, ShardId)] -> Lease + Revision + full MappingSet
  routes[RouteAddress]                                -> Set<MappingIdentity>
  mappings[MappingIdentity]                           -> MappingEntry
```

`MappingEntry = RouteAddress + GatewayId + RelaySessionId + BindingId + GatewayLocator`

| ID | 계약 |
| --- | --- |
| `RT-001` | 한 directory generation의 exact RouteAddress authority는 logical shard 하나다. |
| `RT-002` | shard는 RouteAddress partition이며 Namespace 단위 partition이 아니다. |
| `RT-003` | Register는 새 LeaseId, revision 1과 full snapshot을 설치한다. |
| `RT-004` | Update는 active lease의 더 높은 revision full snapshot으로 원자 교체한다. |
| `RT-005` | equal revision/equal snapshot은 idempotent하고 conflicting/lower revision은 거절한다. |
| `RT-006` | KeepAlive는 mapping을 유지하고 lease deadline을 연장한다. |
| `RT-007` | Deregister, lease expiry와 process restart는 소유 mapping을 제거한다. |
| `RT-008` | stale LeaseId/revision은 current state를 유지한 채 terminal operation으로 끝난다. |
| `RT-009` | Resolve 응답은 exact RouteAddress의 current live MappingSet으로 한정한다. |
| `RT-010` | RT는 memory-only이며 process start state는 empty다. |
| `RT-011` | RT restart 뒤 Gateway current Binding snapshot이 mapping을 재구축한다. |
| `RT-012` | RT failure 동안 established Pipe와 local Binding은 유지된다. |
| `RT-013` | current topology는 immutable format-version 2 directory와 shard당 authority endpoint 하나다. |

Directory generation은 exact JSON artifact bytes의 SHA-256입니다. Authority hash는
`sha256-route-address-modulo-v2`이고 canonical key는 namespace와 destination의 길이 및 bytes를 각각 포함합니다.

```text
RT memory = O(live registration + live Binding)
```
