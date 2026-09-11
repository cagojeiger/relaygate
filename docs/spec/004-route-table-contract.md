# SPEC 004: RouteTable 계약

```text
authority = ShardDirectory[sha256(Destination.canonical_key) mod M]

RouteTableShard
  registrations[(GatewayId, RelaySessionId, ShardId)] -> Lease + optional Revision + current BindingSnapshot
  destination_index[Destination]                                -> Set<BindingIdentity>
  bindings[BindingIdentity]                          -> BindingProjection
```

`BindingProjection = Destination + GatewayId + RelaySessionId + BindingId + GatewayLocator`

| ID | 계약 |
| --- | --- |
| `RT-001` | 한 directory generation의 exact Destination authority는 logical shard 하나다. |
| `RT-002` | shard는 Destination partition이며 Namespace 단위 partition이 아니다. |
| `RT-003` | active RegistrationKey가 없으면 Register는 새 LeaseId를 만들고 revision과 BindingSnapshot은 아직 두지 않는다. 동일 active key의 재시도는 current LeaseId·revision·남은 TTL을 반환하며 deadline·BindingSnapshot을 바꾸지 않는다. |
| `RT-004` | 첫 Update는 revision 1의 current full snapshot을 설치하고 이후 Update는 더 높은 revision full snapshot으로 원자 교체한다. |
| `RT-005` | equal revision/equal snapshot은 idempotent하고 conflicting/lower revision은 거절한다. |
| `RT-006` | KeepAlive는 BindingProjection을 유지하고 lease deadline을 연장한다. |
| `RT-007` | Deregister, lease expiry와 process restart는 소유 BindingProjection을 제거한다. |
| `RT-008` | stale LeaseId/revision은 current state를 유지한 채 terminal operation으로 끝난다. |
| `RT-009` | Resolve 응답은 exact Destination의 current live BindingSet으로 한정한다. |
| `RT-010` | RT는 memory-only이며 process start state는 empty다. |
| `RT-011` | RT restart 뒤 Gateway current BindingSnapshot이 BindingProjection을 재구축한다. |
| `RT-012` | RT failure 동안 established Pipe와 local Binding은 유지된다. |
| `RT-013` | current topology는 immutable format-version 2 directory와 shard당 authority endpoint 하나다. |

Directory generation은 exact JSON artifact bytes의 SHA-256입니다. Authority hash는
`sha256-destination-modulo-v2`이고 canonical key는 Namespace와 DestinationName의 길이 및 bytes를 각각 포함합니다.

```text
RT memory = O(live registration + live Binding)
```
