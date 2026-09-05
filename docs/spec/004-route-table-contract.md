# SPEC 004: RouteTable 계약

## 자료 구조

```text
authority = ShardDirectory[hash(DestinationId)]

RouteTableShard
  registrations[(GatewayId, RelaySessionId)] -> Lease + Revision + full MappingSet
  routes[DestinationId]                      -> Set<MappingIdentity>
  mappings[MappingIdentity]                  -> MappingEntry
```

`MappingEntry`는 DestinationId, GatewayId, RelaySessionId, BindingId와 GatewayLocator만 포함합니다.

- **`RT-001`**: 한 ShardDirectory generation에서 Destination authority는 정확히 한 logical shard다.
- **`RT-002`**: shard는 partition이며 replica가 아니다.
- **`RT-003`**: Register는 RT가 새 LeaseId를 발급하고 revision 1의 full snapshot을 설치한다.
- **`RT-004`**: Update는 같은 active lease의 더 높은 revision full snapshot으로 원자 교체한다.
- **`RT-005`**: equal revision/equal snapshot은 idempotent하고 conflicting/lower revision은 거절한다.
- **`RT-006`**: KeepAlive는 mapping을 바꾸지 않고 lease deadline만 연장한다.
- **`RT-007`**: Deregister, lease expiry와 process restart는 해당 mapping을 제거한다.
- **`RT-008`**: stale LeaseId/revision은 새 state를 만들거나 current state를 제거하지 못한다.
- **`RT-009`**: Resolve는 현재 live MappingSet만 반환하며 history와 cache를 반환하지 않는다.
- **`RT-010`**: RT는 memory-only이고 시작 시 항상 비어 있다.
- **`RT-011`**: RT 재시작 뒤 Gateway가 current Binding snapshot을 다시 등록해 복구한다.
- **`RT-012`**: RT 장애는 established Pipe와 local Binding을 종료하지 않는다.
- **`RT-013`**: online shard resize, replica, quorum과 consensus는 제공하지 않는다.

상태 크기는 live registration과 Binding 수에 비례해야 하며 종료된 identity tombstone을 무한히
축적하지 않습니다.
