# SPEC 003: Destination과 Binding 계약

```text
RelaySession X
  ├── Destination A ── Binding AX
  └── Destination B ── Binding BX

Destination A
  ├── Binding AX -> GW X / Session X
  └── Binding AY -> GW Y / Session Y
```

- **`BIND-001`**: `DestinationId`는 UUIDv4만 허용한다.
- **`BIND-002`**: application이 Destination을 만들고 stable 사용이 필요하면 직접 보관한다.
- **`BIND-003`**: RelayGate는 Destination 중앙 발급, 예약, 소유권과 durable history를 제공하지 않는다.
- **`BIND-004`**: 같은 RelaySession의 같은 Destination publish는 같은 current Binding을 가리킨다.
- **`BIND-005`**: Binding 제거 뒤 재등록은 새 BindingId를 만든다.
- **`BIND-006`**: 서로 다른 RelaySession의 같은 Destination Binding은 동시에 존재할 수 있다.
- **`BIND-007`**: session 종료는 그 session의 모든 Binding을 원자적으로 local registry에서 제거한다.
- **`BIND-008`**: Listener close는 해당 Binding만 제거하고 sibling Destination/Binding을 보존한다.
- **`BIND-009`**: Gateway drain 중에는 새 publish와 session을 받지 않는다.
- **`BIND-010`**: local Binding은 RT 동기화가 지연되어도 local dial의 truth다.

Gateway는 session별 current Binding 전체를 shard별 snapshot으로 투영합니다. 빈 snapshot은 active lease를
명시적으로 종료하며, RT 오류 때문에 local Binding을 삭제하지 않습니다.
