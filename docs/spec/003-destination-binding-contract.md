# SPEC 003: Destination과 Binding 계약

```text
RelaySession X -- alpha/service.a -- Binding AX
               `- alpha/service.b -- Binding BX

alpha/service.a
  |-- Binding AX -> GW X / Session X
  `-- Binding AY -> GW Y / Session Y
```

| ID | 계약 |
| --- | --- |
| `BIND-001` | Destination은 canonical `Namespace/DestinationName` 형식이다. |
| `BIND-002` | Namespace는 lowercase DNS-like label 하나이고 DestinationName은 dot-separated lowercase DNS-like labels다. |
| `BIND-003` | application이 Destination을 생성·보관하며 RelayGate는 exact Destination의 live location만 관리한다. |
| `BIND-004` | 같은 RelaySession의 동일 Destination publish는 current Binding 하나로 수렴한다. |
| `BIND-005` | Binding 제거 뒤 재등록은 새 BindingId를 만든다. |
| `BIND-006` | 서로 다른 RelaySession의 동일 Destination Binding은 동시에 존재할 수 있다. |
| `BIND-007` | session 종료는 그 session의 Binding 전체를 local registry에서 원자 제거한다. |
| `BIND-008` | Listener close는 해당 Binding을 제거하고 sibling state를 유지한다. |
| `BIND-009` | Gateway drain 상태의 신규 publish와 session은 `UNAVAILABLE`로 끝난다. |
| `BIND-010` | local Binding은 RT sync와 독립된 local dial truth다. |

Gateway는 session별 current Binding을 shard별 full snapshot으로 투영합니다. 빈 snapshot은 active lease를
종료하고 RT dependency failure 동안 local Binding을 유지합니다.

PUBLISH는 제어 요청 예산과 [operation authorization](009-operation-jwt-authorization-contract.md)을
통과한 뒤 registry를 변경합니다. 거절은 current Binding을 변경하지 않습니다. UNPUBLISH와 session cleanup은
새 access token을 요구하지 않으며 live session·Binding 소유권으로 제한됩니다. AccessToken 만료는 이미 생성된
Binding의 lifetime을 변경하지 않습니다.
