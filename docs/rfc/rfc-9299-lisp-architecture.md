# RFC 9299: LISP architecture

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc9299.html) |
| 성격 | Informational, 2022-10 |
| 목적 | endpoint identifier와 routing locator 분리 |

```text
EID ── Mapping System ──► RLOC 0..N
```

| 개념 | 의미 |
| --- | --- |
| EID | endpoint identity |
| RLOC | core에서 도달 가능한 routing locator |
| ETR | mapping 등록 |
| ITR | mapping 조회 |
| map-cache | data path의 반복 lookup 감소 |
| Mapping System | data plane과 독립 확장 |

IP prefix, tunnel router와 packet encapsulation은 LISP 고유 영역입니다. Database, sharding, replication과
locator reachability verification은 mapping architecture를 구현하는 system이 결정합니다.

원문 절: [§3.2](https://www.rfc-editor.org/rfc/rfc9299.html#section-3.2), [§3.3](https://www.rfc-editor.org/rfc/rfc9299.html#section-3.3), [§3.4](https://www.rfc-editor.org/rfc/rfc9299.html#section-3.4), [§4.1](https://www.rfc-editor.org/rfc/rfc9299.html#section-4.1)
