# RFC 9301: LISP Control Plane

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc9301.html) |
| 성격 | Standards Track, 2022-10 |
| 목적 | EID-to-RLOC mapping register·resolve interface |

| operation | 역할 |
| --- | --- |
| Map-Register | ETR이 EID와 RLOC set 등록 |
| Map-Notify | 요청된 Register 수신 확인 |
| Map-Request | cache miss, reachability, TTL 갱신 조회 |
| Map-Reply | nonce로 request와 연결하고 mapping 전달 |
| Map-Server/Resolver | 한 장치 또는 분리된 service role |

```text
publisher ── Register ──► Mapping System ◄── Request ── resolver
```

Message format, address family와 authentication은 LISP 고유 영역입니다. Internal database architecture,
cache policy와 locator reachability는 구현 system의 별도 결정입니다.

원문 절: [§4](https://www.rfc-editor.org/rfc/rfc9301.html#section-4), [§5.2–5.7](https://www.rfc-editor.org/rfc/rfc9301.html#section-5.2), [§6](https://www.rfc-editor.org/rfc/rfc9301.html#section-6), [§7](https://www.rfc-editor.org/rfc/rfc9301.html#section-7)
