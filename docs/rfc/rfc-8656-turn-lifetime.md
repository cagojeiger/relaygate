# RFC 8656: Traversal Using Relays around NAT

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc8656.html) |
| 성격 | Standards Track, 2020-02 |
| 목적 | relay resource allocation과 peer packet 교환 |

| resource | lifecycle |
| --- | --- |
| allocation | Allocate/Refresh가 expiry 설정 |
| explicit release | Refresh lifetime 0 |
| permission | allocation과 구별되는 lifetime |
| channel binding | permission과 구별되는 lifetime |

```text
Allocate -> active -> Refresh -> active
                   -> expiry / lifetime 0 -> released
```

NAT traversal, ICE, peer IP와 datagram 절차는 TURN 고유 영역입니다. 다른 soft-state system은 각자의
lifetime과 refresh trigger를 결정합니다.

원문 절: [§3.2](https://www.rfc-editor.org/rfc/rfc8656.html#section-3.2), [§6](https://www.rfc-editor.org/rfc/rfc8656.html#section-6), [§8](https://www.rfc-editor.org/rfc/rfc8656.html#section-8), [§9](https://www.rfc-editor.org/rfc/rfc8656.html#section-9)
