# RFC 9301: Locator/ID Separation Protocol (LISP) Control Plane

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc9301.html)
- 성격: Standards Track, 2022년 10월

## 범위

LISP control plane은 Map-Resolver와 Map-Server를 통해 EID-to-RLOC mapping을 등록하고
조회하는 service interface를 정의한다.

## 핵심

- ETR은 Map-Register로 EID와 RLOC 집합을 주기적으로 등록한다.
- Map-Notify는 요청된 경우 Map-Register 수신을 확인한다.
- ITR은 cache miss, reachability 확인 또는 TTL 갱신을 위해 Map-Request를 보낸다.
- Map-Reply는 요청과 nonce로 연결되며 mapping cache를 갱신할 수 있다.
- mapping service interface는 router를 내부 mapping database 구현과 분리한다.
- Map-Server와 Map-Resolver 역할은 한 장치에 함께 둘 수 있다.

## 구분할 점

- LISP message format, EID/RLOC address family와 authentication은 LISP 고유 계약이다.
- control-plane interface는 내부 mapping database architecture를 고정하지 않는다.
- mapping cache와 locator reachability는 별도 문제다.

## 읽을 절

- [§4 Basic Overview](https://www.rfc-editor.org/rfc/rfc9301.html#section-4)
- [§5.2–5.7 Mapping Messages](https://www.rfc-editor.org/rfc/rfc9301.html#section-5.2)
- [§6 Changing Mapping Contents](https://www.rfc-editor.org/rfc/rfc9301.html#section-6)
- [§7 Routing Locator Reachability](https://www.rfc-editor.org/rfc/rfc9301.html#section-7)
