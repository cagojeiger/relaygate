# RFC 9299: An Architectural Introduction to LISP

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc9299.html)
- 성격: Informational, 2022년 10월

## 범위

LISP는 endpoint identity와 network locator를 분리하고, identifier-to-locator mapping을
control plane에서 조회하는 overlay architecture를 설명한다.

## 핵심

- EID는 endpoint identity, RLOC는 core에서 도달 가능한 routing locator다.
- Mapping System은 EID를 하나 이상의 RLOC에 연결한다.
- ETR은 mapping을 등록하고 ITR은 필요한 mapping을 조회한다.
- local map-cache는 모든 data packet마다 Mapping System을 조회하지 않게 한다.
- Mapping System은 data plane과 분리되어 독립적으로 확장할 수 있다.
- private deployment에서는 Mapping System을 논리적으로 중앙화할 수도 있다.

## 구분할 점

- LISP는 IP prefix, tunnel router와 packet encapsulation을 사용하는 구체적인 network protocol이다.
- identifier-to-locator 분리가 특정 database, sharding 또는 replication 방식을 정하지 않는다.
- cached mapping은 reachability 자체를 보장하지 않으므로 별도 검증과 갱신이 필요하다.

## 읽을 절

- [§3.2 Overview of the Architecture](https://www.rfc-editor.org/rfc/rfc9299.html#section-3.2)
- [§3.3 Data Plane](https://www.rfc-editor.org/rfc/rfc9299.html#section-3.3)
- [§3.4 Control Plane](https://www.rfc-editor.org/rfc/rfc9299.html#section-3.4)
- [§4.1 Cache Management](https://www.rfc-editor.org/rfc/rfc9299.html#section-4.1)
