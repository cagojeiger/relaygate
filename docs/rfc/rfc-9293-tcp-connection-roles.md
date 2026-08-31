# RFC 9293: Transmission Control Protocol (TCP)

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc9293.html)
- 성격: Standards Track, 2022년 8월

## 범위

TCP는 두 endpoint 사이의 reliable, in-order, full-duplex byte-stream transport를 정의한다.

## 핵심

- active OPEN은 특정 remote endpoint로 연결 수립을 시작한다.
- passive OPEN은 들어오는 연결 요청을 기다린다.
- 연결이 수립되면 양쪽 모두 데이터를 보내고 받을 수 있다.
- FIN은 한 방향의 data 종료를 나타내며 half-close가 가능하다.
- RST는 연결을 비정상적으로 종료한다.
- receive window는 receiver가 받을 수 있는 byte 범위를 제한한다.
- TCP keepalive는 TCP 구현에 포함될 수 있지만 필수 기능이 아니며 기본적으로 꺼져 있어야 한다.

## 구분할 점

- active/passive는 연결 수립 역할이며 고정된 업무상 client/server 종류를 뜻하지 않는다.
- TCP는 application message boundary를 제공하지 않는다.
- OS file descriptor, `listen` backlog와 language SDK 모양은 이 RFC의 wire contract가 아니다.
- TCP keepalive는 낮은 수준의 optional probe이며 bounded application-level liveness, request timeout 또는 delivery acknowledgement 계약이 아니다.

## 읽을 절

- [§2.2 Key TCP Concepts](https://www.rfc-editor.org/rfc/rfc9293.html#section-2.2)
- [§3.5 Establishing a Connection](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.5)
- [§3.6 Closing a Connection](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.6)
- [§3.8.4 TCP Keep-Alives](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.8.4)
- [§3.9 Interfaces](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.9)
