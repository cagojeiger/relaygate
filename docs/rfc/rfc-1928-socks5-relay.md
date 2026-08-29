# RFC 1928: SOCKS Protocol Version 5

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc1928.html)
- 성격: Standards Track, 1996년 3월

## 범위

SOCKS5는 애플리케이션이 firewall을 경유해 TCP 또는 UDP 통신을 수립하도록 relay 절차를
정의한다. 애플리케이션 계층과 transport 계층 사이의 shim으로 동작한다.

## 핵심

- TCP client는 먼저 SOCKS server와 연결하고 인증 방법을 협상한다.
- relay request는 command와 destination address/port를 포함한다.
- server는 request를 평가해 success 또는 구체적인 failure를 반환한다.
- CONNECT가 성공한 뒤 양방향 데이터 전달을 시작한다.
- BIND와 UDP ASSOCIATE는 CONNECT와 다른 수명 및 응답 절차를 가진다.

## 구분할 점

- SOCKS5는 IP network-layer forwarding이나 ICMP gateway를 정의하지 않는다.
- 인증 협상, IP/port address 형식, BIND와 UDP relay는 SOCKS 고유 계약이다.
- CONNECT success는 이후 애플리케이션 payload 처리 결과를 보장하지 않는다.

## 읽을 절

- [§1 Introduction](https://www.rfc-editor.org/rfc/rfc1928.html#section-1)
- [§3 Procedure for TCP-based clients](https://www.rfc-editor.org/rfc/rfc1928.html#section-3)
- [§4 Requests](https://www.rfc-editor.org/rfc/rfc1928.html#section-4)
- [§6 Replies](https://www.rfc-editor.org/rfc/rfc1928.html#section-6)
