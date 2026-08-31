# RFC 9000: QUIC: A UDP-Based Multiplexed and Secure Transport

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc9000.html)
- 성격: Standards Track, 2021년 5월

## 범위

QUIC은 하나의 secure transport connection 안에서 여러 ordered byte stream을 제공하고
connection과 stream의 수명, flow control과 오류 처리를 정의한다.

## 핵심

- stream은 unidirectional 또는 bidirectional이다.
- stream은 identifier로 구분되며 각 stream은 독립적인 ordered byte sequence를 가진다.
- stream ID의 최하위 두 bit는 initiator와 방향을 나타내고 나머지 bit는 같은 종류 안에서 증가하는 stream number다.
- stream별 flow control은 한 stream의 receive-buffer 독점을 제한한다.
- connection-level flow control은 모든 stream의 총 receive-buffer 사용량을 제한한다.
- stream reset은 해당 방향을 종료하고 다른 방향의 state와 구분된다.
- QUIC transport에서는 한 stream의 loss가 다른 stream의 application delivery를 직접 막지 않는다.
- connection은 negotiated idle timeout을 가질 수 있고, 일정 시간 동안 idle이면 state를 버릴 수 있다.
- endpoint는 peer가 곧 timeout할 수 있을 때 liveness를 확인하기 위해 PING 또는 ack-eliciting frame을 보낼 수 있다.
- PING으로 idle timeout을 미루는 기능은 application protocol이 언제 적절한지 지침을 가져야 한다.

## 구분할 점

- 두 수준의 flow control 개념만 차용해도 TCP transport의 head-of-line blocking은 사라지지 않는다.
- QUIC의 security, loss recovery, congestion control과 connection migration은 하나의 protocol
  전체를 이룬다.
- stream priority와 scheduling policy는 애플리케이션 요구에 따라 별도로 정해야 한다.
- QUIC의 `RESET_STREAM`은 한 송신 방향을 종료하며 connection 전체 종료와 구분된다.
- initiator bit와 direction bit를 일부만 차용하는 protocol은 QUIC stream-ID contract와 다른 자체 규칙을 명시해야 한다.
- idle timeout과 liveness testing은 connection lifecycle 개념이며 application payload 성공을 뜻하지 않는다.

## 읽을 절

- [§2 Streams](https://www.rfc-editor.org/rfc/rfc9000.html#section-2)
- [§3 Stream States](https://www.rfc-editor.org/rfc/rfc9000.html#section-3)
- [§4 Flow Control](https://www.rfc-editor.org/rfc/rfc9000.html#section-4)
- [§4.6 Controlling Concurrency](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.6)
- [§10.1 Idle Timeout](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.1)
- [§10.1.1 Liveness Testing](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.1.1)
- [§10.1.2 Deferring Idle Timeout](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.1.2)
