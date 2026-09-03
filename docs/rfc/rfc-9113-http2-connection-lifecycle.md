# RFC 9113: HTTP/2

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc9113.html)
- 성격: Standards Track, 2022년 6월

## 범위

HTTP/2는 하나의 connection 위에 여러 독립적인 stream을 multiplex하고, connection과
stream의 상태 및 control frame을 정의한다.

## 핵심

- 각 stream은 identifier와 독립적인 lifecycle을 가진다.
- `PING`은 특정 stream이 아닌 connection 전체에 적용되며 round-trip time과 연결성을 확인한다.
- `PING` acknowledgement는 application payload 처리 성공을 뜻하지 않는다.
- `GOAWAY`는 새 stream 수락을 중단하고 처리했을 수 있는 마지막 stream 범위를 알린다.
- `GOAWAY`를 받은 endpoint는 영향받은 request의 재시도 가능성을 application semantics에 따라 판단한다.
- connection-level control과 stream-level state는 서로 다른 수명과 실패 범위를 가진다.

## 구분할 점

- HTTP/2 frame layout, header compression, priority와 HTTP semantics는 HTTP/2 고유 계약이다.
- `PING`만으로 특정 stream이나 application의 건강 상태를 판단할 수 없다.
- `GOAWAY`는 즉시 connection을 닫는 신호가 아니며 graceful shutdown 절차의 일부다.

## 읽을 절

- [§5 Streams and Multiplexing](https://www.rfc-editor.org/rfc/rfc9113.html#section-5)
- [§6.7 PING](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.7)
- [§6.8 GOAWAY](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.8)
