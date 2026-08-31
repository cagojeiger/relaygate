# RFC 9113 HTTP/2 connection lifecycle

| 항목 | 값 |
| --- | --- |
| 원문 | https://www.rfc-editor.org/rfc/rfc9113.html |
| RelayGate에서 참고하는 부분 | connection-level control frame과 stream lifecycle 분리 |

## 핵심

HTTP/2는 하나의 connection 위에 여러 stream을 multiplex한다. `PING`은 connection liveness와 round-trip time을 확인하는 control frame이며, 특정 stream의 application 처리 성공을 뜻하지 않는다.

`GOAWAY`는 connection이 새 stream을 받지 않겠다는 신호와 마지막으로 처리한 stream 범위를 전달한다. RelayGate는 현재 단계에서 `GOAWAY`와 같은 graceful drain frame을 정의하지 않는다.

## RelayGate에 적용하는 개념

```text
transport heartbeat
  = SDK-Gateway session 또는 PeerTransport 생존 확인

Pipe / RelayStream state
  = 개별 logical byte stream의 열림, half-close, close, reset
```

RelayGate의 `PING`/`PONG`은 transport-level control frame이다. Pipe idle, payload 응답 부재, application acknowledgement, Listener application health를 의미하지 않는다.

## RelayGate에 적용하지 않는 것

- HTTP/2 frame layout
- stream priority
- header compression
- GOAWAY 기반 graceful drain
- HTTP semantics
