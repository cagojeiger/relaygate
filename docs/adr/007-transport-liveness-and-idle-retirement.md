# ADR 007: Transport liveness와 idle retirement를 분리한다

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 전제 | [ADR 001](001-relayed-pipe-responsibility-boundary.md), [ADR 006](006-one-hop-peer-multiplexing.md) |

## 맥락

RelayGate는 오래 살아 있는 SDK-Gateway session과 Gateway 간 `PeerTransport` 위에 여러 Pipe를 올린다. 아무 frame도 오가지 않는 silent network failure를 operation deadline에서만 발견하면, 죽은 transport가 current state에 오래 남을 수 있다.

반대로 application `Pipe.read()`가 조용하다는 이유만으로 Pipe나 session을 닫으면 opaque byte stream 계약을 깨뜨린다.

## 결정

```text
SDK-Gateway session
  └── activity-aware Ping/Pong
        timeout -> whole session close

PeerTransport(stream_count > 0)
  └── activity-aware Ping/Pong
        timeout -> PeerTransport close

PeerTransport(stream_count == 0)
  └── no keepalive
        idle timeout -> PeerTransport retire
```

Heartbeat는 transport liveness만 확인한다. Pipe health, application health, payload 처리 성공, delivery acknowledgement를 뜻하지 않는다.

Heartbeat timer는 valid inbound transport activity가 있으면 `PING` 전송 전에는 연장될 수 있다. idle interval 동안 inbound activity가 없을 때 `PING`을 보내고, 그 `PING`이 commit된 뒤 configured response deadline 전에 matching `PONG`을 받지 못하면 해당 transport를 닫는다. unrelated inbound frame, outbound write, nonce가 다른 `PONG`, deadline 이후의 늦은 `PONG`은 이미 commit된 probe를 만족시키지 않는다.

`PeerTransport`는 live `RelayStream`이 하나 이상 있을 때만 heartbeat 대상이다. stream 수가 0이 되면 keepalive를 중단하고 idle-retirement timer를 시작한다. 새 stream이 같은 transport를 재사용하면 retirement timer는 취소된다. timeout까지 재사용되지 않으면 transport를 정상 종료한다.

RT registration의 `KeepAlive`는 RouteTable soft state lease 갱신이다. SDK-Gateway 또는 peer transport heartbeat와 같은 계약이 아니다.

## 결과

- silent failure는 bounded timeout 안에 session 또는 active PeerTransport cleanup으로 수렴한다.
- idle Pipe read는 실패 조건이 아니다.
- 빈 PeerTransport는 즉시 닫지 않고 재사용 기회를 갖지만, 무기한 유지하지 않는다.
- Gateway pair의 eager full mesh나 RT sharding 변경은 요구하지 않는다.
- heartbeat 실패는 기존 Pipe나 payload를 replay, reroute, resume하지 않는다.

## 이 ADR에서 정하지 않는 것

- heartbeat interval, timeout, idle-retirement timeout의 기본값
- Ping/Pong frame encoding과 nonce 크기
- TLS, mTLS, service mesh 구현
- application-level keepalive, request acknowledgement, delivery acknowledgement
- RT shard 수, replication, online shard reconfiguration

## 참고

- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [RFC 4254](../rfc/rfc-4254-ssh-channel.md)
- [RFC 9000](../rfc/rfc-9000-quic-streams.md)
- [RFC 9113](../rfc/rfc-9113-http2-connection-lifecycle.md)
