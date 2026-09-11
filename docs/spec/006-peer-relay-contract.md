# SPEC 006: Gateway one-hop relay 계약

```text
GW A <===== PeerTransport A->B =====> GW B
         Stream 1 = Pipe 1
         Stream 2 = Pipe 2
         Stream N = Pipe N
```

| ID | 계약 |
| --- | --- |
| `PEER-001` | remote data path는 Entry Gateway → Owner Gateway 한 hop이다. |
| `PEER-002` | RT는 register·resolve control plane에 위치한다. |
| `PEER-003` | ordered Gateway pair의 PeerTransport는 방향별 0..1개다. |
| `PEER-004` | 두 방향 transport는 독립적이고 각각 여러 RelayStream을 multiplex한다. |
| `PEER-005` | StreamId는 Dialer/Acceptor bit와 방향별 monotonic counter로 유일하다. |
| `PEER-006` | stream FIN/CLOSE/RESET cleanup은 해당 stream에 한정된다. |
| `PEER-007` | writer commit failure와 transport loss는 해당 transport의 stream 전체를 terminal cleanup한다. |
| `PEER-008` | empty transport는 idle-retirement deadline에 정상 종료한다. |
| `PEER-009` | active transport heartbeat timeout은 transport와 소속 stream을 종료한다. |
| `PEER-010` | peer OPEN은 exact Destination, Binding과 origin open identity를 current state와 대조한다. |
| `PEER-011` | unknown·late·foreign frame은 current state를 유지하는 terminal/no-op 결과다. |
| `PEER-012` | peer connect, handshake, queue와 frame은 bounded다. |

PeerTransport는 다음 dial에서 재생성할 수 있는 availability optimization입니다. Established stream의
lifecycle은 해당 transport terminal 결과로 끝납니다.

Peer frame은 access token을 포함하지 않습니다. Entry Gateway의 operation authorization 결과는 선택된
Destination과 open identity로 축소되며 Owner Gateway는 current Binding을 다시 대조합니다.
