# SPEC 006: Gateway one-hop relay 계약

```text
GW A <===== PeerTransport A->B =====> GW B
         Stream 1 = Pipe 1
         Stream 2 = Pipe 2
         Stream N = Pipe N
```

- **`PEER-001`**: remote data path는 Entry Gateway에서 Owner Gateway까지 최대 one hop이다.
- **`PEER-002`**: RT는 payload와 established Pipe 경로에 참여하지 않는다.
- **`PEER-003`**: ordered Gateway pair는 방향별 PeerTransport를 최대 하나 유지한다.
- **`PEER-004`**: 방향별 두 transport가 존재할 수 있으며 각 transport는 여러 RelayStream을 multiplex한다.
- **`PEER-005`**: StreamId는 initiator bit와 방향별 monotonic counter로 충돌을 막는다.
- **`PEER-006`**: 한 stream의 FIN/CLOSE/RESET은 sibling stream을 보존한다.
- **`PEER-007`**: writer commit 실패나 transport loss는 그 transport의 모든 stream만 terminal cleanup한다.
- **`PEER-008`**: stream이 0인 transport는 idle retirement deadline 뒤 닫는다.
- **`PEER-009`**: idle 중 heartbeat 응답이 없으면 transport와 그 stream을 닫는다.
- **`PEER-010`**: peer OPEN은 Destination, selected Binding과 origin open identity를 current state와 검증한다.
- **`PEER-011`**: unknown/late/foreign frame은 state를 부활시키지 않는다.
- **`PEER-012`**: peer 연결과 handshake, queue와 frame은 모두 bounded다.

PeerTransport는 availability optimization이며 Pipe continuation을 보장하지 않습니다. 다음 dial은 필요하면
새 transport를 만들지만 종료된 stream을 복원하지 않습니다.
