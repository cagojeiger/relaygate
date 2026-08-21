# ADR 013: End-to-End payload delivery receipt

## 배경

이 결정 전의 `Pipe.Send`는 bounded local transport handoff만 증명했다. Sender가 relay hop 뒤 retry 여부를 결정하기에는 부족했다. Payload가 peer SDK에 도달했는지 peer receive queue 전에 멈췄는지 구분할 수 없었기 때문이다.

RelayGate는 durable broker가 되어서는 안 된다. Application processing, durable message storage, replay, exactly-once effect는 소유하지 않는다.

## 결정

- 모든 public/peer `PipePayload`는 `PipeId`와 direction 범위의 exact `PayloadId`를 가진다.
- Receiving SDK는 exact payload가 bounded receive queue에 들어간 뒤에만 `PipePayloadReceived`를 보낸다. 이 queue admission이 payload delivery linearization point다.
- Sender는 exact receipt를 관찰한 뒤에만 성공을 반환한다.
- Local authenticated-stream handoff 이전 실패는 `NotSent`, exact remote refusal은 `Rejected`다. Handoff 이후 receipt 관찰 전 timeout, Pipe/session loss, transport loss는 `Unknown`이다.
- Receipt와 rejection은 exact `PipeId`와 `PayloadId`를 가진다. Unknown, malformed, foreign, duplicate-conflicting, wrong-phase correlation은 protocol-fatal이다.
- `Unknown`은 caller-visible absorbing outcome이다. 늦은 exact receipt/rejection은 bounded NoOp이며 이미 반환한 결과를 바꾸지 않는다.
- Sender pending state와 receiver receipt history는 SDK/Pipe runtime의 bounded process memory다. Controller Raft에 저장하거나 Pipe/session/process 종료 뒤 resume하지 않는다.
- RelayGate는 payload를 자동 retry/replay하지 않는다. 새 Pipe에서 `Unknown` delivery를 retry하는 application은 자체 stable message identity와 idempotent processing contract를 가져야 한다.

## 결과

- `Send` 성공은 peer SDK가 payload를 receive queue에 넣었다는 한 가지 의미를 갖는다.
- Peer application의 read, processing, durable commit은 receipt contract 밖이다.
- Receipt loss의 모호성은 `Unknown`으로 노출하며 stable failure로 바꾸지 않는다.
- Same-Gateway와 cross-Gateway Pipe는 동일 receipt semantics를 가지며 각 Gateway는 exact correlated payload/receipt frame만 전달한다.
