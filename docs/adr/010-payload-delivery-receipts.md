# ADR 010: 종단 간 payload 전달 확인

## 배경

이 결정 전의 `Pipe.Send`는 bounded local transport handoff만 증명했다. Sender가 relay hop 뒤 retry 여부를 결정하기에는 부족했다. Payload가 peer SDK에 도달했는지 peer receive queue 전에 멈췄는지 구분할 수 없었기 때문이다.

RelayGate는 durable broker가 되어서는 안 된다. Application processing, durable message storage, replay, exactly-once effect는 소유하지 않는다.

## 결정

- 모든 public/peer `PipePayload`는 `PipeId`와 direction 범위의 exact `PayloadId`를 가진다.
- 수신 SDK는 exact payload가 상한이 있는 수신 대기열에 들어간 뒤에만 `PipePayloadReceived`를 보낸다. 이 대기열 수락이 payload 전달 선형화 지점이다.
- Sender는 exact receipt를 관찰한 뒤에만 성공을 반환한다.
- 로컬 인증 stream 전달 이전 실패는 `NotSent`, exact 원격 거부는 `Rejected`다. 전달 이후 확인 관찰 전 제한 시간 초과, Pipe·세션 손실, 전송 손실은 `Unknown`이다.
- Receipt와 rejection은 exact `PipeId`와 `PayloadId`를 가진다. Unknown, malformed, foreign, duplicate-conflicting, wrong-phase correlation은 protocol-fatal이다.
- `Unknown`은 caller-visible absorbing outcome이다. 늦은 exact receipt/rejection은 bounded NoOp이며 이미 반환한 결과를 바꾸지 않는다.
- 발신자 대기 상태와 수신자 확인 이력은 SDK·Pipe 실행 상태의 상한이 있는 프로세스 메모리다. Controller Raft에 저장하거나 Pipe·세션·프로세스 종료 뒤 재개하지 않는다.
- RelayGate는 payload를 자동 재시도·재생하지 않는다. 새 Pipe에서 `Unknown` 전달을 재시도하는 애플리케이션은 자체 안정 메시지 식별자와 멱등 처리 계약을 가져야 한다.

## 결과

- `Send` 성공은 peer SDK가 payload를 receive queue에 넣었다는 한 가지 의미를 갖는다.
- 상대 애플리케이션의 읽기, 처리, 영속 commit은 전달 확인 계약 밖이다.
- 확인 손실의 모호성은 `Unknown`으로 노출하며 확정 실패로 바꾸지 않는다.
- Same-Gateway와 cross-Gateway Pipe는 동일 receipt semantics를 가지며 각 Gateway는 exact correlated payload/receipt frame만 전달한다.
