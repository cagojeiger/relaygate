# SPEC 002: SDK와 Pipe 사용자 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 근거 | [ADR 001](../adr/001-relayed-pipe-responsibility-boundary.md), [ADR 002](../adr/002-application-protocol-boundary.md), [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 007](../adr/007-transport-liveness-and-idle-retirement.md) |
| 용어 | [SPEC 001](001-terminology-and-object-model.md) |
| 오류와 상태 | [SPEC 007](007-error-and-state-model.md) |

이 문서는 구현 언어와 무관하게 SDK 사용자가 관찰하는 `Connector`, `Listener`와 `Pipe` 동작을 정의한다.

## 사용자 모델

```text
open(ClientId) -> Pipe
Listener(ClientId).accept() -> Pipe
Pipe = read + write + shutdown(write) + close

Connector SDK runtime
  └── current ConnectorSession 0..1

Listener SDK runtime
  ├── current ListenerSession 0..1
  ├── ClientId -> pending ListenAttempt 0..1
  └── ClientId -> returned non-CLOSED Listener handle 0..1
```

이 문서의 `open(ClientId)`는 논리적인 Pipe 연결 operation을 뜻한다. 공개 SDK API는 transport runtime 생성과 logical Pipe 생성을 구분한다.

```text
Connector::connect(Config) -> Connector
connector.open(ClientId)   -> Pipe

ListenerRuntime::connect(Config)           -> ListenerRuntime
listener_runtime.listen(ClientId, ClientKey) -> Listener
listener.accept()                           -> Pipe
```

따라서 `Connector::connect(Config)`의 성공은 Gateway session 준비만 뜻하며 Listener에 대한 Pipe 성공이 아니다. 이 SPEC의 `open(ClientId)`는 공개 API의 `connector.open(ClientId)`에 대응한다.

공개 SDK의 `Pipe`는 Tokio `AsyncRead`와 `AsyncWrite`를 구현한다. `Pipe::into_split()`은 Pipe를 소비하고 하나의 `PipeReadHalf`와 하나의 `PipeWriteHalf`를 반환한다. 두 half는 clone할 수 없고 동일한 bounded Pipe state와 terminal 결과를 공유하며, read cursor와 inbound receiver는 read half 하나만 소유한다. 따라서 독립 task가 동시에 읽고 쓰더라도 별도의 Pipe, queue 또는 buffering layer를 만들지 않는다.

Tokio `AsyncWrite::poll_shutdown`은 이 문서의 `shutdown(write)`와 같은 `FIN`이다. half 하나의 drop은 `FIN`이나 전체 close를 합성하지 않는다. 마지막 public Pipe owner가 drop되면 기존 Pipe drop과 같은 전체 cleanup을 정확히 한 번 시작한다. Tokio I/O adapter가 `std::io::Error`를 반환할 때에도 원래 RelayGate `Error`를 `get_ref()`로 downcast 가능한 inner error로 보존해야 한다. RelayGate 오류 code와 observation을 직접 다뤄야 하는 사용자를 위해 이름이 충돌하지 않는 `read_into`와 `write_all_bytes` 구조화 메서드를 같은 state와 전송 경로 위에 유지한다. split 뒤 write와 `shutdown_write`·`close`·`reset`은 유일한 `PipeWriteHalf`가 소유하여 한 outbound 순서로 직렬화한다.

Listener 등록 API와 lifecycle은 SPEC 003이 정의한다. 이 문서는 등록된 Listener의 연결 수신 계약만 소유한다.

Listener의 최초 등록 성공은 `ClientKey` 검증과 Gateway-local binding 설치가 끝나 `ACTIVE`가 된 뒤 반환한다. RT registration은 별도의 `UNSYNCED/SYNCED` 상태이며, 등록 성공은 remote Gateway에서 이미 발견 가능하다는 보장이 아니다. shard-local mapping의 일시적 상실은 이미 성공한 Listener handle을 새로 만들거나 local admission을 중단시키지 않는다.

## 연결 성공

```text
Connector                 Listener SDK runtime          Listener application
    │ open(ClientId)                │                            │
    │                               │ enqueue Pipe               │
    │◄──── open success ───────────┤                            │
    │                               │──── accept() -> Pipe ─────►│
```

Listener SDK runtime이 선택된 binding의 bounded incoming queue에 Listener endpoint를 넣는 queue admission이 Pipe 생성 시점이다. Connector의 `open`은 그 뒤 `OPENED`를 확인해야만 Connector endpoint를 반환한다. queue admission 뒤 성공 확인을 잃거나 attempt가 실패하면 이미 생성되어 queued 또는 accept된 Pipe는 terminal로 닫힌다. 다음은 `open` 성공 조건이 아니다.

- Listener application이 `accept`를 호출함
- application handshake 또는 peer 인증·인가를 완료함
- application이 payload를 처리하거나 저장함

## Listener와 accept

한 Listener SDK runtime에서 같은 `ClientId`의 pending `ListenAttempt`와 non-`CLOSED` Listener handle은 합쳐서 하나만 존재할 수 있다. 생성은 runtime의 `ClientId` index에서 atomic하게 직렬화한다. 이미 reservation 또는 handle이 있으면 두 번째 생성은 `ALREADY_EXISTS`로 끝나며 새 registration, binding, incoming queue를 만들지 않는다. attempt가 terminal 실패하거나 기존 handle이 `CLOSED`가 되면 같은 `ClientId`로 새 attempt를 시작할 수 있고, 새 session registration은 재사용하지 않은 새 `BindingId`를 사용한다.

`accept`는 해당 Listener handle의 incoming queue에서 아직 전달되지 않은 current `ListenerSession`의 Pipe 하나를 정확히 한 번 반환한다. 하나 이상의 live `Listener` owner를 유지한 채 대기 중인 `accept` future만 drop 또는 abort하면 그 호출에는 SDK 반환값이 없고 해당 waiter만 제거한다. Listener, sibling Listener handle, 다른 `accept` 또는 queue의 다른 Pipe를 닫지 않는다. abort한 task가 마지막 `Listener` owner도 함께 소유했다면 task drop은 handle drop을 일으키므로 아래의 Listener close 계약을 따른다.

`accept`와 `ListenerSession` 종료는 한 순서로 직렬화한다. `accept` 성공은 dequeue 뒤의 마지막 `ACTIVE` 및 Pipe non-terminal 확인에서 확정된다. 이 확인이 먼저면 반환된 Pipe는 직후 발생한 session failure를 I/O 오류로 관찰할 수 있다. session 종료가 먼저 확정되면 그 session에서 queue admission을 마쳤지만 아직 accept되지 않은 Pipe를 queue에서 제거하고, `SUSPENDED` 또는 `REGISTERING`인 handle의 `accept`는 이후 session의 새 Pipe를 기다린다. `BLOCKED`와 `CLOSED`는 이 성공 확인 전에 관찰되면 queue보다 우선하며 각각 저장된 등록 오류와 closed 오류를 반환한다.

Listener handle을 닫으면 그 desired registration의 신규 Pipe 수신과 pending `accept`가 종료되고 아직 accept되지 않은 queued Pipe는 닫힌다. close와 Pipe queue admission은 runtime의 current desired index에서 한 순서로 직렬화되어야 한다. admission이 먼저면 close가 그 미수락 Pipe를 제거하고, close가 먼저면 새 Pipe를 queue에 넣지 않는다.

정상 `ACTIVE` binding을 닫는 경우 SDK actor는 해당 binding만 `UNREGISTER`하며 shared `ListenerSession`, sibling Listener handle과 이미 accept된 Pipe를 닫지 않는다. 다만 이미 반환된 Listener의 recovery `REGISTER`가 session outbound path에 commit된 뒤 결과가 확정되지 않은 동안 그 Listener가 close/drop되면 개별 binding의 존재 여부를 안전하게 판단할 수 없다. 이 경우 Listener handle이 session token을 직접 취소하지 않고, shared actor가 desired 제거와 committed registration을 함께 관찰하여 current `ListenerSession`을 종료한다. 그 session의 Pipe는 실패로 닫히며, closing Listener를 제외한 returned sibling Listener는 desired state로 남아 새 session에 재등록된다.

같은 `ClientId`는 서로 다른 `ListenerSession`, 즉 서로 다른 Listener SDK runtime에서 동시에 `0..N`개 등록할 수 있다. 반면 한 Listener SDK runtime은 current `ListenerSession` 하나를 공유하므로 같은 `ClientId`의 pending attempt 또는 returned Listener를 하나만 허용한다.

```text
ClientId A
  ├── ListenerRuntime 1 / ListenerSession 1
  └── ListenerRuntime 2 / ListenerSession 2

open(A) 1회 -> 위 Listener 중 정확히 하나
```

선택된 Listener가 거절, timeout 또는 단절로 실패해도 같은 `open` attempt를 sibling Listener로 자동 fallback하지 않는다. 애플리케이션이 새 `open(ClientId)` operation을 시작하면 그때의 live binding set에서 다시 하나를 선택할 수 있다. SDK는 이 새 operation의 시작 여부나 업무 retry 정책을 대신 결정하지 않는다.

## Pipe byte stream

Pipe는 방향별 순서를 보존하는 opaque byte stream이며 application message boundary를 제공하지 않는다.

- 성공한 write는 bytes가 Pipe의 bounded 전송 경로에 받아들여졌다는 뜻이다.
- 성공한 write는 peer application의 수신·처리·저장 성공을 뜻하지 않는다.
- write-direction shutdown은 먼저 받아들인 bytes 뒤에 EOF를 전달하며 반대 방향 read는 계속할 수 있다.
- full close는 양방향을 종료하며 반복 호출해도 안전해야 한다.
- transport 상실이나 terminal failure가 발생하면 영향을 받은 Pipe는 종료되고 복구된 session으로 이동하지 않는다.
- read와 write는 동시에 진행할 수 있다. owned split은 동시 사용을 위한 ownership adapter일 뿐 Pipe의 identity, buffer, ordering 또는 terminal state를 복제하지 않는다.

payload framing, delivery acknowledgement, idempotency와 업무 retry는 application protocol이 소유한다.

## Pipe 종료 의미

```text
FIN   = 한 방향의 graceful EOF
CLOSE = 정상적인 전체 Pipe 종료
RESET = 실패에 의한 전체 Pipe 종료
```

`shutdown(write)`는 같은 방향에서 먼저 성공한 write bytes가 peer read에 순서대로 전달된 뒤 EOF가 보이게 하는 `FIN` 의미다. `FIN` 뒤 같은 방향 write는 실패하지만 반대 방향은 계속 사용할 수 있다. 양방향이 모두 `FIN`이면 Pipe는 정상적으로 끝난다.

`close`는 `CLOSE` 의미로 양방향을 정상 종료하지만, application payload 처리나 아직 drain하지 않은 bytes의 전달을 보장하지 않는다. graceful write drain이 필요하면 먼저 `shutdown(write)`를 사용한다. transport 상실, protocol 위반 또는 terminal 내부 실패는 `RESET` 의미로 양방향 pending I/O를 실패시킨다. 세 종료 신호의 중복 처리는 안전해야 하고 닫힌 Pipe를 다시 열어서는 안 된다.

## Backpressure

SDK의 incoming queue와 Pipe buffer는 모두 bounded여야 한다.

- incoming queue가 가득 차면 기존 queued Pipe를 버리거나 교체해서는 안 된다.
- SDK의 control operation과 내부 transport frame write는 configured deadline 안에서 끝나야 한다. application의 `Pipe` read/write는 TCP와 같은 bounded backpressure이며 SDK가 임의의 I/O deadline을 부여하지 않는다. caller는 필요하면 자신의 timeout이나 cancellation을 적용할 수 있다. drop 또는 abort된 I/O future에는 SDK 반환값이 없으며 Pipe와 shared session을 terminal로 만들지 않는다. 취소 전 이미 queue에 수락된 bytes는 partial write로 관찰될 수 있다. session이 실패하면 capacity를 기다리는 Pipe operation도 terminal failure로 풀려야 하며 bytes를 조용히 버려서는 안 된다. 같은 session의 서로 다른 Pipe 사이 FIFO writer scheduling은 보장하지 않는다.
- 느린 Listener나 Pipe 하나 때문에 memory 사용량이 무제한 증가해서는 안 된다.

queue와 buffer 크기, flow-control protocol과 scheduling은 별도 SPEC 또는 configuration이 정한다.

## 취소와 종료 경쟁

끝까지 await한 `open`, `listen` 또는 `accept`는 성공이나 오류 하나만 반환한다. Rust future를 drop 또는 abort하면 해당 호출에는 SDK 반환값이 없지만 내부 attempt는 terminal로 정확히 한 번 정리되어야 한다.

- `open` future가 drop 또는 abort되면 commit 전 attempt를 제거하거나 commit 뒤 `CANCEL`을 한 번 보내며, 늦은 성공 응답이 Pipe를 반환하거나 state를 되살려서는 안 된다.
- `listen` future가 drop 또는 abort되면 reservation을 제거한다. `REGISTER`가 이미 commit되어 결과가 불확실하면 current `ListenerSession`을 종료하며, `REGISTERED`를 처리했지만 Listener handle을 아직 반환하지 않았다면 알려진 binding을 같은 session에서 `UNREGISTER`한다. 어느 경우에도 같은 attempt를 새 session에서 재등록하지 않는다.
- live `Listener` owner를 유지한 채 `accept` future만 drop 또는 abort하면 해당 waiter만 제거하고 Listener registration, sibling Listener handle, 다른 waiter와 queued Pipe는 유지한다. abort한 task가 마지막 owner도 함께 drop하면 Listener close 계약을 따른다.
- `open`이 이미 Pipe를 반환했다면 과거 호출을 취소하는 대신 반환된 Pipe를 닫아야 한다.
- 명시적으로 닫힌 runtime 또는 Listener에서 새로 await한 operation은 `CANCELLED`를 반환한다.

경쟁 상황의 canonical 상태와 오류 분류는 SPEC 007이 정의한다.

## 관리되는 재연결

SDK runtime은 Gateway 연결이 끊어지면 사용자가 별도 supervisor를 만들지 않아도 재연결을 시도해야 한다.

```text
SDK가 자동 복구      = 자기 SDK-Gateway Session + 이미 반환된 Listener registration
SDK가 자동 복구 안 함 = 기존 Pipe + commit된 OPEN + payload
애플리케이션 책임     = 새 open(ClientId) + application handshake와 업무 retry
```

- 이미 반환된 `Listener` handle들은 일시적인 Gateway 단절 동안 각자의 desired `ClientId`를 유지한다. SDK runtime은 returned live Listener set을 복구 source of truth로 삼고 shared session을 다시 만든 뒤 그 handle들의 registration을 전부 다시 등록한다. pending `ListenAttempt`를 이 복구 집합에 포함하거나 과거 wire command를 replay해서는 안 된다.
- 한 live `ListenerSession`에 commit된 `REGISTER`가 configured response deadline까지 terminal 응답을 받지 못하면 SDK는 그 session을 종료한다. 해당 request를 시작한 pending `ListenAttempt`는 terminal 실패하며 새 session으로 이동하지 않는다. 이미 반환된 Listener만 새 `ListenerSessionId`와 새 request identity로 다시 선언한다.
- 반복되는 session failure와 `REGISTER` response timeout은 bounded reconnect backoff를 따라야 한다. TCP session을 만들었다는 이유만으로 failure streak을 즉시 초기화하지 않고, current desired registration의 terminal 성공을 관찰한 뒤 정상 수렴으로 취급한다.
- 최초 `listen(ClientId, ClientKey)` 호출은 `REGISTER`가 live session의 bounded outbound path에 commit되기 전까지만 자신의 deadline 안에서 session 준비를 기다릴 수 있다. commit 뒤 명시적 등록 실패, response deadline, session 상실 또는 future drop·abort에 따른 내부 취소는 attempt를 terminal로 끝내고 reservation을 제거한다. 결과가 불확실한 committed request는 session 종료로, 이미 `REGISTERED`를 처리한 binding은 같은 session의 `UNREGISTER`로 정리한다. drop 또는 abort된 호출에는 SDK 오류를 반환하지 않는다. 같은 호출을 새 session에서 자동 재등록하지 않으며 애플리케이션이 새 `listen`을 시작할지 결정한다.
- 이미 반환된 Listener의 recovery registration이 transient 실패하면 SDK는 bounded backoff 뒤 새 request identity로 다시 선언할 수 있다. recovery `REGISTER`의 credential·permission terminal 거절은 해당 Listener를 `BLOCKED`로 만들고 자동 재등록하지 않는다.
- Gateway의 `ClientKey` map은 process 시작 시 고정되며 runtime revocation event는 없다. replacement Gateway가 recovery `REGISTER`의 저장된 key를 거절하면 영향받은 Listener handle만 `BLOCKED`에 머문다. 애플리케이션은 그 handle을 닫고 새 key로 새 `listen(ClientId, ClientKey)` operation을 시작해야 한다.
- `BLOCKED` handle은 current binding을 갖지 않고 신규 Pipe를 받지 않는다. 전환 전에 종료된 session에서 남은 미수락 Pipe를 모두 제거하고 pending·후속 `accept`에 저장된 등록 오류를 반환한다. sibling Listener handle과 shared session은 유지한다.
- 재연결 중 pending `accept`는 취소되거나 Listener가 닫히지 않는 한 이후의 새 Pipe를 기다릴 수 있다.
- 단절 전에 queue에 있었지만 transport와 함께 유효성을 잃은 Pipe는 복구하지 않는다.
- `Connector`는 재연결 뒤 애플리케이션이 요청한 새로운 `open(ClientId)`를 수행할 수 있다.
- reconnect backoff 중 아직 live `ConnectorSession`의 bounded outbound path에 commit되지 않은 open request는 자신의 deadline 또는 취소 전까지 연결 준비를 기다릴 수 있다. 요청이 outbound path에 한 번 commit된 뒤에는 Gateway 수신이나 Listener queue 도달 여부와 관계없이 session 단절 시 그 시도를 새 session으로 옮기거나 자동 replay해서는 안 된다.
- Connector 재연결은 새 `ConnectorSessionId`를 사용한다. 이전 session의 open attempt, Pipe와 늦은 응답은 새 session에 귀속되지 않는다.
- 닫히거나 실패한 Pipe, 이미 write한 payload와 application handshake를 자동 replay하거나 resume해서는 안 된다.

`ConnectorSessionId`는 relay 내부 lifecycle identity다. Listener application의 peer 인증·인가 또는 application caller identity로 사용해서는 안 된다.

open request의 commit point는 SDK가 그 요청을 한 live `ConnectorSession`의 bounded outbound path에 받아들인 시점이다. commit 전에는 Gateway가 요청을 관찰할 수 없으므로 같은 사용자 호출이 새 session 준비를 기다릴 수 있다. commit 뒤 terminal 응답을 확인하지 못하면 Listener queue에 도달하지 않았다는 증명이 있는 경우만 `NOT_OBSERVED`이고, 그 외에는 실제 도달 여부와 관계없이 보수적으로 `MAYBE_OBSERVED`다.

commit된 open request가 operation deadline까지 `OPENED` 또는 `FAILED`를 받지 못하면 SDK는 그 호출만 끝내고 같은 `ConnectorSession`을 계속 사용하지 않는다. current `ConnectorSession`을 종료하고 그 session의 다른 attempt와 Pipe도 terminal failure로 수렴시킨 뒤, managed reconnect는 새 session identity로 시작한다. timeout된 attempt, Pipe와 payload는 replay하지 않는다.

SDK session의 outbound write는 cancellation과 configured deadline에 종속되어야 한다. frame write가 그 bound 안에 끝나지 않으면 해당 session을 재사용하지 않고 transport loss와 같은 cleanup·reconnect 경로로 수렴시킨다. 단순한 application `Pipe.read()` idle은 control operation timeout이 아니며 session을 닫는 근거가 아니다.

SDK-Gateway session은 activity-aware heartbeat를 사용한다.

```text
valid inbound activity 있음 -> PING 전송 전 heartbeat timer 연장 가능
idle interval 경과          -> PING commit
deadline 전 matching PONG  -> READY 유지
timeout                    -> session 종료 -> 기존 cleanup/reconnect 경로
```

Heartbeat는 SDK와 Gateway 사이 transport liveness만 확인한다. application `Pipe.read()`가 조용하거나 payload 응답이 없다는 사실은 heartbeat failure가 아니다. heartbeat timeout으로 session을 닫으면 그 session의 pending operation과 Pipe는 기존 transport-loss cleanup을 따르고, SDK는 새 session으로 managed reconnect만 수행한다. commit된 `open`, 기존 Pipe와 payload는 자동 replay, reroute, resume하지 않는다.

## SDK runtime lifetime

명시적인 runtime `close`는 managed reconnect와 current session을 종료한다. 개별 clone이나 하나의 Listener handle을 drop하는 것만으로 shared runtime을 닫아서는 안 된다. 반대로 해당 runtime을 사용할 수 있는 마지막 public owner가 사라지면 background reconnect가 스스로를 영구 소유해서는 안 되며 runtime을 종료해야 한다. 이미 반환된 live `Pipe`는 자신의 I/O를 위해 runtime lifetime을 유지하는 owner로 취급한다.

Listener registration의 승인·거절·갱신 절차는 SPEC 003이, 상태와 retry 가능 오류는 SPEC 007이 정의한다.

## 요구사항

- **`SDK-001`**: `open(ClientId)`, 즉 공개 API의 `connector.open(ClientId)`는 정확히 하나의 Listener에 대한 Pipe 하나를 요청해야 한다. `Connector::connect(Config)`는 SDK-Gateway session 생성이며 이 operation과 구분해야 한다.
- **`SDK-002`**: Listener queue admission은 Pipe를 정확히 하나 만들고, Connector의 open 성공은 그 뒤 `OPENED`를 확인한 경우에만 Connector endpoint를 반환해야 한다. queue admission 뒤 attempt가 실패하면 queued 또는 accept된 Pipe를 terminal로 닫아야 한다.
- **`SDK-003`**: incoming queue가 가득 찼을 때 기존의 non-terminal queued Pipe를 제거해 새 연결을 성공시켜서는 안 된다. 반대로 accept 전에 terminal이 된 Pipe는 애플리케이션의 추가 호출을 기다리지 않고 queue capacity에서 제거되어야 한다.
- **`SDK-004`**: `Listener.accept()`는 같은 Pipe를 두 번 반환하지 않고 distinct Pipe 하나를 반환해야 한다. `accept`와 session 종료는 한 순서로 직렬화하고, 종료가 먼저면 old session의 미수락 Pipe를 반환해서는 안 된다. `SUSPENDED/REGISTERING`은 이후 새 Pipe를 기다릴 수 있지만 `BLOCKED/CLOSED`는 queue보다 우선해 terminal 오류를 반환해야 한다.
- **`SDK-005`**: live Listener owner가 유지된 pending accept future의 drop 또는 abort는 SDK 결과를 반환하지 않고 해당 waiter만 종료하며 Listener, sibling Listener handle, 다른 waiter와 queued Pipe의 상태를 변경해서는 안 된다. 취소 task가 마지막 Listener owner를 함께 drop하면 Listener handle close 계약을 적용한다.
- **`SDK-006`**: Listener handle close와 해당 handle의 Pipe queue admission은 한 순서로 직렬화되어야 한다. 정상 `ACTIVE` binding의 close는 자신의 신규·대기·미수락 Pipe와 binding만 종료하고 shared `ListenerSession`, sibling Listener handle 또는 이미 accept된 Pipe를 닫아서는 안 된다. returned Listener의 recovery `REGISTER`가 commit된 채 결과가 불확실하면 handle이 session을 직접 취소하지 않고 shared actor가 current `ListenerSession`을 종료해야 한다. 이 session reset은 closing Listener를 재등록하지 않고 returned sibling Listener만 새 session에 재등록하며 old session의 Pipe를 terminal failure로 끝내야 한다.
- **`SDK-007`**: Pipe는 방향별 byte 순서를 보존해야 하며 message boundary를 추가해서는 안 된다.
- **`SDK-008`**: SDK의 모든 incoming queue와 Pipe buffer는 bounded여야 하며 capacity 부족을 backpressure 또는 명시적 실패로 관찰시켜야 한다.
- **`SDK-009`**: write 성공을 peer application의 처리 성공으로 보고해서는 안 된다.
- **`SDK-010`**: write-direction shutdown 뒤에도 반대 방향 read를 허용해야 하며 full close는 idempotent해야 한다.
- **`SDK-011`**: SDK runtime은 자신이 소유한 SDK-Gateway session의 일시적인 단절 뒤 managed reconnect를 수행해야 한다.
- **`SDK-012`**: managed reconnect는 shared `ListenerSession` 하나를 새로 만들고 살아 있는 각 Listener handle의 desired `ClientId`를 그 session에 자동 재등록해야 한다.
- **`SDK-013`**: reconnect는 이전 open request, Pipe 또는 payload를 자동 replay, reroute, migrate 또는 resume해서는 안 된다. 새 `open(ClientId)`와 application 업무 retry는 SDK 사용자가 결정해야 한다.
- **`SDK-014`**: 끝까지 await한 pending open, listen 또는 accept는 성공이나 오류 하나만 반환해야 한다. future drop 또는 abort에는 SDK 반환값이 없지만 내부 attempt는 정확히 한 번 terminal cleanup되고 늦은 응답으로 Pipe나 registration이 부활해서는 안 된다.
- **`SDK-015`**: transport 상실의 영향을 받은 accepted Pipe는 terminal failure로 종료하고 복구된 session에 붙여서는 안 된다. 같은 old session에서 아직 accept되지 않은 Pipe는 incoming queue에서 제거하고 이후 session의 Pipe처럼 반환해서는 안 된다.
- **`SDK-016`**: Listener의 최초 등록 성공은 `ClientKey` 검증과 Gateway-local binding 설치가 끝난 뒤 반환해야 한다. RT registration 성공 여부는 별도의 상태이며, 등록 성공을 remote discovery 완료로 해석해서는 안 된다.
- **`SDK-017`**: recovery `REGISTER`의 credential·permission terminal 거절로 `BLOCKED`가 된 Listener handle은 자동 재등록하거나 같은 handle을 다시 활성화해서는 안 된다. old session의 미수락 Pipe를 제거하고 pending·후속 `accept`에는 등록 오류를 반환하되 sibling handle과 shared session은 유지한다. 새 credential 적용은 기존 handle을 닫은 뒤 새 `listen` operation으로 수행한다.
- **`SDK-018`**: Connector SDK runtime은 재연결마다 새 `ConnectorSessionId`의 session을 사용해야 한다. 이전 session의 outbound path에 commit된 open request, attempt와 Pipe를 새 session으로 replay, migrate 또는 resume해서는 안 된다.
- **`SDK-019`**: 같은 Listener SDK runtime에서 동일 `ClientId`의 pending `ListenAttempt`와 non-`CLOSED` Listener handle은 합쳐서 최대 하나여야 한다. 나머지 생성 호출은 `ALREADY_EXISTS`이고 새 registration, binding 또는 incoming queue를 만들지 않아야 하며, attempt의 terminal 실패 또는 기존 handle의 `CLOSED` 뒤에는 새 생성이 가능해야 한다. 이 제한은 서로 다른 Listener SDK runtime과 `ListenerSession`이 같은 `ClientId`를 등록하는 N:M 관계를 막지 않는다.
- **`SDK-020`**: `shutdown(write)`는 같은 방향에서 먼저 수락한 bytes 뒤에 peer EOF를 전달하고 그 방향의 이후 write를 실패시켜야 한다. 반대 방향은 독립적으로 `FIN`할 때까지 계속 사용할 수 있으며 양방향 `FIN` 뒤 Pipe는 정상 종료해야 한다.
- **`SDK-021`**: `CLOSE`는 정상적인 전체 종료, `RESET`은 실패에 의한 전체 종료로 관찰되어야 한다. 둘 다 양방향을 terminal로 만들고 idempotent해야 한다. 서로 다른 terminal 신호가 경쟁하면 먼저 확정된 결과만 유지하고 뒤의 신호가 정상·실패 의미를 바꾸지 못한다. `CLOSE`를 payload delivery acknowledgement로 해석해서는 안 된다.
- **`SDK-022`**: open request는 한 live `ConnectorSession`의 bounded outbound path에 최대 한 번 commit되어야 한다. commit 전 호출만 새 session 준비를 기다릴 수 있고, commit 뒤 terminal 결과를 확인하지 못하면 queue 미도달을 증명할 수 있는 경우를 제외하고 `MAYBE_OBSERVED`로 끝내며 자동 replay해서는 안 된다.
- **`SDK-023`**: commit된 open request가 operation deadline까지 terminal 응답을 받지 못하면 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`로 끝내고 current `ConnectorSession`을 종료해야 한다. 그 session의 다른 attempt와 Pipe도 terminal cleanup하며 새 session으로 replay, migrate 또는 resume해서는 안 된다.
- **`SDK-024`**: SDK session의 outbound frame write는 cancellation과 configured deadline 안에 끝나야 한다. write 실패 또는 deadline은 해당 session을 종료하고 managed reconnect 경로로 수렴시켜야 하며 pending operation과 Pipe를 무기한 붙잡아서는 안 된다.
- **`SDK-025`**: 명시적 runtime close는 current session과 managed reconnect를 종료해야 한다. 개별 clone drop은 shared runtime을 닫지 않지만, live `Pipe`를 포함하여 runtime을 사용할 마지막 public owner가 사라지면 background task는 runtime을 자기 소유로 남기지 않고 종료해야 한다.
- **`SDK-026`**: SDK-Gateway session은 activity-aware heartbeat를 수행해야 한다. idle interval 동안 valid inbound activity가 없으면 `PING`을 commit하고, configured response deadline 전에 matching `PONG`을 확인하지 못하면 current session을 transport loss와 같이 종료해야 한다. unrelated inbound frame, outbound write, nonce가 다른 `PONG`, deadline 이후의 늦은 `PONG`은 commit된 probe를 만족시키지 않는다. heartbeat는 Pipe/application health나 delivery acknowledgement가 아니며 application Pipe read idle만으로 session을 닫아서는 안 된다.
- **`SDK-027`**: commit된 최초 `REGISTER`가 명시적 실패, response deadline 또는 session 상실로 terminal 성공을 확인하지 못하면 해당 `ListenAttempt`를 한 번만 실패시키고 reservation을 제거해야 한다. response deadline은 current `ListenerSession` 전체를 종료해야 한다. SDK는 pending attempt를 새 session으로 옮기거나 같은 request를 replay해서는 안 되며, 이미 반환된 current Listener만 bounded reconnect backoff 뒤 새 session identity와 새 registration request로 재등록해야 한다. TCP 연결 성공만으로 연속 failure backoff를 초기화해서는 안 되며 반환된 Listener의 recovery registration 성공 뒤 초기화할 수 있다.
- **`SDK-028`**: 공개 SDK의 `Pipe`는 Tokio `AsyncRead`와 `AsyncWrite`를 구현하고, consuming owned split으로 non-clone `PipeReadHalf` 하나와 `PipeWriteHalf` 하나를 제공해야 한다. 두 half는 하나의 bounded Pipe state와 terminal 결과를 공유하며 read receiver와 cursor는 read half만, outbound sender와 write·`shutdown_write`·`close`·`reset` 순서는 write half만 소유해야 한다. `AsyncWrite` shutdown은 `FIN`과 같고 half 하나의 drop은 protocol frame을 합성하지 않으며 마지막 public owner의 drop만 전체 cleanup을 정확히 한 번 시작해야 한다. Tokio I/O 오류는 원래 RelayGate `Error`를 downcast 가능한 inner error로 보존해야 하고, 이름 충돌 없는 `read_into`·`write_all_bytes`와 trait adapter는 같은 ordering·backpressure·terminal 경로를 사용해야 한다.
- **`SDK-029`**: SDK의 timeout configuration은 양수이고 monotonic deadline으로, reconnect backoff configuration은 양수·순서 조건을 만족하며 bounded wake-up timer로 표현 가능해야 한다. `Connector::connect(Config)`와 `ListenerRuntime::connect(Config)`는 runtime timer로 표현할 수 없는 값을 transport 연결이나 background state 생성 전에 `INVALID_ARGUMENT`, `NOT_OBSERVED`로 거절해야 하며 runtime panic으로 넘겨서는 안 된다.
