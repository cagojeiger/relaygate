# SPEC 002: SDK와 Pipe 사용자 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 001](../adr/001-relayed-pipe-responsibility-boundary.md), [ADR 002](../adr/002-application-protocol-boundary.md), [ADR 003](../adr/003-client-id-listener-binding.md) |
| 용어 | [SPEC 001](001-terminology-and-object-model.md) |
| 오류와 상태 | [SPEC 007](007-error-and-state-model.md) |

이 문서는 구현 언어와 무관하게 SDK 사용자가 관찰하는 `Connector`, `Listener`와 `Pipe` 동작을 정의한다.

## 사용자 모델

```text
Connector.connect(ClientId) -> Pipe
Listener(ClientId).accept() -> Pipe
Pipe = read + write + shutdown(write) + close

Connector SDK runtime
  └── current ConnectorSession 0..1

Listener SDK runtime
  ├── current ListenerSession 0..1
  ├── ClientId -> pending ListenAttempt 0..1
  └── ClientId -> returned non-CLOSED Listener handle 0..1
```

Listener 등록 API와 lifecycle은 SPEC 003이 정의한다. 이 문서는 등록된 Listener의 연결 수신 계약만 소유한다.

Listener의 최초 등록 성공은 `ClientKey` 검증과 Gateway-local binding 설치가 끝나 `ACTIVE`가 된 뒤 반환한다. RT registration은 별도의 `UNSYNCED/SYNCED` 상태이며, 등록 성공은 remote Gateway에서 이미 발견 가능하다는 보장이 아니다. shard-local mapping의 일시적 상실은 이미 성공한 Listener handle을 새로 만들거나 local admission을 중단시키지 않는다.

## 연결 성공

```text
Connector                 Listener SDK runtime          Listener application
    │ connect(ClientId)             │                            │
    │                               │ enqueue Pipe               │
    │◄──── connect success ─────────┤                            │
    │                               │──── accept() -> Pipe ─────►│
```

`connect` 성공은 상대 Pipe가 Listener SDK runtime에서 선택된 binding에 대응하는 Listener handle의 bounded incoming queue에 들어간 뒤에만 반환할 수 있다. 다음은 성공 조건이 아니다.

- Listener application이 `accept`를 호출함
- application handshake 또는 peer 인증·인가를 완료함
- application이 payload를 처리하거나 저장함

## Listener와 accept

한 Listener SDK runtime에서 같은 `ClientId`의 pending `ListenAttempt`와 non-`CLOSED` Listener handle은 합쳐서 하나만 존재할 수 있다. 생성은 runtime의 `ClientId` index에서 atomic하게 직렬화한다. 이미 reservation 또는 handle이 있으면 두 번째 생성은 `ALREADY_EXISTS`로 끝나며 새 registration, binding, incoming queue를 만들지 않는다. attempt가 terminal 실패하거나 기존 handle이 `CLOSED`가 되면 같은 `ClientId`로 새 attempt를 시작할 수 있고, 새 session registration은 재사용하지 않은 새 `BindingId`를 사용한다.

`accept`는 해당 Listener handle의 incoming queue에서 아직 전달되지 않은 Pipe 하나를 정확히 한 번 반환한다. 대기 중인 `accept`를 취소해도 Listener, sibling Listener handle, 다른 `accept` 또는 queue의 다른 Pipe를 닫지 않는다.

Listener handle을 닫으면 그 desired registration의 신규 Pipe 수신과 pending `accept`가 종료되고 아직 accept되지 않은 queued Pipe는 닫힌다. close와 Pipe queue admission은 runtime의 current desired index에서 한 순서로 직렬화되어야 한다. admission이 먼저면 close가 그 미수락 Pipe를 제거하고, close가 먼저면 새 Pipe를 queue에 넣지 않는다. shared `ListenerSession`, sibling Listener handle과 이미 accept된 Pipe는 닫지 않는다.

## Pipe byte stream

Pipe는 방향별 순서를 보존하는 opaque byte stream이며 application message boundary를 제공하지 않는다.

- 성공한 write는 bytes가 Pipe의 bounded 전송 경로에 받아들여졌다는 뜻이다.
- 성공한 write는 peer application의 수신·처리·저장 성공을 뜻하지 않는다.
- write-direction shutdown은 먼저 받아들인 bytes 뒤에 EOF를 전달하며 반대 방향 read는 계속할 수 있다.
- full close는 양방향을 종료하며 반복 호출해도 안전해야 한다.
- transport 상실이나 terminal failure가 발생하면 영향을 받은 Pipe는 종료되고 복구된 session으로 이동하지 않는다.

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
- SDK의 control operation과 내부 transport frame write는 configured deadline 안에서 끝나야 한다. application의 `Pipe` read/write는 TCP와 같은 bounded backpressure이며 SDK가 임의의 I/O deadline을 부여하지 않는다. caller는 필요하면 자신의 timeout이나 cancellation을 적용할 수 있고, 취소 전 이미 queue에 수락된 bytes는 partial write로 관찰될 수 있다. session이 실패하면 capacity를 기다리는 Pipe operation도 terminal failure로 풀려야 하며 bytes를 조용히 버려서는 안 된다.
- 느린 Listener나 Pipe 하나 때문에 memory 사용량이 무제한 증가해서는 안 된다.

queue와 buffer 크기, flow-control protocol과 scheduling은 별도 SPEC 또는 configuration이 정한다.

## 취소와 종료 경쟁

하나의 pending `connect` 또는 `accept`는 성공, 실패, 취소 중 하나의 terminal 결과만 사용자에게 반환해야 한다.

- connect 취소가 성공보다 먼저 확정되면 그 호출은 이후 Pipe를 반환해서는 안 된다.
- connect가 이미 성공했다면 기존 connect 호출을 취소하는 대신 반환된 Pipe를 닫아야 한다.
- accept 취소는 해당 대기 호출만 종료하며 Listener registration을 제거하지 않는다.

경쟁 상황의 canonical 상태와 오류 분류는 SPEC 007이 정의한다.

## 관리되는 재연결

SDK runtime은 Gateway 연결이 끊어지면 사용자가 별도 supervisor를 만들지 않아도 재연결을 시도해야 한다.

- 이미 반환된 `Listener` handle들은 일시적인 Gateway 단절 동안 각자의 desired `ClientId`를 유지한다. SDK runtime은 returned live Listener set을 복구 source of truth로 삼고 shared session을 다시 만든 뒤 그 handle들의 registration을 전부 다시 등록한다. pending `ListenAttempt`를 이 복구 집합에 포함하거나 과거 wire command를 replay해서는 안 된다.
- 한 live `ListenerSession`에 commit된 `REGISTER`가 configured response deadline까지 terminal 응답을 받지 못하면 SDK는 그 session을 종료한다. 해당 request를 시작한 pending `ListenAttempt`는 terminal 실패하며 새 session으로 이동하지 않는다. 이미 반환된 Listener만 새 `ListenerSessionId`와 새 request identity로 다시 선언한다.
- 반복되는 session failure와 `REGISTER` response timeout은 bounded reconnect backoff를 따라야 한다. TCP session을 만들었다는 이유만으로 failure streak을 즉시 초기화하지 않고, current desired registration의 terminal 성공을 관찰한 뒤 정상 수렴으로 취급한다.
- 최초 `listen(ClientId, ClientKey)` 호출은 `REGISTER`가 live session의 bounded outbound path에 commit되기 전까지만 자신의 deadline 안에서 session 준비를 기다릴 수 있다. commit 뒤 명시적 등록 실패, response deadline, session 상실 또는 호출 취소는 attempt를 terminal로 끝내고 reservation을 제거한다. 같은 호출을 새 session에서 자동 재등록하지 않으며 애플리케이션이 새 `listen`을 시작할지 결정한다.
- 이미 반환된 Listener의 recovery registration이 transient 실패하면 SDK는 bounded backoff 뒤 새 request identity로 다시 선언할 수 있다. credential·permission의 terminal 거절은 해당 Listener를 `BLOCKED`로 만들고 자동 재등록하지 않는다.
- credential 거절이나 등록 권한 폐기는 transport 단절로 취급하지 않는다. 영향받은 Listener handle은 `BLOCKED`에 머물며 자동 재등록하지 않는다. 현재 public API에서 credential을 바꾸려면 애플리케이션이 기존 handle을 닫고 새 `listen(ClientId, ClientKey)` operation을 시작해야 한다.
- `BLOCKED` 전이 전에 queue에 적재된 Pipe와 이미 accept된 Pipe는 유지한다. `ClientKey` 폐기는 신규 Pipe admission을 중단하지만 기존 Pipe의 application 권한이나 종료를 소급해 결정하지 않는다.
- 재연결 중 pending `accept`는 취소되거나 Listener가 닫히지 않는 한 이후의 새 Pipe를 기다릴 수 있다.
- 단절 전에 queue에 있었지만 transport와 함께 유효성을 잃은 Pipe는 복구하지 않는다.
- `Connector`는 재연결 뒤 새로운 connect를 수행할 수 있다.
- reconnect backoff 중 아직 live `ConnectorSession`의 bounded outbound path에 commit되지 않은 connect는 자신의 deadline 또는 취소 전까지 연결 준비를 기다릴 수 있다. 요청이 outbound path에 한 번 commit된 뒤에는 Gateway 수신이나 Listener queue 도달 여부와 관계없이 session 단절 시 그 시도를 새 session으로 옮기거나 자동 replay해서는 안 된다.
- Connector 재연결은 새 `ConnectorSessionId`를 사용한다. 이전 session의 connect attempt, Pipe와 늦은 응답은 새 session에 귀속되지 않는다.
- 닫히거나 실패한 Pipe, 이미 write한 payload와 application handshake를 자동 replay하거나 resume해서는 안 된다.

`ConnectorSessionId`는 relay 내부 lifecycle identity다. Listener application의 peer 인증·인가 또는 application caller identity로 사용해서는 안 된다.

connect request의 commit point는 SDK가 그 요청을 한 live `ConnectorSession`의 bounded outbound path에 받아들인 시점이다. commit 전에는 Gateway가 요청을 관찰할 수 없으므로 같은 사용자 호출이 새 session 준비를 기다릴 수 있다. commit 뒤 terminal 응답을 확인하지 못하면 Listener queue에 도달하지 않았다는 증명이 있는 경우만 `NOT_OBSERVED`이고, 그 외에는 실제 도달 여부와 관계없이 보수적으로 `MAYBE_OBSERVED`다.

commit된 connect가 operation deadline까지 `OPENED` 또는 `FAILED`를 받지 못하면 SDK는 그 호출만 끝내고 같은 `ConnectorSession`을 계속 사용하지 않는다. current `ConnectorSession`을 종료하고 그 session의 다른 attempt와 Pipe도 terminal failure로 수렴시킨 뒤, managed reconnect는 새 session identity로 시작한다. timeout된 attempt, Pipe와 payload는 replay하지 않는다.

SDK session의 outbound write는 cancellation과 configured deadline에 종속되어야 한다. frame write가 그 bound 안에 끝나지 않으면 해당 session을 재사용하지 않고 transport loss와 같은 cleanup·reconnect 경로로 수렴시킨다. 단순한 application `Pipe.read()` idle은 control operation timeout이 아니며 session을 닫는 근거가 아니다.

RelayGate SDK는 periodic heartbeat를 필수로 사용하지 않는다. 아무 operation도 없는 silent network blackhole은 즉시 탐지된다고 보장하지 않으며, local close, transport event 또는 active control operation의 deadline에서 failure를 관찰한다.

## SDK runtime lifetime

명시적인 runtime `close`는 managed reconnect와 current session을 종료한다. 개별 clone이나 하나의 Listener handle을 drop하는 것만으로 shared runtime을 닫아서는 안 된다. 반대로 해당 runtime을 사용할 수 있는 마지막 public owner가 사라지면 background reconnect가 스스로를 영구 소유해서는 안 되며 runtime을 종료해야 한다. 이미 반환된 live `Pipe`는 자신의 I/O를 위해 runtime lifetime을 유지하는 owner로 취급한다.

Listener registration의 승인·거절·갱신 절차는 SPEC 003이, 상태와 retry 가능 오류는 SPEC 007이 정의한다.

## 요구사항

- **`SDK-001`**: `Connector.connect(ClientId)`는 정확히 하나의 Listener에 대한 Pipe 하나를 요청해야 한다.
- **`SDK-002`**: connect 성공은 상대 Pipe가 Listener SDK runtime에서 선택된 Listener handle의 bounded incoming queue에 적재된 뒤에만 반환해야 한다.
- **`SDK-003`**: incoming queue가 가득 찼을 때 기존 queued Pipe를 제거해 새 연결을 성공시켜서는 안 된다.
- **`SDK-004`**: `Listener.accept()`는 같은 Pipe를 두 번 반환하지 않고 distinct Pipe 하나를 반환해야 한다.
- **`SDK-005`**: pending accept의 취소는 해당 호출만 종료하고 Listener, sibling Listener handle과 다른 Pipe의 상태를 변경해서는 안 된다.
- **`SDK-006`**: Listener handle close와 해당 handle의 Pipe queue admission은 한 순서로 직렬화되어야 한다. close는 자신의 신규·대기·미수락 Pipe만 종료하고 shared `ListenerSession`, sibling Listener handle 또는 이미 accept된 Pipe를 암묵적으로 닫아서는 안 된다.
- **`SDK-007`**: Pipe는 방향별 byte 순서를 보존해야 하며 message boundary를 추가해서는 안 된다.
- **`SDK-008`**: SDK의 모든 incoming queue와 Pipe buffer는 bounded여야 하며 capacity 부족을 backpressure 또는 명시적 실패로 관찰시켜야 한다.
- **`SDK-009`**: write 성공을 peer application의 처리 성공으로 보고해서는 안 된다.
- **`SDK-010`**: write-direction shutdown 뒤에도 반대 방향 read를 허용해야 하며 full close는 idempotent해야 한다.
- **`SDK-011`**: SDK runtime은 일시적인 Gateway 단절 뒤 managed reconnect를 수행해야 한다.
- **`SDK-012`**: managed reconnect는 shared `ListenerSession` 하나를 새로 만들고 살아 있는 각 Listener handle의 desired `ClientId`를 그 session에 자동 재등록해야 한다.
- **`SDK-013`**: reconnect는 이전 connect 요청, Pipe 또는 payload를 자동 replay, migrate 또는 resume해서는 안 된다.
- **`SDK-014`**: pending connect 또는 accept는 성공, 실패, 취소 중 하나의 terminal 결과만 반환해야 한다.
- **`SDK-015`**: transport 상실의 영향을 받은 Pipe는 terminal failure로 종료해야 하며 복구된 session에 붙여서는 안 된다.
- **`SDK-016`**: Listener의 최초 등록 성공은 `ClientKey` 검증과 Gateway-local binding 설치가 끝난 뒤 반환해야 한다. RT registration 성공 여부는 별도의 상태이며, 등록 성공을 remote discovery 완료로 해석해서는 안 된다.
- **`SDK-017`**: credential 거절 또는 등록 권한 폐기로 `BLOCKED`가 된 Listener handle은 자동 재등록하거나 같은 handle을 다시 활성화해서는 안 된다. sibling handle, shared session과 `BLOCKED` 전 admission을 마친 queued·accepted Pipe를 유지해야 하며, 새 credential 적용은 기존 handle을 닫은 뒤 새 `listen` operation으로 수행한다.
- **`SDK-018`**: Connector SDK runtime은 재연결마다 새 `ConnectorSessionId`의 session을 사용해야 한다. 이전 session의 outbound path에 commit된 connect request, attempt와 Pipe를 새 session으로 replay, migrate 또는 resume해서는 안 된다.
- **`SDK-019`**: 같은 Listener SDK runtime에서 동일 `ClientId`의 pending `ListenAttempt`와 non-`CLOSED` Listener handle은 합쳐서 최대 하나여야 한다. 나머지 생성 호출은 `ALREADY_EXISTS`이고 새 registration, binding 또는 incoming queue를 만들지 않아야 하며, attempt의 terminal 실패 또는 기존 handle의 `CLOSED` 뒤에는 새 생성이 가능해야 한다.
- **`SDK-020`**: `shutdown(write)`는 같은 방향에서 먼저 수락한 bytes 뒤에 peer EOF를 전달하고 그 방향의 이후 write를 실패시켜야 한다. 반대 방향은 독립적으로 `FIN`할 때까지 계속 사용할 수 있으며 양방향 `FIN` 뒤 Pipe는 정상 종료해야 한다.
- **`SDK-021`**: `CLOSE`는 정상적인 전체 종료, `RESET`은 실패에 의한 전체 종료로 관찰되어야 한다. 둘 다 양방향을 terminal로 만들고 idempotent해야 한다. 서로 다른 terminal 신호가 경쟁하면 먼저 확정된 결과만 유지하고 뒤의 신호가 정상·실패 의미를 바꾸지 못한다. `CLOSE`를 payload delivery acknowledgement로 해석해서는 안 된다.
- **`SDK-022`**: connect request는 한 live `ConnectorSession`의 bounded outbound path에 최대 한 번 commit되어야 한다. commit 전 호출만 새 session 준비를 기다릴 수 있고, commit 뒤 terminal 결과를 확인하지 못하면 queue 미도달을 증명할 수 있는 경우를 제외하고 `MAYBE_OBSERVED`로 끝내며 자동 replay해서는 안 된다.
- **`SDK-023`**: commit된 connect가 operation deadline까지 terminal 응답을 받지 못하면 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`로 끝내고 current `ConnectorSession`을 종료해야 한다. 그 session의 다른 attempt와 Pipe도 terminal cleanup하며 새 session으로 replay, migrate 또는 resume해서는 안 된다.
- **`SDK-024`**: SDK session의 outbound frame write는 cancellation과 configured deadline 안에 끝나야 한다. write 실패 또는 deadline은 해당 session을 종료하고 managed reconnect 경로로 수렴시켜야 하며 pending operation과 Pipe를 무기한 붙잡아서는 안 된다.
- **`SDK-025`**: 명시적 runtime close는 current session과 managed reconnect를 종료해야 한다. 개별 clone drop은 shared runtime을 닫지 않지만, live `Pipe`를 포함하여 runtime을 사용할 마지막 public owner가 사라지면 background task는 runtime을 자기 소유로 남기지 않고 종료해야 한다.
- **`SDK-026`**: periodic heartbeat 없이 failure-on-use를 허용한다. idle session의 silent network failure 탐지 시간을 보장해서는 안 되며, application Pipe read idle만으로 session을 닫아서는 안 된다.
- **`SDK-027`**: commit된 최초 `REGISTER`가 명시적 실패, response deadline 또는 session 상실로 terminal 성공을 확인하지 못하면 해당 `ListenAttempt`를 한 번만 실패시키고 reservation을 제거해야 한다. response deadline은 current `ListenerSession` 전체를 종료해야 한다. SDK는 pending attempt를 새 session으로 옮기거나 같은 request를 replay해서는 안 되며, 이미 반환된 current Listener만 bounded reconnect backoff 뒤 새 session identity와 새 registration request로 재등록해야 한다. TCP 연결 성공만으로 연속 failure backoff를 초기화해서는 안 되며 반환된 Listener의 recovery registration 성공 뒤 초기화할 수 있다.

## 이 SPEC에서 정하지 않는 것

- Listener registration API와 credential 처리
- binding resolve와 selection
- Gateway 간 relay와 wire protocol
- 오류 enum 값과 상태 이름
- timeout, reconnect backoff와 queue 크기의 기본값
- 구현 언어, signature와 module layout
