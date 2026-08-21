# ADR 008: Cross-Gateway Pipe

## 배경

Caller ingress와 Listener owner가 달라도 하나의 temporary Pipe contract를 보존해야 한다. 이를 durable queue나 reconnect protocol로 만들면 RelayGate의 책임을 벗어난다.

## 결정

```text
Caller --public--> Ingress ==internal bidi stream==> Owner --public--> Listener
```

- Owner address는 current control session memory에만 존재한다.
- Ingress는 exact `(Owner GatewayId, GatewayInstanceId, relay address)`마다 하나의 shared gRPC/HTTP2 `ClientConn`을 유지한다.
- 각 remote Pipe는 shared connection 위에 독립된 internal bidirectional stream 하나를 사용한다.
- Owner identity/address가 바뀌면 새 Open은 새 connection을 사용하고, 이전 connection은 그 위의 기존 Pipe가 모두 끝난 뒤 닫는다.
- Active stream이 없는 connection은 bounded LRU idle cache에만 남으며 상한을 넘은 과거 owner connection은 닫는다.
- Authority는 ingress, owner, auth, exact binding에 묶인 expiring single-use Open context를 발급한다.
- Owner는 context와 current local binding을 다시 검증한 뒤 attempt를 atomic reserve한다.
- 이미 reserve된 attempt는 response나 `PipeId`를 replay하지 않고 fail closed한다.
- Listener accept가 Open linearization point이며 Owner가 이때 `PipeId`를 만든다.
- Ingress와 Owner는 동일 logical `PipeId`의 자기 segment만 소유한다.
- 각 방향은 FIFO이며 buffer와 wait는 bounded다.
- 여러 Pipe를 multiplex하는 public Relay stream은 control/terminal과 payload를 별도 bounded lane으로 보내 ready control/terminal이 queued payload pressure를 우회한다.
- Pipe 하나를 운반하는 internal peer stream은 모든 send를 하나의 bounded lane에서 직렬화한다. Blocked send가 timeout/cancel되면 해당 Pipe를 terminalize하고 stream만 취소하며 shared ClientConn과 sibling Pipe stream은 유지한다.
- Internal hop은 payload redial, retry, resume, replay를 하지 않는다.
- [ADR 013](013-payload-delivery-receipts.md)은 durable payload storage, hop retry, Pipe resume, application processing acknowledgement 없이 exact end-to-end SDK queue-admission receipt를 추가한다.

Open이 linearize된 뒤 response나 hop이 사라지면 caller outcome은 `Unknown`일 수 있다. Caller는 같은 request에 다시 붙지 않고 새 Open을 시작한다.

## 결과

- Cross-Gateway path는 same-Gateway와 동일한 volatile Pipe semantics를 가진다.
- Connection 수는 SDK session 수가 아니라 active owner Gateway identity와 bounded idle cache에 비례하고, stream 수는 active remote Pipe 수에 비례한다.
- 한 peer stream 장애는 shared connection의 sibling Pipe를 종료하지 않는다. Connection-level 장애만 그 connection의 stream 전체에 영향을 준다.
- Attempt outcome과 payload는 Gateway crash 뒤 복구하지 않는다.
- Expiring context는 bounded deployment clock skew를 가정한다.
- Internal peer transport는 authentication/mTLS가 제공되기 전까지 trusted local/dev network로 제한한다.
