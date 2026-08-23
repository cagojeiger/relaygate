# ADR 009: Gateway 간 Pipe

## 배경

호출자 진입 Gateway와 Listener 소유 Gateway가 달라도 하나의 임시 Pipe 계약을 보존해야 한다. 이를 영속 대기열이나 재연결 프로토콜로 만들면 RelayGate의 책임을 벗어난다.

## 결정

```text
Caller --public--> Ingress ==internal bidi stream==> Owner --public--> Listener
```

- 소유자 주소는 현재 제어 세션 메모리에만 존재한다.
- 진입 Gateway는 exact `(Owner GatewayId, GatewayInstanceId, relay address)`마다 하나의 공유 gRPC/HTTP2 `ClientConn`을 유지한다.
- 각 원격 Pipe는 공유 연결 위에 독립된 내부 양방향 stream 하나를 사용한다.
- 소유자 식별자나 주소가 바뀌면 새 Open은 새 연결을 사용하고, 이전 연결은 그 위의 기존 Pipe가 모두 끝난 뒤 닫는다.
- 활성 stream이 없는 연결은 상한이 있는 LRU 유휴 cache에만 남으며 상한을 넘은 과거 소유자 연결은 닫는다.
- 권한 주체는 진입 Gateway, 소유자, 인증, exact binding에 묶인 만료 가능한 일회용 Open context를 발급한다.
- 소유자는 context와 현재 로컬 바인딩을 다시 검증한 뒤 시도를 원자적으로 예약한다.
- 이미 예약된 시도는 응답이나 `PipeId`를 재생하지 않고 닫힌 실패로 처리한다.
- Listener 수락이 Open 선형화 지점이며 소유자가 이때 `PipeId`를 만든다.
- Ingress와 Owner는 동일 logical `PipeId`의 자기 segment만 소유한다.
- 각 방향은 FIFO이며 buffer와 wait는 bounded다.
- 여러 Pipe를 multiplex하는 public Relay stream은 control/terminal과 payload를 별도 bounded lane으로 보내 ready control/terminal이 queued payload pressure를 우회한다.
- Pipe 하나를 운반하는 internal peer stream은 모든 send를 하나의 bounded lane에서 직렬화한다. Blocked send가 timeout/cancel되면 해당 Pipe를 terminalize하고 stream만 취소하며 shared ClientConn과 sibling Pipe stream은 유지한다.
- 내부 구간은 payload 재연결, 재시도, 재개, 재생을 하지 않는다.
- [ADR 010](010-payload-delivery-receipts.md)은 durable payload storage, hop retry, Pipe resume, application processing acknowledgement 없이 exact end-to-end SDK queue-admission receipt를 추가한다.

Open이 선형화된 뒤 응답이나 구간이 사라지면 호출자 결과는 `Unknown`일 수 있다. 호출자는 같은 요청에 다시 붙지 않고 새 Open을 시작한다.

## 결과

- Gateway 간 경로는 같은 Gateway 내부 경로와 동일한 휘발성 Pipe 의미를 가진다.
- 연결 수는 SDK 세션 수가 아니라 활성 소유 Gateway 식별자와 상한이 있는 유휴 cache에 비례하고, stream 수는 활성 원격 Pipe 수에 비례한다.
- 한 Peer stream 장애는 공유 연결의 다른 Pipe를 종료하지 않는다. 연결 수준 장애만 해당 연결의 stream 전체에 영향을 준다.
- 시도 결과와 payload는 Gateway 장애 뒤 복구하지 않는다.
- 만료 context는 배포 환경 시계 오차의 상한을 가정한다.
- 내부 Peer 전송은 인증이나 mTLS가 제공되기 전까지 신뢰할 수 있는 로컬·개발 네트워크로 제한한다.
