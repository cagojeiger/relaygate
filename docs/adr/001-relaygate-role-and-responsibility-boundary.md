# ADR 001: RelayGate의 역할

## 배경

연결 중계의 경계를 명확히 하지 않으면 제품 책임이 저장, 재전송, 워크플로까지 확장된다.

## 결정

RelayGate는 **주소로 찾을 수 있는 임시 양방향 Pipe**를 연결하고 중계한다.

- 인증된 namespace 안에서 현재 Listener를 찾는다.
- Pipe를 열고, 제한된 backpressure 안에서 불투명 payload를 전달하며, 종료를 전파한다.
- SDK session, local Listener binding, Pipe, buffer, payload는 Gateway process memory에만 존재한다.
- Controller는 durable Raft에 현재 `GatewaySession`과 exact route로 구성된 control-plane directory만 저장한다. 이 현재 상태만의 예외와 복구 경계는 [ADR 002](002-current-state-cluster-and-recovery.md)가 정의한다.

RelayGate는 애플리케이션·Pipe·payload의 durable storage, message queue, pub/sub, application routing, workflow, application work, Open/Pipe/payload의 retry·replay·resume을 제공하지 않는다. Control/SDK session의 fresh reconnect는 이 금지와 별개다. 연결이 사라진 뒤의 다음 연결은 새 session, 새 Listener declaration 또는 새 Pipe다.

## 결과

- RelayGate는 현재 reachability와 connection state만 다루며 application outcome이나 payload history를 보존하지 않는다.
- business result 저장, deduplication, retry는 애플리케이션 책임이다.
- 새 기능은 Pipe discovery, connection, forwarding, termination에 직접 필요한 경우에만 포함한다.
