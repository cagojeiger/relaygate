# ADR 008: Cross-Gateway Pipe

## Context

Caller ingress와 Listener owner가 달라도 하나의 일시적 Pipe 계약을 유지해야 한다. 이를 durable queue나
reconnect protocol로 만들면 RelayGate의 책임을 넘는다.

## Decision

```text
Caller --public--> Ingress ==internal bidi stream==> Owner --public--> Listener
```

- Owner address는 current control session memory에만 둔다.
- Remote Pipe마다 internal gRPC bidi stream 하나를 사용한다.
- Authority는 ingress, owner, auth와 exact binding에 묶인 expiring single-use Open context를 발급한다.
- Owner는 context와 current local binding을 다시 확인한 뒤 attempt를 원자적으로 reserve한다.
- 이미 reserve된 attempt는 response나 `PipeId`를 replay하지 않고 fail closed한다.
- Listener accept가 Open의 선형화점이며 Owner가 여기서 `PipeId`를 만든다.
- Ingress와 Owner는 같은 logical `PipeId`의 자기 segment만 소유한다.
- 각 방향은 FIFO이고 buffer는 bounded다. Control과 terminal event는 payload보다 우선한다.
- Internal hop은 redial, retry, resume과 payload replay를 하지 않는다.

Open이 선형화된 뒤 response나 hop을 잃으면 caller outcome은 `Unknown`일 수 있다. 같은 request를 이어 붙이지
않고 새 Open을 시작한다.

## Consequences

- Cross-Gateway 경로도 same-Gateway와 같은 volatile Pipe 의미를 가진다.
- Gateway crash 뒤 attempt outcome과 payload를 복구하지 않는다.
- Expiring context는 deployment의 bounded clock-skew를 전제로 한다.
- Internal peer transport는 인증/mTLS가 제공되기 전까지 trusted local/dev network로 제한한다.
