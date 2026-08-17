# ADR 008: Cross-Gateway hop과 replay fence

## Context

Ingress와 Listener owner가 다를 때도 하나의 일시적 Pipe 계약을 유지해야 한다. 이를 durable queue나
directory로 만들면 RelayGate의 범위를 넘는다.

## Decision

```text
Caller --public--> Ingress ==dedicated internal bidi stream==> Owner --public--> Listener
```

- Owner address는 `Hello`의 exact current control session에만 memory로 묶는다.
- Remote Pipe마다 internal gRPC stream 하나를 사용한다. Multiplex, redial, resume은 없다.
- Authority가 발급한 context는 epoch/authority, ingress session, auth, exact binding, owner session/address와
  expiry를 묶는다. Ingress는 자기 provenance를 forwarding 전에 검증한다.
- Owner의 `O`는 current authority/session/auth/binding/capacity와 strict expiry를 다시 확인하고
  `AttemptId` reservation을 원자적으로 삽입한다. 그 뒤에만 Listener에게 offer한다.
- Successful reservation은 같은 owner process에서 expiry까지 유지한다. Duplicate는 이전 response나
  `PipeId`를 replay하지 않고 fail closed한다.
- Listener accept가 Open 선형화점이며 여기서 `PipeId`를 만든다. 이후 response/hop loss는 caller에게
  `Unknown`일 수 있다.
- O 이전 authority/owner-session 변경은 context를 무효화한다. O 이후 attempt는 volatile Pipe lifecycle을
  따르며 retry, reconnect, resume과 payload replay를 하지 않는다.

Absolute expiry는 `ClockSkewBound < relay.open_timeout` 배포 가정을 요구한다. 현재 internal peer listener는
authentication/mTLS가 없으므로 trusted local/dev network에서만 사용한다.

## Consequences

- Ingress와 Owner는 같은 logical `PipeId`의 자기 segment만 소유한다.
- Owner crash 뒤 replay cache와 정확한 outcome은 복구할 수 없다.
- Production readiness에는 peer authentication/mTLS와 clock-skew evidence가 필요하다.
