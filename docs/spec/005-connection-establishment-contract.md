# SPEC 005: dial과 연결 수립 계약

```text
Relay A DIAL(dst)
  -> Entry GW local lookup
  -> local miss이면 authority RT Resolve
  -> self Binding 제외
  -> current candidate 하나 선택
  -> local OFFER 또는 Owner GW로 one hop OPEN
  -> Listener queue admission
  -> OPENED
  -> Pipe 1개
```

- **`DIAL-001`**: ConnectionId는 RelaySession 안에서 단조 증가하며 overflow는 terminal resource 오류다.
- **`DIAL-002`**: Gateway는 `(SessionId, ConnectionId)` 중복·역행 DIAL을 거절한다.
- **`DIAL-003`**: local lookup은 RT보다 우선한다.
- **`DIAL-004`**: remote lookup은 dial attempt마다 authority shard에 한 번 수행한다.
- **`DIAL-005`**: 같은 session 소유 Binding을 제외하고 후보 하나만 선택한다.
- **`DIAL-006`**: 후보가 없으면 `NOT_FOUND`, self 후보만 있으면 `FAILED_PRECONDITION`이다.
- **`DIAL-007`**: selected Binding 실패 시 같은 attempt에서 fallback·reroute·replay하지 않는다.
- **`DIAL-008`**: OFFER deadline은 selected RelaySession 전체를 종료해 불확실한 queue state를 정리한다.
- **`DIAL-009`**: caller가 dial future를 취소하면 current PipeId에 `CANCEL`을 보내고 sibling은 유지한다.
- **`DIAL-010`**: `OPENED`는 Listener queue admission만 뜻한다.

## observation

| 값 | 의미 |
| --- | --- |
| `NOT_OBSERVED` | selected Listener queue admission이 없음을 증명할 수 있음 |
| `MAYBE_OBSERVED` | commit 뒤 응답 유실로 admission을 부정할 수 없음 |
| `OBSERVED` | caller SDK가 `OPENED`를 확인함 |

observation은 payload delivery나 application 처리 증명이 아닙니다.
