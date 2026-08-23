# ADR 005: 만료는 확인된 리더가 명령으로 제안한다

## 배경

[ADR 004](004-control-state-authority-split.md)에 따라 `V`는 리더 임기 동안만 존재한다. 리더가 바뀌면 `C`에는 `GatewaySession`과 route가 남아 있지만 어떤 Gateway가 실제로 살아 있는지는 알 수 없다.

이 상태를 영원히 두면 죽은 Gateway의 route가 directory에 남는다. 그렇다고 리더 교체 즉시 지우면 정상 재시작 중인 Gateway까지 잃는다. 재연결할 시간을 주되 돌아오지 않으면 정리하는 규칙이 필요하다.

문제는 복제 상태 기계에서 시간 기반 만료를 구현하는 방법이다. 각 노드가 자기 시계로 만료를 판정하면 같은 시점에 노드마다 다른 상태를 갖게 되어 복제가 깨진다.

## 결정

만료는 시각을 복제하지 않고 **삭제 명령을 복제**한다.

- Grace deadline은 현재 리더의 시계로 계산하며 리더 메모리에만 존재한다. `C`에 기록하지 않는다.
- 만료 판정은 리더십을 확인한 리더만 수행한다. 판정 결과는 `RemoveGateway` 명령으로 Raft에 제안한다.
- 모든 노드는 이 명령을 동일하게 적용한다. 노드는 스스로 만료를 판정하지 않는다.
- `RemoveGateway`는 대상 인스턴스가 여전히 `C`의 current gateway일 때만 적용된다. 교체된 인스턴스를 오래된 판정이 지울 수 없다.
- 적용되면 해당 `GatewaySession`과 그 Gateway가 소유한 route를 연쇄 삭제한다.

Deadline은 세 지점에서 등록한다.

| 지점 | 이유 |
| --- | --- |
| 새 권한 주체 확립 | committed `C`의 모든 Gateway에 부여한다. 누가 살아 있는지 아직 모른다 |
| `OpenSession` 성공 | `C`만 확립된 상태다. 전체 snapshot이 오기 전까지는 부재 Gateway처럼 만료 대상이다 |
| `EndSession` | 세션이 끝났으므로 재연결 시간을 준다 |

Deadline은 해당 Gateway가 전체 snapshot으로 재검증에 성공하면 해제한다. 리더십을 잃으면 deadline 집합 전체를 버린다.

Grace 판정 시간은 `gateway_revalidation_timeout`이며 기본값은 15초다. 판정은 `authority_probe_interval` 주기로 리더십을 확인한 뒤에만 수행한다.

연결이 끊겼다는 사실 자체는 grace 타이머가 아니라 제어 평면 keepalive가 보장한다. keepalive는 10초 주기 PING과 5초 응답 대기로 구성되며, TCP가 half-open으로 남는 경우에도 감지 상한을 정한다.

## 결과

- 복제 상태 기계의 결정성이 유지된다. 노드 간 시계 차이는 만료 시점을 앞당기거나 늦출 뿐 상태를 갈라지게 하지 않는다.
- 리더 교체는 grace를 새로 시작한다. 리더가 바뀌면 모든 Gateway가 어차피 재연결해야 하므로 이는 정상 동작이다.
- Quorum을 잃으면 만료도 멈춘다. 삭제 명령을 제안할 수 없기 때문이며, directory가 오래된 채 남는 것이 임의 삭제보다 안전하다.
- Gateway가 grace 안에 재연결하지 못하면 route를 잃는다. 재연결 후 현재 Listener를 다시 bind해야 하며 이전 선언은 재생되지 않는다.
- 만료 판정 자체는 관찰되지 않는다. 관찰 가능한 것은 `RemoveGateway` 적용 결과뿐이다.
