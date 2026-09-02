# ADR 008: 운영 health 신호를 실패 영역별로 분리한다

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 전제 | [ADR 004](004-current-state-routing-topology.md), [ADR 005](005-soft-state-registration-lifecycle.md), [ADR 007](007-transport-liveness-and-idle-retirement.md) |

## 맥락

Gateway process, SDK session admission과 RouteTable 의존성은 서로 다른 실패 영역이다. 이를
하나의 health 값으로 합치면 RT 단절이 Gateway 재시작이나 live local Pipe 종료로 잘못 전파될
수 있다.

## 결정

```text
ProcessLiveness       = process와 critical runtime이 진행 가능한가
SdkAdmissionReadiness = 새 SDK session이 HELLO -> WELCOME을 완료할 수 있는가
RouteDependencyHealth = remote registration과 Resolve에 필요한 RT 의존성이 사용 가능한가
```

세 신호는 서로를 대신하지 않는다.

```text
RT 단절
  -> RouteDependencyHealth 저하
  -> ProcessLiveness와 SdkAdmissionReadiness를 직접 변경하지 않음
  -> local binding과 established Pipe 유지

SDK admission capacity 소진
  -> SdkAdmissionReadiness 저하 가능
  -> ProcessLiveness와 RouteDependencyHealth를 직접 변경하지 않음

critical runtime의 격리 불가능한 실패
  -> process 종료와 supervisor 재시작으로 수렴
```

기존 `relaygate-server check`는 SDK protocol admission을 확인하는
`SdkAdmissionReadiness` probe다. process 생존은 배포 환경의 process supervision으로
관찰한다. `RouteDependencyHealth`는 Gateway가 마지막으로 관찰한 current dependency 상태이며
routing truth나 payload 전달 증명이 아니다.

## 결과

- RT 장애를 이유로 live local state나 established Pipe를 제거하지 않는다.
- SDK 신규 연결 수락 불가와 process failure를 구분할 수 있다.
- local-only Gateway는 RT 의존성이 없음을 명시적으로 나타낼 수 있다.
- 운영자는 하나의 신호로 전체 system health를 추론할 수 없다.

## 이 ADR에서 정하지 않는 것

- probe endpoint, port와 wire format 추가
- timeout, polling interval과 배포 probe 설정값
- Prometheus와 OpenTelemetry exporter
- application health, payload 처리 성공과 delivery acknowledgement
- RT replication, consensus와 online shard reconfiguration

## 참고

- [RFC 5880](../rfc/rfc-5880-bfd-liveness.md)
- [RFC 7426](../rfc/rfc-7426-sdn-architecture.md)
