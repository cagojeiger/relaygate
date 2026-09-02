# RFC 7426: SDN layers and architecture terminology

- 원문: [RFC Editor HTML](https://www.rfc-editor.org/rfc/rfc7426.html)
- 문서 정보: [RFC Editor Info](https://www.rfc-editor.org/info/rfc7426)
- 제목: *Software-Defined Networking (SDN): Layers and Architecture Terminology*
- 분류: Informational, IRTF

## 목적

RFC 7426은 Software-Defined Networking을 특정 구현이나 protocol로 규정하지 않고,
network 기능을 설명하기 위한 plane, abstraction layer와 interface 용어를 정리한다.

## Plane

| Plane | 책임 |
| --- | --- |
| Application Plane | network service를 사용하거나 network 동작을 정의하는 application과 service |
| Control Plane | traffic을 어떻게 forwarding할지 결정하고 forwarding plane에 반영 |
| Forwarding Plane | control plane의 결정에 따라 실제 data path를 처리 |
| Operational Plane | port, interface, queue, memory와 같은 network resource의 현재 운영 상태 |
| Management Plane | network element의 설정, 감시와 유지보수 |

Control Plane과 Management Plane은 관심 대상, 변경 주기, persistence와 locality가
다를 수 있지만 모든 system에서 동일한 process 경계로 구현되어야 하는 것은 아니다.
하나의 physical 또는 virtual network element가 여러 plane의 기능을 함께 가질 수
있다.

## Abstraction layer

문서는 Device and resource Abstraction Layer, Control Abstraction Layer, Management
Abstraction Layer와 Network Services Abstraction Layer를 구분한다. 이 layer들은 서로
다른 plane의 resource와 service를 interface 뒤에 숨기는 개념적 경계다.

## 적용 범위에 대한 주의

RFC 7426은 특정 controller, southbound protocol, 배포 topology나 orchestration 방식을
요구하지 않는다. 세부 layer와 interface 정의는 RFC 원문을 따른다.
