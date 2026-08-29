# RFC 8656: Traversal Using Relays around NAT (TURN)

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc8656.html)
- 성격: Standards Track, 2020년 2월

## 범위

TURN은 direct communication이 어려운 host가 relay resource를 할당하고 peer와 packet을
교환하는 control/data 절차를 정의한다.

## 핵심

- allocation은 relay address와 관련 state를 가진 임시 resource다.
- allocation에는 time-to-expiry가 있고 Allocate 또는 Refresh가 timer를 설정한다.
- data 전송 자체는 allocation을 refresh하지 않는다.
- expiry에 도달하면 allocation 관련 state를 해제할 수 있다.
- Refresh에서 lifetime zero를 요청하면 allocation을 명시적으로 삭제할 수 있다.
- permission과 channel binding은 allocation과 구별되는 별도 lifetime을 가진다.

## 구분할 점

- TURN allocation, permission과 channel은 서로 다른 resource다.
- NAT traversal, ICE 연계, peer IP address와 datagram 절차는 TURN 고유 계약이다.
- TURN의 기본 lifetime 값은 다른 soft-state system의 일반 기본값이 아니다.

## 읽을 절

- [§3.2 Allocations](https://www.rfc-editor.org/rfc/rfc8656.html#section-3.2)
- [§6 Allocations](https://www.rfc-editor.org/rfc/rfc8656.html#section-6)
- [§8 Refreshing an Allocation](https://www.rfc-editor.org/rfc/rfc8656.html#section-8)
- [§9 Permissions](https://www.rfc-editor.org/rfc/rfc8656.html#section-9)
