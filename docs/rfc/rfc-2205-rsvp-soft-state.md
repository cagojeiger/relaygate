# RFC 2205: Resource ReSerVation Protocol (RSVP)

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc2205.html)
- 성격: Standards Track, 1997년 9월

## 범위

RSVP는 application data를 전달하거나 route를 계산하는 protocol이 아니라, 경로상의
resource reservation state를 만들고 유지하는 control protocol이다.

## 핵심

- RSVP state는 periodic refresh message로 유지되는 soft state다.
- refresh가 없으면 state는 timeout 후 자동으로 제거된다.
- 명시적 teardown은 빠른 제거를 돕지만 soft-state cleanup을 대체하지 않는다.
- control state와 data forwarding path는 분리된다.
- refresh와 timeout은 membership 및 route 변화에 점진적으로 수렴하게 한다.

## 구분할 점

- RSVP의 reservation, QoS, multicast 모델은 일반적인 registry protocol이 아니다.
- refresh-or-expire 원리는 재사용할 수 있지만 RSVP message와 time parameter가 그대로
  적용되는 것은 아니다.

## 읽을 절

- [§1 Introduction](https://www.rfc-editor.org/rfc/rfc2205.html#section-1)
- [§2.3 Soft State](https://www.rfc-editor.org/rfc/rfc2205.html#section-2.3)
- [§2.4 Teardown](https://www.rfc-editor.org/rfc/rfc2205.html#section-2.4)
- [§3.7 Time Parameters](https://www.rfc-editor.org/rfc/rfc2205.html#section-3.7)
