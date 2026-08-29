# RFC 1958: Internet architecture 원칙

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc1958.html)
- 제목: *Architectural Principles of the Internet*
- 분류: Informational

## 목적

Internet architecture에서 반복적으로 유용했던 일반 설계 원칙을 기록한다. 정식 표준이나 불변 reference model은 아니다.

## 핵심 개념

- intelligence와 end-to-end 기능은 가능한 한 endpoint에 둔다.
- network 내부에 필요한 state는 topology와 activity 변화에 맞춰 self-healing해야 한다.
- 내부 state 양과 수동 설정을 최소화한다.
- connectivity가 남아 있다면 내부 state 손실은 영구 손상보다 일시적인 service denial로 수렴해야 한다.
- 같은 문제를 이미 해결한 일반적인 방법이 있으면 특별한 이유 없이 새 방법을 만들지 않는다.
- 단순성, scale-out과 modularity를 함께 고려한다.

```text
endpoint  ── end-to-end meaning and integrity
network   ── minimal, derived, self-healing state
```

## 오해하지 말아야 할 점

- network 내부에 어떤 state도 두지 말라는 뜻이 아니다.
- endpoint가 모든 transport와 routing 기능을 직접 구현하라는 뜻이 아니다.
- 이 문서만으로 특정 protocol, storage 또는 recovery mechanism이 결정되지는 않는다.

세부 논거와 원문 표현은 RFC Editor 원문을 따른다.
