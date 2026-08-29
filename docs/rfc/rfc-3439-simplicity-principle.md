# RFC 3439: Simplicity Principle

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc3439.html)
- 제목: *Some Internet Architectural Guidelines and Philosophy*
- 분류: Informational

## 목적

대규모 network에서 complexity가 scale, 비용과 reliability에 미치는 영향을 설명하고 가능한 한 단순한 architecture를 선택하도록 안내한다.

## 핵심 개념

- complexity는 효율적인 scaling을 방해하고 운영 비용을 높이는 주요 요인이다.
- end-to-end argument는 core의 기능과 endpoint state 상호작용을 줄이는 방향을 지지한다.
- optimization과 feature는 실제 필요가 증명되기 전에 core architecture를 복잡하게 만들 수 있다.
- 계층과 구성 요소의 결합은 failure와 변경의 영향을 증폭할 수 있다.
- 대규모 system은 가능한 한 단순한 service path와 명확한 responsibility boundary를 가져야 한다.

```text
simple core
    + explicit boundary
    + endpoint-owned complexity
    = lower scaling and operational cost
```

## 오해하지 말아야 할 점

- 모든 optimization이나 기능이 해롭다는 뜻이 아니다.
- 단순성을 위해 필요한 resource protection이나 correctness를 생략하라는 뜻이 아니다.
- 이 문서만으로 특정 topology나 protocol이 결정되지는 않는다.

세부 논거와 원문 표현은 RFC Editor 원문을 따른다.
