# RFC 5880: Bidirectional Forwarding Detection

- 원문: [RFC Editor HTML](https://www.rfc-editor.org/rfc/rfc5880.html)
- 문서 정보와 갱신 관계: [RFC Editor Info](https://www.rfc-editor.org/info/rfc5880)
- 제목: *Bidirectional Forwarding Detection (BFD)*
- 분류: Standards Track

## 목적

BFD는 인접한 두 forwarding system 사이의 양방향 경로 장애를 낮은 부하와 짧은
시간 안에 감지하기 위한 protocol이다. media, data protocol과 routing protocol에
독립적인 생존 감지 기능을 제공한다.

## 핵심 개념

```text
session identity   = My Discriminator / Your Discriminator
session state      = AdminDown | Down | Init | Up
failure detection  = negotiated interval과 Detect Mult로 계산한 감지 시간
```

- asynchronous mode에서는 양쪽이 주기적으로 control packet을 보내고, 정해진 감지
  시간 동안 유효한 packet을 받지 못하면 session을 `Down`으로 판단한다.
- demand mode에서는 별도의 연결성 검증 수단이 있다는 전제 아래 주기적 송신을 줄이고,
  필요할 때 명시적으로 경로를 검증한다.
- discriminator는 같은 system pair 사이에 여러 session이 있을 때 각 session을
  식별한다.
- 송신 주기에는 동기화된 control packet burst를 줄이기 위한 jitter가 적용된다.
- 감지 결과는 경로의 `Up` 또는 `Down` 상태다. application 요청 처리 성공, payload
  전달 확인이나 장애 원인까지 증명하지 않는다.

## 적용 범위에 대한 주의

BFD는 장애 감지 protocol이지 routing, failover 또는 application recovery 정책이
아니다. timer 협상, 상태 전이, 인증과 packet format의 정확한 규칙은 RFC 원문을
따른다.
