# ADR 016: 공개 Relay는 TLS가 구현될 때까지 loopback만 bind한다

## 배경

[ADR 003](003-protocol-boundaries.md)은 "Public Relay는 TLS termination 뒤에서 제공한다"고 기록했고, [ADR 006](006-client-isolation-and-credentials.md)은 "Non-loopback Public Relay는 TLS가 제공될 때만 허용한다"고 기록했다. 두 문장 모두 TLS를 조건으로 두면 non-loopback bind가 가능하다는 의미로 읽힌다.

실제 구현은 다르다. 설정 검증은 공개 Relay의 bind 주소가 loopback이 아니면 TLS 제공 여부와 무관하게 기동을 거부한다. 저장소에 TLS 설정 항목과 termination 구현이 없으므로 조건을 만족시킬 방법 자체가 없다.

`SPEC 002`는 "Bearer 인증 정보를 TLS로 보호하기 전에는 공개 Relay를 loopback에만 bind할 수 있다"로 정확히 서술하고 있어, 두 ADR과 SPEC이 서로 어긋난 상태다.

이 결정은 ADR 003과 ADR 006의 해당 문장을 대체한다. 두 문서의 원문은 고치지 않고 Git history에 남긴다.

## 결정

공개 Relay의 전송 보안은 현재 다음과 같다.

- 공개 Relay bind 주소는 **loopback이어야 한다**. 조건부 예외는 없다.
- 비 loopback 주소는 시작 설정 검증에서 거부하며 프로세스는 기동하지 않는다.
- TLS termination은 구현되어 있지 않다. 설정 항목도 없다.
- 따라서 현재 배포 형태는 신뢰할 수 있는 로컬 또는 개발 네트워크로 제한된다.

내부 제어, Peer, Raft 전송의 신뢰 조건은 이 결정의 범위 밖이며 [ADR 003](003-protocol-boundaries.md)이 정한 배포 경계를 그대로 따른다. 이들 역시 현재 인증이나 mTLS가 없다.

비 loopback 공개 Relay를 허용하려면 다음이 모두 필요하며, 그때 별도 결정으로 기록한다.

1. TLS termination 구현 또는 termination proxy를 전제로 한 명시적 배포 계약
2. 인증서·키 수명주기를 다루는 설정 표면
3. 검증 규칙 완화와 그에 대응하는 검증 근거

## 결과

- 현재 릴리스는 공개 네트워크에 노출되는 배포를 지원하지 않는다. 이는 미구현이지 설정 실수가 아니다.
- Bearer API key가 평문으로 전송될 수 있는 경로가 원천 차단된다. 검증이 loopback을 강제하므로 운영자가 실수로 노출할 수 없다.
- Compose와 로컬 개발 구성은 loopback 제약 안에서 동작한다. Gateway 간 통신은 컨테이너 네트워크 경계 안에서 이루어진다.
- ADR 003과 ADR 006의 TLS 관련 문장은 이 결정으로 대체되었다. 두 문서를 참조할 때 전송 보안 항목은 이 문서를 함께 본다.
