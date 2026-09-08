# ADR 014: 내부 전송은 mTLS 기본값과 명시적 plaintext를 제공한다

| 경계 | 결정 |
| --- | --- |
| SDK ↔ Gateway | TLS와 ClusterToken 유지 |
| GW ↔ GW·RT 기본값 | `mtls` |
| 격리된 테스트 설치 | `plaintext` 명시 선택 |
| 내부 인증 | mTLS: 신뢰 CA + Gateway 역할 DNS SAN, plaintext: 인증 없음 |
| mode 오류·TLS 실패 | 명시적 실패, 평문 fallback 없음 |

```text
SDK ── TLS ── Gateway ── mtls | plaintext ── Gateway / RouteTable
```

`plaintext`는 내부 인증서·CA 없이 실행하는 무인증·무암호화 모드다.
운영자는 접근 가능한 네트워크와 trust domain을 제한한다.

GatewayName·GatewayId는 논리적 식별과 incarnation fencing에 사용한다. mTLS는 Gateway 역할을
인증하며 개별 Pod 이름과 인증서를 연결하지 않는다. 내부 공통 키는 사용하지 않는다.
전용 CA의 발급 정책으로 Gateway 역할 SAN과 clientAuth 용도를 제한한다.

Gateway와 RouteTable은 같은 내부 전송 모드를 사용한다. 모드 전환은 maintenance window에서
coordination하며 existing Pipe 연속성 대신 SDK reconnect·republish로 회복한다.

Helm은 모드별 env·Secret mount를 소유한다. `autoReload`는 mTLS에서만 사용할 수 있고,
설치된 Reloader가 role별 leaf·공개 trust Secret 변경을 StatefulSet rollout으로 적용한다.
CA 서명키는 Issuer가 사용하며 GW/RT에는 전달하지 않는다.
