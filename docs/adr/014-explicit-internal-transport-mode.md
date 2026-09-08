# ADR 014: 내부 전송은 mTLS 기본값과 명시적 plaintext를 제공한다

| 경계 | 결정 |
| --- | --- |
| SDK ↔ Gateway | TLS와 ClusterToken 유지 |
| GW ↔ GW·RT 기본값 | `mtls` |
| 격리된 테스트 설치 | `plaintext` 명시 선택 |
| 내부 인증 | 두 모드 모두 GatewayName/key와 incarnation handshake |
| mode 오류·TLS 실패 | 명시적 실패, 평문 fallback 없음 |

```text
SDK ── TLS ── Gateway ── mtls | plaintext ── Gateway / RouteTable
```

`plaintext`는 내부 인증서·CA 없이 실행하며 내부 키와 payload를 암호화하지 않는다.
운영자는 접근 가능한 네트워크와 trust domain을 제한한다.

Gateway와 RouteTable은 같은 내부 전송 모드를 사용한다. 모드 전환은 maintenance window에서
coordination하며 existing Pipe 연속성 대신 SDK reconnect·republish로 회복한다.

Helm은 모드별 env·Secret mount를 소유한다. `autoReload`는 mTLS에서만 사용할 수 있고,
설치된 Reloader가 role별 leaf·공개 trust Secret 변경을 StatefulSet rollout으로 적용한다.
CA 서명키는 Issuer가 사용하며 GW/RT에는 전달하지 않는다.
