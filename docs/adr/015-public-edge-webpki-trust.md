# ADR 015: 공개 edge 인증서는 Web PKI roots로 검증한다

| 항목 | 값 |
| --- | --- |
| 상태 | 채택, 구현됨 |
| 관계 | ADR 012와 ADR 014의 SDK-facing TLS trust source를 확장 |

## 결정

```text
private edge CA  -> explicit custom CA
public edge CA   -> bundled Web PKI roots
internal mTLS    -> explicit private CA 유지
```

SDK와 Gateway readiness는 같은 trust source와 server name으로 SDK-facing certificate를 검증합니다.
공개 CA mode의 server Secret은 certificate chain과 private key만 가지며 root certificate를 요구하거나
server chain에서 trust anchor를 추출하지 않습니다. 두 mode 모두 `relaygate/2` ALPN과 ClusterToken 검증을
유지하고 실패 시 평문으로 전환하지 않습니다.

## 결과

- cert-manager ACME Secret을 별도 root 복사 없이 사용할 수 있습니다.
- private deployment는 기존 custom CA를 계속 사용할 수 있습니다.
- public root 갱신은 SDK/runtime release에 포함되며 internal mTLS trust domain과 분리됩니다.

## 참고

- [cert-manager Certificate](https://cert-manager.io/docs/usage/certificate/)
- [webpki-roots](https://crates.io/crates/webpki-roots)
- [RFC 8446 Section 4.4.2](https://www.rfc-editor.org/rfc/rfc8446.html#section-4.4.2)
