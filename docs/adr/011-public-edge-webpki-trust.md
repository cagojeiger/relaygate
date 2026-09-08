# ADR 011: 공개 edge 인증서는 Web PKI roots로 검증한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted, implemented |
| private edge | explicit custom CA |
| public edge | bundled Web PKI roots |
| internal mTLS | explicit private CA |

## 결정

```text
custom CA    -> ca.crt + tls.crt + tls.key
Web PKI      -> bundled roots + tls.crt + tls.key
both modes   -> server name + relaygate/2 ALPN + ClusterToken
```

SDK와 Gateway readiness는 같은 trust source와 server name으로 certificate를 검증합니다. 공개 CA mode는
server chain과 private key를 사용하고 trust anchor는 SDK/runtime의 bundled root set에서 가져옵니다.

| 운영 축 | 적용 |
| --- | --- |
| ACME edge Secret | Web PKI mode로 직접 사용 |
| private deployment | custom CA mode 사용 |
| public root 갱신 | SDK/runtime release |
| internal CA 갱신 | internal mTLS trust domain 절차 |

## 참고

- [cert-manager Certificate](https://cert-manager.io/docs/usage/certificate/)
- [webpki-roots](https://crates.io/crates/webpki-roots)
- [RFC 8446 §4.4.2](https://www.rfc-editor.org/rfc/rfc8446.html#section-4.4.2)
