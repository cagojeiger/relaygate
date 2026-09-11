# RFC 7518: JSON Web Algorithms

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc7518.html) |
| 성격 | Standards Track |
| 목적 | JOSE에서 사용하는 cryptographic algorithm과 identifier 정의 |

```text
ES256 = ECDSA using P-256 and SHA-256
```

RelayGate는 operation grant 검증 알고리즘을 ES256으로 고정한다. 이 선택은 private key 보관,
발급 API와 key rotation 절차를 정의하지 않는다. 구현은 원문의
[ECDSA](https://www.rfc-editor.org/rfc/rfc7518.html#section-3.4)를 참고한다.
