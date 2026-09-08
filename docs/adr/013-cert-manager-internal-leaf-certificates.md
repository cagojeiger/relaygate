# ADR 013: 인증서 생명주기는 배포자가 소유한다

| 소유자 | 책임 |
| --- | --- |
| 배포자 / GitOps | Certificate·Issuer·CA·Secret 공급, 갱신과 rollout 정책 |
| Helm chart | 기존 Secret 참조·mount, 범용 workload annotation 전달 |
| runtime | startup 시 인증서 load, TLS·mTLS 검증 |

```text
사용자 선택: 수동 발급 / cert-manager / 기타 공급 도구
                         ↓
               trust · GW leaf · RT leaf Secret
                         ↓
                   Gateway / RouteTable
```

Gateway leaf는 Gateway 역할 SAN과 clientAuth/serverAuth를, RT leaf는 RT 역할 SAN과 serverAuth를 가진다.
runtime은 공개 trust와 자기 역할 leaf만 사용하며 CA private key를 소유하지 않는다.

갱신 Secret은 배포자의 rollout 정책으로 적용한다. CA rotation은 old/new trust overlap 후 leaf를 교체한다.
전송·논리적 handshake·SDK 재연결 계약은 인증서 공급 방식과 독립적이다.

## 참고

- [cert-manager Certificate](https://cert-manager.io/docs/usage/certificate/)
- [cert-manager CA Issuer](https://cert-manager.io/docs/configuration/ca/)
