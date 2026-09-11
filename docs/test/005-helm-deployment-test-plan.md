# TEST 005: Helm 계약

```text
기본: GW x 1 + RT shard x 1
확장: values override -> GW x 3 + RT shards x 2
Gateway authorization: 사용자 ConfigMap의 public-key JSON -> read-only mount
인증서: 사용자가 준비한 Secret -> read-only mount
```

| 범주 | 검증 계약 |
| --- | --- |
| resource scope | GW/RT, Service, directory, probe; authorization server·Certificate·Issuer·Secret 생성 0개 |
| authorization | `authorization.existingConfigMap/configKey`를 Gateway에만 mount하고 `RELAYGATE_AUTH_CONFIG_PATH` 고정 |
| key boundary | public ES256 JWK config만 mount; private key·token·JWKS fetch config 없음 |
| component release | GW와 RT image가 서로의 Pod template을 유지 |
| chart release | package version만 바뀌면 Pod template 유지 |
| identity | Gateway Pod name, RT ordinal, 동일 format-version 2 ShardDirectory |
| TLS isolation | edge/internal trust 분리, 각 role의 leaf만 mount, 표준 Secret key 사용 |
| plaintext | 내부 certificate mount 없이 SDK TLS·operation authorization 유지 |
| platform policy | 범용 annotations는 StatefulSet metadata에 전달, Pod template·다른 role 유지 |
| security | non-root, read-only ConfigMap/Secret, persistent volume 0개 |
| invalid config | 빈 authorization ConfigMap/key·Secret 이름, 제거된 legacy credential/cert-manager chart field, non-string annotation 거절 |

CI는 Helm lint·render, 최소 기본 구성, 운영 override, Pod-template isolation, ConfigMap과 Secret mount를 검증합니다.
`.github/scripts/helm_transport_test.py`는 issuer·certificate·token 발급 도구와 무관한 chart wiring만 소유합니다.

## Kind platform acceptance

| 구성 | 공급 / 판정 |
| --- | --- |
| authorization | 테스트가 public ES256 current/next key JSON ConfigMap 생성; application fixture가 private key로 token 발급 |
| mTLS | 테스트가 trust·GW·RT Secret을 먼저 생성 |
| plaintext | 내부 Secret 없이 동일 RT2/GW3 기능과 operation authorization 검증 |
| certificate renewal | 테스트 platform이 Issuer·Certificate·Reloader를 설치하고 chart values로 기존 Secret·watch 지정 |

`tests/kind/internal-certificates.yaml`과 `cert-manager-values.yaml`은 chart 외부 설정입니다. Key spec 변경 뒤
새 certificate serial, 해당 role Pod 교체, 다른 role 유지, SDK republish와 fresh authorized dial을 검증합니다.
실제 ACME 발급·만료 대기와 existing Pipe 연속성은 별도 운영 검증입니다.
