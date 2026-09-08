# TEST 005: Helm 계약

```text
기본: GW × 1 + RT shard × 1
확장 검증: values override → GW × 3 + RT shards × 2
인증서: 사용자가 준비한 Secret → 동일 mount 경로
```

| 범주 | 검증 계약 |
| --- | --- |
| resource scope | GW/RT, Service, directory, probe; Certificate·Issuer·Secret 생성 0개 |
| component release | GW와 RT image가 서로의 Pod template을 유지 |
| chart release | package version만 바뀌면 Pod template 유지 |
| identity | Gateway pod name, RT ordinal, 동일 ShardDirectory |
| TLS isolation | edge/internal trust 분리, 각 role의 leaf만 mount, 표준 Secret key 사용 |
| plaintext | 내부 certificate mount 없이 SDK TLS·ClusterToken 유지 |
| platform policy | 범용 annotations는 StatefulSet metadata에 전달, Pod template·다른 role 유지 |
| security | non-root, read-only Secret, persistent volume 0개 |
| invalid config | 제거된 source/certManager/autoReload/reloadToken, 빈 Secret 이름, non-string annotation 거절 |

CI는 Helm lint·render, 최소 기본 구성, 운영 override, Pod-template isolation과 Secret mount를 검증한다.
`.github/scripts/helm_transport_test.py`는 인증서 공급 도구와 무관한 chart 계약을 소유한다.

## Kind platform acceptance

| 구성 | 공급 / 판정 |
| --- | --- |
| mTLS | 테스트가 trust·GW·RT Secret을 먼저 생성 |
| plaintext | 내부 Secret 없이 동일 RT2/GW3 기능 검증 |
| cert-manager | 테스트 platform이 Issuer·Certificate·Reloader를 설치하고 values로 기존 Secret·watch 지정 |

`tests/kind/internal-certificates.yaml`과 `cert-manager-values.yaml`은 chart 외부 설정이다.
key spec 변경 → 새 serial → 해당 role Pod 교체 → 다른 role 유지 → SDK 재등록·fresh dial을 검증한다.
edge는 각 Gateway·Envoy passthrough의 제공 serial을 비교한다.
테스트 CA의 재발급 적용 검증이며 실제 ACME 발급·만료 시각까지의 대기·기존 Pipe 연속성 검증과 구분한다.
