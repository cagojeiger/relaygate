# ADR 012: 모든 protocol transport를 TLS로 보호한다

| 항목 | 값 |
| --- | --- |
| 상태 | 부분 대체됨 — SDK transport와 배포 경계는 ADR 014 |
| 관계 | ADR 002의 infrastructure trust와 ADR 011의 SDK admission을 구체화 |

## 결정

```text
SDK <-> GW : TLS/TCP + 서버 인증 + ClusterToken, client 인증서 없음
GW  <-> GW : mTLS/TCP + 기존 peer handshake의 logical GatewayId 검증
GW  <-> RT : mTLS/TCP + 기존 RT transport의 logical identity 검증
Helm       : 외부에서 만든 certificate Secret을 mount하고 경로를 배선
```

0.2 runtime은 framed protocol을 native TLS/TCP 위에서 실행한다. HTTP를 사용하지 않으므로 이를
HTTPS라고 부르지 않는다. SDK는 CA와 server name을 검증한 뒤에만 ClusterToken을 전송한다.
port-forward처럼 dial 주소와 인증 이름이 다른 환경을 위해 server name을 별도 config로 받는다.

내부 transport는 양쪽 certificate chain을 검증한다. mTLS는 RelayGate cluster membership을
확인하고, 현재 peer/RT handshake는 protocol이 주장하는 `GatewayId`와 `ShardId`를 기존 배포 config와
대조한다. certificate 검증 실패나 logical identity 불일치는 연결 실패이며 평문으로 전환하지 않는다.

TLS는 Rust runtime이 수행한다. Helm은 certificate를 생성하지 않고 `existingSecret`의 CA, certificate,
private key를 read-only file로 mount한다. certificate 변경은 새 process rollout로 적용하며 0.2는
hot reload를 제공하지 않는다. 명시적인 개발용 insecure profile은 unit test에만 허용하고 배포 차트의
기본 또는 fallback으로 제공하지 않는다.

## 결과

- SDK와 내부 component는 같은 framed protocol과 lifecycle을 유지하면서 전송 구간을 보호한다.
- 구간 TLS는 SDK 간 E2E 암호화나 Pipe 상대 application 인증이 아니다.
- RelayGate 운영자에게도 payload를 숨겨야 하면 application이 Pipe 위에 별도 보안 채널을 만든다.
- RT shard 간 복제, service mesh와 sidecar를 요구하지 않는다.

## 참고

- [RFC 8446 §1](https://www.rfc-editor.org/rfc/rfc8446.html#section-1)
- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [SPEC 008](../spec/008-runtime-observability-contract.md)
