# TEST 002: 단일 Gateway SDK 검증

`sdk_gateway_contract` integration target가 다음 local 경로를 검증합니다.

```text
Relay A.listen(alpha/a, publish token)   Relay B.listen(alpha/b, publish token)
Relay A.dial(alpha/b, dial token)        Relay B.dial(alpha/a, dial token)
                    \                    /
                         Gateway 하나
                    /                    \
                 Pipe A->B            Pipe B->A
```

- 한 Relay가 credential-free session 하나에서 listen/dial하고 Listener가 accept 수행
- 같은 Destination을 여러 Relay가 publish하고 dial마다 하나만 선택
- 같은 Relay의 동일 Destination 중복 listen 거절
- self Binding만 존재하면 실패
- valid publish/dial grant 허용, invalid JWT·action/destination mismatch 거절
- authorization 거절 뒤 RelaySession과 sibling state 유지
- SDK–Gateway 실제 TLS handshake와 `relaygate/3` ALPN
- Gateway restart 뒤 AccessTokenSource 재호출, Listener republish, old Pipe 종료와 fresh Pipe 성공

Compose는 같은 SDK 계약을 `RT 2 / GW 3` topology에서 반복하며 예제와 배포 wiring을 확인합니다.
