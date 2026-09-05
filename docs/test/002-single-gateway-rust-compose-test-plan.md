# TEST 002: 단일 Gateway SDK 검증

`v02_public_sdk` integration target가 다음 최소 local 경로를 검증합니다.

```text
Relay A.listen(dst-a)       Relay B.listen(dst-b)
Relay A.dial(dst-b)         Relay B.dial(dst-a)
           \                  /
            Gateway 하나
           /                  \
        Pipe A->B           Pipe B->A
```

- 한 Relay가 같은 세션에서 listen/dial하고 반환된 Listener가 accept를 수행
- 같은 Destination의 여러 Relay 중 하나만 선택
- self Binding만 있으면 실패
- current/next ClusterToken과 invalid token 거절
- SDK–Gateway 실제 TLS handshake
- Gateway restart 뒤 Listener republish, old Pipe 종료와 fresh Pipe 성공

Compose는 같은 SDK 계약을 `RT 2 / GW 3` topology에서 반복하며 예제와 배포 wiring까지 확인합니다.
