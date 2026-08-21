# RelayGate Echo 예제

이 로컬 예제는 persistent Raft controller 하나, durable store가 없는 Gateway 하나, Echo listener 두 개를 실행한다.

- Go listener: endpoint `/examples/echo`, target `go`
- Rust listener: endpoint `/examples/echo`, target `rust`

각 listener는 한 번에 Pipe 하나를 처리하며, 받은 non-empty payload frame을 그대로 돌려준다. Go와 Rust 예제는 권장 SDK 경로인 `ManagedClient`를 사용하므로 일시적인 Relay session 단절 뒤 현재 Listener만 자동 재연결·재등록한다. 기존 Open, Pipe, payload는 재시도하거나 복원하지 않는다. controller의 named volume은 local single-node Raft state를 보존하고, Gateway는 Raft volume이나 listener를 갖지 않는다. 기준 local-development credential과 명시적인 loopback plaintext를 사용한다. Raft 고가용성, failover와 TLS는 이 예제의 검증 범위가 아니다.

## 실행

이 디렉터리에서 실행한다.

```bash
RELAYGATE_BOOTSTRAP_ONCE=true docker compose up -d --build
docker compose up -d --no-build --no-deps --force-recreate controller
```

첫 명령의 command-scoped bootstrap 입력은 빈 volume 최초 생성에만 사용한다. 두 번째 명령은 controller만 `bootstrap=false`로 다시 만들며, 이후 재시작에는 `docker compose up -d`만 사용한다.

두 listener가 `ECHO_READY`를 출력하면 다른 터미널에서 호출한다.

```bash
docker compose run --rm echo-go send rust "hello from Go"
docker compose run --rm echo-rust send go "hello from Rust"
```

첫 명령은 Go SDK에서 Rust listener를 호출하고, 두 번째 명령은 Rust SDK에서 Go listener를 호출한다. 임시 single-node 환경은 다음 명령으로 종료하고 제거한다.

```bash
docker compose down --remove-orphans
```

Relay port는 host에 publish하지 않는다. Echo container가 Gateway container의 network namespace를 공유하고 loopback-only public Relay listener에 연결한다.

## 네 SDK 조합 검증

테스트는 고유한 Compose project를 소유하고 Bind 이후의 정확한 readiness를 기다린다. 네 caller/listener 언어 조합을 실행한 뒤 자신이 만든 container를 제거한다.

```bash
./test/run.sh
```

이 검증은 예제의 happy path만 확인한다. 장애와 상태 전이 검증은 [core test plan](../../docs/test/001-core-correctness-test-plan.md)이 담당한다.
