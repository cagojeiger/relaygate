# RelayGate Go 언어 SDK

공개 모듈: `github.com/cagojeiger/relaygate/sdk/go`

```bash
go get github.com/cagojeiger/relaygate/sdk/go@v0.1.0
```

공개 API는 `Client`, `ManagedClient`, `Listener`, `Offer`, `Pipe`를 노출한다. 생성된 protobuf type은 `internal/gen` 아래 비공개이며 서버, 제어, Peer, Raft package는 이 모듈의 의존성이 아니다.

권장 애플리케이션 시작점은 `ConnectManaged`다. 프로세스 내부 세션 감독 goroutine 하나가 상한이 있는 backoff로 재연결하고 현재 `ManagedListener` 선언만 새로 Bind한다. 세션 경계를 넘어 Open·Pipe·payload 작업을 대기열에 넣거나 재시도하지 않는다. `Close`는 감독자를 취소하고 합류한다. 애플리케이션이 세션 재연결과 Listener 재선언을 직접 소유할 때만 원시 `Connect`를 사용한다.

Bind/Unbind 거부는 형식이 있는 작업 범위 오류이며 인증된 세션을 계속 사용할 수 있다. `Pipe.Send`는 원격 SDK가 exact `PayloadId`를 상한이 있는 수신 대기열에 넣은 뒤에만 nil을 반환한다. `DeliveryError`는 `NotSent`, `Rejected`, 전달 이후 `Unknown`을 구분한다. 거부는 exact Pipe만 종료하고 Client는 종료하지 않는다. 전달 확인은 애플리케이션 처리나 영속 commit을 증명하지 않는다.

관리형 재연결은 연결·재바인딩·준비 과정의 잘못된 인자, 인증, 권한, 사전 조건 실패, 프로토콜 위반을 영구 오류로 분류한다. 일시적인 전송·가용성 오류만 상한이 있는 backoff를 적용한다. 알 수 없는 응답 코드, `UNSPECIFIED`, 다른 요청과의 연관은 프로토콜 종료 오류다. `ErrOpenDuplicateInFlight`는 중복 진행 Open 거부를 별도 결과로 보존한다. `ErrPipeNotOwned`는 소유하지 않은 Pipe 닫기를 나타내며 이전 호환 종료 처리를 위해 `errors.Is`에서 `ErrPipeClosed`와도 일치한다.

```go
client, err := relaygate.ConnectManaged(ctx,
    relaygate.NewConfig(address, clientID, keyID, apiKey))
if err != nil {
    return err
}
defer client.Close()

listener, err := client.Bind(ctx, "/echo", "server")
if err != nil {
    return err
}
for {
    offer, err := listener.Next(ctx)
    if err != nil {
        return err
    }
    // Offer/Pipe 처리는 애플리케이션이 세션 범위에서 소유한다.
    _ = offer
}
```

저장소 workspace 없이 모듈을 독립 검증한다.

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

정규 schema는 `proto/relaygate/relay/v1/relay.proto`다. 저장소 최상위 경로에서 생성된 header에 기록된 고정 plugin 버전으로 비공개 wire package를 재생성한다.

```bash
PATH="$(go env GOPATH)/bin:$PATH" protoc -I . \
  --go_out=sdk/go \
  --go_opt=module=github.com/cagojeiger/relaygate/sdk/go \
  --go_opt=Mproto/relaygate/relay/v1/relay.proto=github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1 \
  --go-grpc_out=sdk/go \
  --go-grpc_opt=module=github.com/cagojeiger/relaygate/sdk/go \
  --go-grpc_opt=Mproto/relaygate/relay/v1/relay.proto=github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1 \
  proto/relaygate/relay/v1/relay.proto
```
