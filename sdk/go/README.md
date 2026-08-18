# RelayGate Go SDK

Public module: `github.com/cagojeiger/relaygate/sdk/go`

```bash
go get github.com/cagojeiger/relaygate/sdk/go@v0.1.0
```

The public API exposes `Client`, `ManagedClient`, `Listener`, `Offer`, and `Pipe`. Generated protobuf types are private under
`internal/gen`; server, control, peer, and Raft packages are not dependencies of this module.

`ConnectManaged` is an opt-in in-process session supervisor. It uses one goroutine, reconnects with bounded backoff,
and fresh-Binds only current `ManagedListener` declarations. It never queues or retries Open/Pipe/payload work across a
session boundary; `Close` cancels and joins the supervisor.

Bind and Unbind rejections are typed, operation-local errors and leave the authenticated session usable. A
`PipePayloadRejected` response is terminal for that exact Pipe because payload frames have no acknowledgement ID; it does
not terminate the Client. Managed reconnect treats invalid arguments, authentication, permission, failed preconditions,
and protocol violations as permanent, while transient transport and availability failures enter bounded backoff.

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
    // Offer/Pipe handling remains application-owned and session-bound.
    _ = offer
}
```

Validate the module independently from the repository workspace:

```bash
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

`proto/relaygate/relay/v1/relay.proto` is the canonical schema. From the repository root, regenerate the private wire
package with the pinned plugin versions recorded in the generated headers:

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
