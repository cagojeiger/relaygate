# RelayGate

RelayGate는 NAT 뒤 애플리케이션이 외부로 연 하나의 장기 세션을 통해 논리 주소를 수신하고,
다른 논리 주소로 양방향 byte stream을 여는 Rust relay입니다.

```text
Relay A ── TLS ──► GW A ══ mTLS, 최대 one hop ══ GW B ◄── TLS ── Relay B
                       └──────── mTLS ────────► RT shards

Relay A.listen(DestinationId)        DestinationId -> 0..N live Binding
Relay B.dial(DestinationId)          dial 1회 -> Binding 1개 -> Pipe 1개
Listener.accept()                    Pipe = opaque bidirectional byte stream
```

고정 Connector/Listener 세션 역할은 없습니다. 하나의 `Relay`가 `listen`과 `dial`을 수행하고,
반환된 `Listener`가 `accept`를 수행합니다. `DestinationId`는 애플리케이션이 생성하고 보관하는
UUIDv4이며, 같은 주소를 여러 Relay가 listen할 수 있습니다.

## 책임 경계

RelayGate가 제공하는 것:

- SDK–Gateway TLS와 단일 trust-domain `ClusterToken` admission
- live `DestinationId -> BindingSet` 조회
- local 또는 최대 one-hop Pipe 연결
- bounded queue, timeout, heartbeat, cleanup과 재연결
- GW–GW 및 GW–RT mTLS

RelayGate가 제공하지 않는 것:

- 사용자·장비 identity와 Destination별 ACL
- payload 해석, 업무 acknowledgement와 payload retry
- 기존 Pipe의 migration 또는 resume
- Destination 발급·영속 저장
- RT persistence, replication과 online resharding

Pipe 상대 인증이나 RelayGate 운영자에게도 숨겨야 하는 payload 보호는 Pipe 위의 application
protocol이 담당합니다.

## Rust SDK

```rust,no_run
use relaygate_sdk::{ClientTlsConfig, Config, DestinationId, GatewayTransportConfig, Relay};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let ca = std::fs::read("ca.crt")?;
let tls = ClientTlsConfig::server_authenticated("relaygate-gateway.internal", &ca)?;
let transport = GatewayTransportConfig::tls_tcp("127.0.0.1:27420", tls);
let relay = Relay::connect(Config::new(
    std::env::var("RELAYGATE_CLUSTER_TOKEN")?,
    transport,
)).await?;

let destination = DestinationId::new();
let listener = relay.listen(destination).await?;

// 다른 Relay에서: let mut pipe = relay.dial(destination).await?;
let mut incoming = listener.accept().await?;
# let _ = &mut incoming;
# Ok(())
# }
```

세션이 끊기면 SDK는 jitter가 포함된 bounded backoff로 재연결하고, 이미 반환된 `Listener`만 새
Session/Binding으로 다시 등록합니다. 기존 Pipe, 완료가 불확실한 dial과 payload는 자동 replay하지
않습니다.

## 로컬 검증

Docker Compose는 RT 2개, Gateway 3개, TLS 초기화 컨테이너, SDK 예제와 topology probe를 띄웁니다.
개발용 인증서는 named volume에 일회성으로 생성되며 저장소에는 개인키를 남기지 않습니다.

```bash
docker compose up --build --abort-on-container-exit --exit-code-from topology-probe
docker compose down --volumes --remove-orphans
```

관측 스택까지 확인하려면:

```bash
docker compose --profile observability up --build \
  --abort-on-container-exit --exit-code-from observability-probe \
  observability-probe
docker compose --profile observability down --volumes --remove-orphans
```

격리된 Kubernetes에서 RT2/GW3, Envoy passthrough, rolling restart, reconnect storm과 bounded soak를
검증하려면 `kind`, `kubectl`, `helm`, Docker가 준비된 환경에서 실행합니다. 임시 cluster와 인증서는
종료 시 제거되고 증거는 `target/kind-acceptance`에 남습니다.

```bash
tests/kind/run.sh
```

## Helm

차트는 RT와 Gateway만 배포합니다. SDK workload, credential과 certificate를 생성하지 않습니다.
배포 전에 release namespace에 다음 Secret을 준비해야 합니다.

- credential Secret: `internal-gateway-keys`, `cluster-token`, 선택적 `next-cluster-token`
- edge TLS Secret: `ca.crt`, `tls.crt`, `tls.key`
- internal mTLS Secret: `ca.crt`, `gateway.crt`, `gateway.key`, `route-table.crt`, `route-table.key`

```bash
helm lint deploy/helm/relaygate
helm template relaygate deploy/helm/relaygate --kube-version 1.32.0
```

기본 topology는 `RouteTable shard 2 / Gateway 3`입니다. RT 수는 replica가 아니라 hash partition
수이므로 StatefulSet만 scale하지 말고 동일한 ShardDirectory generation과 함께 배포해야 합니다.

## 개발 검증

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

설계 결정은 [ADR](docs/adr/), 동작 계약은 [SPEC](docs/spec/), 검증 대응은
[TEST](docs/test/)에 있습니다.

## Workspace

```text
crates/
├── relaygate-protocol/              # SDK–GW wire
├── relaygate-transport/             # TLS/mTLS transport adapter
├── relaygate-sdk/                   # public Relay, Listener, Pipe API
├── relaygate-gateway/               # binding, dial, relay, cleanup
├── relaygate-route-table/           # memory-only current-state shard
├── relaygate-route-table-transport/ # GW–RT bounded transport/auth
└── relaygate-server/                # config, process wiring, metrics, shutdown
examples/
├── echo-listener/
└── echo-probe/
deploy/
├── docker/
└── helm/relaygate/
tests/
└── kind/run.sh                       # isolated RT2/GW3 acceptance
```
