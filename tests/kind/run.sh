#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CLUSTER_NAME=${RELAYGATE_KIND_CLUSTER_NAME:-relaygate-v02-${GITHUB_RUN_ID:-$$}}
NODE_IMAGE=${RELAYGATE_KIND_NODE_IMAGE:-kindest/node:v1.32.2}
ENVOY_IMAGE=${RELAYGATE_ENVOY_IMAGE:-envoyproxy/envoy:v1.33.4}
ARTIFACTS=${RELAYGATE_KIND_ARTIFACTS:-$ROOT/target/kind-acceptance}
KEEP_CLUSTER=${RELAYGATE_KIND_KEEP_CLUSTER:-false}
NAMESPACE=relaygate
RELEASE=relaygate
IMAGE_TAG=kind-${GITHUB_SHA:-local}
GATEWAY_IMAGE=relaygate-gateway:$IMAGE_TAG
ROUTE_TABLE_IMAGE=relaygate-route-table:$IMAGE_TAG
GATEWAYS=127.0.0.1:28420,127.0.0.1:28421,127.0.0.1:28422
DESTINATION_A=11111111-1111-4111-8111-111111111111
DESTINATION_B=22222222-2222-4222-8222-222222222222
DESTINATION_C=33333333-3333-4333-8333-333333333333
DESTINATION_SHARED=44444444-4444-4444-8444-444444444444
DESTINATION_CONTINUITY=55555555-5555-4555-8555-555555555555
TEMP_DIR=
CLUSTER_CREATED=false
BACKGROUND_PIDS=()
ORIGINAL_CONTEXT=

require_commands() {
  local command
  for command in bash cargo curl docker helm jq kind kubectl openssl; do
    if ! command -v "$command" >/dev/null 2>&1; then
      echo "required command is missing: $command" >&2
      return 1
    fi
  done
}

record_pass() {
  printf 'PASS %s %s\n' "$1" "$2" | tee -a "$ARTIFACTS/summary.txt"
}

wait_for_log() {
  local file=$1
  local pattern=$2
  local attempts=${3:-120}
  local attempt
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if grep -Fq "$pattern" "$file" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "log did not contain expected text: $pattern" >&2
  return 1
}

wait_for_file() {
  local file=$1
  local attempts=${2:-120}
  local attempt
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if [[ -s "$file" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "file did not become ready: $file" >&2
  return 1
}

wait_for_process_exit() {
  local pid=$1
  local attempts=${2:-120}
  local attempt
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "process did not exit before the deadline: $pid" >&2
  return 1
}

wait_for_replaced_pod() {
  local pod=$1
  local old_uid=$2
  local attempts=${3:-180}
  local attempt uid ready
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    uid=$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    ready=$(kubectl -n "$NAMESPACE" get pod "$pod" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
    if [[ -n "$uid" && "$uid" != "$old_uid" && "$ready" == True ]]; then
      return 0
    fi
    sleep 1
  done
  echo "replacement pod did not become Ready: $pod" >&2
  return 1
}

stop_process() {
  local pid=$1
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  wait "$pid" >/dev/null 2>&1 || true
}

capture_evidence() {
  if [[ "$CLUSTER_CREATED" != true ]]; then
    return
  fi
  mkdir -p "$ARTIFACTS/logs" "$ARTIFACTS/metrics"
  kubectl --context "kind-$CLUSTER_NAME" get all -n "$NAMESPACE" -o wide \
    >"$ARTIFACTS/resources.txt" 2>&1 || true
  kubectl --context "kind-$CLUSTER_NAME" get events -n "$NAMESPACE" --sort-by=.lastTimestamp \
    >"$ARTIFACTS/events.txt" 2>&1 || true
  local pod
  while IFS= read -r pod; do
    kubectl --context "kind-$CLUSTER_NAME" logs -n "$NAMESPACE" "$pod" --all-containers=true \
      >"$ARTIFACTS/logs/$pod.log" 2>&1 || true
  done < <(
    kubectl --context "kind-$CLUSTER_NAME" get pods -n "$NAMESPACE" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
  )
  local index
  for index in 0 1 2; do
    curl -fsS "http://127.0.0.1:$((28430 + index))/metrics" \
      >"$ARTIFACTS/metrics/gateway-$index.prom" 2>/dev/null || true
  done
  for index in 0 1; do
    curl -fsS "http://127.0.0.1:$((28440 + index))/metrics" \
      >"$ARTIFACTS/metrics/route-table-$index.prom" 2>/dev/null || true
  done
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  capture_evidence
  local pid
  for pid in "${BACKGROUND_PIDS[@]}"; do
    stop_process "$pid"
  done
  if [[ "$CLUSTER_CREATED" == true && "$KEEP_CLUSTER" != true ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null
  fi
  if [[ -n "$ORIGINAL_CONTEXT" ]]; then
    kubectl config use-context "$ORIGINAL_CONTEXT" >/dev/null 2>&1 || true
  fi
  if [[ -n "$TEMP_DIR" && -d "$TEMP_DIR" ]]; then
    rm -rf "$TEMP_DIR"
  fi
  exit "$status"
}

generate_certificates() {
  local directory=$1
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj '/CN=relaygate-kind-ca' \
    -keyout "$directory/ca.key" -out "$directory/ca.crt" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -subj '/CN=relaygate-gateway.internal' \
    -keyout "$directory/edge.key" -out "$directory/edge.csr" >/dev/null 2>&1
  printf '%s\n' \
    'subjectAltName=DNS:relaygate-gateway.internal' \
    'extendedKeyUsage=serverAuth' >"$directory/edge.ext"
  openssl x509 -req -days 2 -sha256 \
    -in "$directory/edge.csr" -CA "$directory/ca.crt" -CAkey "$directory/ca.key" \
    -CAcreateserial -extfile "$directory/edge.ext" -out "$directory/edge.crt" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -subj '/CN=relaygate-gateway.internal' \
    -keyout "$directory/internal-gateway.key" \
    -out "$directory/internal-gateway.csr" >/dev/null 2>&1
  printf '%s\n' \
    'subjectAltName=DNS:relaygate-gateway.internal' \
    'extendedKeyUsage=serverAuth,clientAuth' >"$directory/internal-gateway.ext"
  openssl x509 -req -days 2 -sha256 \
    -in "$directory/internal-gateway.csr" -CA "$directory/ca.crt" \
    -CAkey "$directory/ca.key" -CAserial "$directory/ca.srl" \
    -extfile "$directory/internal-gateway.ext" \
    -out "$directory/internal-gateway.crt" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -subj '/CN=relaygate-route-table.internal' \
    -keyout "$directory/internal-rt.key" -out "$directory/internal-rt.csr" >/dev/null 2>&1
  printf '%s\n' \
    'subjectAltName=DNS:relaygate-route-table.internal' \
    'extendedKeyUsage=serverAuth,clientAuth' >"$directory/internal-rt.ext"
  openssl x509 -req -days 2 -sha256 \
    -in "$directory/internal-rt.csr" -CA "$directory/ca.crt" \
    -CAkey "$directory/ca.key" -CAserial "$directory/ca.srl" \
    -extfile "$directory/internal-rt.ext" -out "$directory/internal-rt.crt" >/dev/null 2>&1

  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj '/CN=relaygate-wrong-ca' \
    -keyout "$directory/wrong-ca.key" -out "$directory/wrong-ca.crt" >/dev/null 2>&1
}

write_kind_config() {
  local path=$1
  {
    printf '%s\n' 'kind: Cluster' 'apiVersion: kind.x-k8s.io/v1alpha4' 'nodes:'
    printf '%s\n' '- role: control-plane' '  extraPortMappings:'
    local mapping
    for mapping in \
      30020:28420 30021:28421 30022:28422 30023:28423 \
      30030:28430 30031:28431 30032:28432 30040:28440 30041:28441; do
      printf '%s\n' \
        "  - containerPort: ${mapping%%:*}" \
        "    hostPort: ${mapping##*:}" \
        '    listenAddress: "127.0.0.1"' \
        '    protocol: TCP'
    done
  } >"$path"
}

create_secrets() {
  local certificate_dir=$1
  kubectl create namespace "$NAMESPACE"
  kubectl -n "$NAMESPACE" create secret generic relaygate-credentials \
    --from-literal=internal-gateway-keys="relaygate-gateway-0=$GATEWAY_KEY_0,relaygate-gateway-1=$GATEWAY_KEY_1,relaygate-gateway-2=$GATEWAY_KEY_2" \
    --from-literal=cluster-token="$CLUSTER_TOKEN"
  kubectl -n "$NAMESPACE" create secret generic relaygate-edge-tls \
    --from-file=ca.crt="$certificate_dir/ca.crt" \
    --from-file=tls.crt="$certificate_dir/edge.crt" \
    --from-file=tls.key="$certificate_dir/edge.key"
  kubectl -n "$NAMESPACE" create secret generic relaygate-internal-tls \
    --from-file=ca.crt="$certificate_dir/ca.crt" \
    --from-file=gateway.crt="$certificate_dir/internal-gateway.crt" \
    --from-file=gateway.key="$certificate_dir/internal-gateway.key" \
    --from-file=route-table.crt="$certificate_dir/internal-rt.crt" \
    --from-file=route-table.key="$certificate_dir/internal-rt.key"
}

apply_host_access() {
  kubectl -n "$NAMESPACE" apply -f - <<'YAML'
apiVersion: v1
kind: Service
metadata:
  name: relaygate-gateway-0-host
spec:
  type: NodePort
  selector:
    statefulset.kubernetes.io/pod-name: relaygate-gateway-0
  ports:
    - name: sdk
      port: 27420
      targetPort: sdk
      nodePort: 30020
    - name: metrics
      port: 27422
      targetPort: metrics
      nodePort: 30030
---
apiVersion: v1
kind: Service
metadata:
  name: relaygate-gateway-1-host
spec:
  type: NodePort
  selector:
    statefulset.kubernetes.io/pod-name: relaygate-gateway-1
  ports:
    - name: sdk
      port: 27420
      targetPort: sdk
      nodePort: 30021
    - name: metrics
      port: 27422
      targetPort: metrics
      nodePort: 30031
---
apiVersion: v1
kind: Service
metadata:
  name: relaygate-gateway-2-host
spec:
  type: NodePort
  selector:
    statefulset.kubernetes.io/pod-name: relaygate-gateway-2
  ports:
    - name: sdk
      port: 27420
      targetPort: sdk
      nodePort: 30022
    - name: metrics
      port: 27422
      targetPort: metrics
      nodePort: 30032
---
apiVersion: v1
kind: Service
metadata:
  name: relaygate-rt-0-host
spec:
  type: NodePort
  selector:
    statefulset.kubernetes.io/pod-name: relaygate-rt-0
  ports:
    - name: metrics
      port: 27431
      targetPort: metrics
      nodePort: 30040
---
apiVersion: v1
kind: Service
metadata:
  name: relaygate-rt-1-host
spec:
  type: NodePort
  selector:
    statefulset.kubernetes.io/pod-name: relaygate-rt-1
  ports:
    - name: metrics
      port: 27431
      targetPort: metrics
      nodePort: 30041
YAML
}

apply_envoy_passthrough() {
  kubectl -n "$NAMESPACE" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: relaygate-envoy
data:
  envoy.yaml: |
    static_resources:
      listeners:
        - name: relaygate_sdk
          address:
            socket_address: { address: 0.0.0.0, port_value: 10000 }
          filter_chains:
            - filters:
                - name: envoy.filters.network.tcp_proxy
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
                    stat_prefix: relaygate_sdk
                    cluster: relaygate_gateway
      clusters:
        - name: relaygate_gateway
          type: STRICT_DNS
          connect_timeout: 2s
          load_assignment:
            cluster_name: relaygate_gateway
            endpoints:
              - lb_endpoints:
                  - endpoint:
                      address:
                        socket_address:
                          address: relaygate.$NAMESPACE.svc.cluster.local
                          port_value: 27420
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: relaygate-envoy
spec:
  replicas: 1
  selector:
    matchLabels: { app: relaygate-envoy }
  template:
    metadata:
      labels: { app: relaygate-envoy }
    spec:
      containers:
        - name: envoy
          image: $ENVOY_IMAGE
          args: ["-c", "/etc/envoy/envoy.yaml"]
          ports:
            - { name: relay, containerPort: 10000 }
          readinessProbe:
            tcpSocket: { port: relay }
            periodSeconds: 1
          volumeMounts:
            - { name: config, mountPath: /etc/envoy, readOnly: true }
      volumes:
        - name: config
          configMap: { name: relaygate-envoy }
---
apiVersion: v1
kind: Service
metadata:
  name: relaygate-envoy-host
spec:
  type: NodePort
  selector: { app: relaygate-envoy }
  ports:
    - name: relay
      port: 10000
      targetPort: relay
      nodePort: 30023
YAML
  kubectl -n "$NAMESPACE" rollout status deployment/relaygate-envoy --timeout=120s
}

start_listener() {
  local index=$1
  local address=$2
  local destinations=$3
  RELAYGATE_ADDR="$address" \
    RELAYGATE_DESTINATIONS="$destinations" \
    "$LISTENER" >"$ARTIFACTS/listener-$index.log" 2>&1 &
  local pid=$!
  BACKGROUND_PIDS+=("$pid")
  echo "$pid"
}

run_probe() {
  local name=$1
  shift
  "$PROBE" "$@" 2>&1 | tee "$ARTIFACTS/$name.log"
}

wait_for_destination() {
  local destination=$1
  "$PROBE" wait-client "$destination" \
    >"$ARTIFACTS/wait-$destination.log" 2>&1
}

assert_check_fails() {
  local name=$1
  shift
  if "$@" >"$ARTIFACTS/$name.log" 2>&1; then
    echo "$name unexpectedly succeeded" >&2
    return 1
  fi
}

assert_wrong_alpn_rejected() {
  local before after attempt openssl_pid
  before=$(kubectl -n "$NAMESPACE" logs relaygate-gateway-0 | grep -Fc 'gateway.session.tls_rejected' || true)
  openssl s_client \
    -connect 127.0.0.1:28420 \
    -servername relaygate-gateway.internal \
    -CAfile "$CERTIFICATES/ca.crt" \
    -verify_return_error \
    -alpn relaygate/wrong </dev/null \
    >"$ARTIFACTS/wrong-alpn.log" 2>&1 &
  openssl_pid=$!
  for ((attempt = 1; attempt <= 5; attempt++)); do
    if ! kill -0 "$openssl_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  stop_process "$openssl_pid"
  for ((attempt = 1; attempt <= 20; attempt++)); do
    after=$(kubectl -n "$NAMESPACE" logs relaygate-gateway-0 | grep -Fc 'gateway.session.tls_rejected' || true)
    if ((after > before)); then
      return 0
    fi
    sleep 1
  done
  echo "Gateway did not report rejection of the wrong ALPN" >&2
  return 1
}

wait_for_cleanup_baseline() {
  local attempt index metric body nonzero
  for ((attempt = 1; attempt <= 60; attempt++)); do
    nonzero=
    for index in 0 1 2; do
      body=$(curl -fsS "http://127.0.0.1:$((28430 + index))/metrics" || true)
      for metric in \
        relaygate_gateway_sessions \
        relaygate_gateway_bindings \
        relaygate_gateway_pending_offers \
        relaygate_gateway_live_pipes \
        relaygate_gateway_remote_dial_attempts \
        relaygate_gateway_peer_streams; do
        if awk -v metric="$metric" '$1 ~ ("^" metric "({|$)") && ($NF + 0) != 0 { found=1 } END { exit !found }' \
          <<<"$body"; then
          nonzero="$nonzero gateway-$index:$metric"
        fi
      done
    done
    for index in 0 1; do
      body=$(curl -fsS "http://127.0.0.1:$((28440 + index))/metrics" || true)
      for metric in \
        relaygate_route_table_registrations \
        relaygate_route_table_mappings \
        relaygate_route_table_routes \
        relaygate_route_table_expiry_records; do
        if awk -v metric="$metric" '$1 ~ ("^" metric "({|$)") && ($NF + 0) != 0 { found=1 } END { exit !found }' \
          <<<"$body"; then
          nonzero="$nonzero route-table-$index:$metric"
        fi
      done
    done
    if [[ -z "$nonzero" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "current-state gauges did not return to zero:$nonzero" >&2
  return 1
}

assert_no_secret_or_payload_leak() {
  capture_evidence
  local marker
  for marker in \
    "$CLUSTER_TOKEN" "$GATEWAY_KEY_0" "$GATEWAY_KEY_1" "$GATEWAY_KEY_2" \
    'BEGIN PRIVATE KEY' 'hello relaygate' 'relaygate matrix entry='; do
    if grep -R -F -n -- "$marker" "$ARTIFACTS/logs" "$ARTIFACTS/metrics" >/dev/null; then
      echo "server evidence contains a protected marker" >&2
      return 1
    fi
  done
}

main() {
  require_commands
  cd "$ROOT"
  case "$ARTIFACTS" in
    "$ROOT"/target/*) ;;
    *)
      echo "RELAYGATE_KIND_ARTIFACTS must be inside $ROOT/target" >&2
      return 1
      ;;
  esac
  rm -rf "$ARTIFACTS"
  mkdir -p "$ARTIFACTS"
  : >"$ARTIFACTS/summary.txt"
  TEMP_DIR=$(mktemp -d)
  CERTIFICATES=$TEMP_DIR/certificates
  mkdir -p "$CERTIFICATES"
  CLUSTER_TOKEN=kind-cluster-$(openssl rand -hex 16)
  GATEWAY_KEY_0=kind-gateway-0-$(openssl rand -hex 16)
  GATEWAY_KEY_1=kind-gateway-1-$(openssl rand -hex 16)
  GATEWAY_KEY_2=kind-gateway-2-$(openssl rand -hex 16)
  export CLUSTER_TOKEN GATEWAY_KEY_0 GATEWAY_KEY_1 GATEWAY_KEY_2 CERTIFICATES

  generate_certificates "$CERTIFICATES"
  write_kind_config "$TEMP_DIR/kind.yaml"
  ORIGINAL_CONTEXT=$(kubectl config current-context 2>/dev/null || true)

  cargo build --workspace --locked
  LISTENER=$ROOT/target/debug/relaygate-echo-listener
  PROBE=$ROOT/target/debug/relaygate-echo-probe
  SERVER=$ROOT/target/debug/relaygate-server
  export LISTENER PROBE SERVER

  docker build -f deploy/docker/Dockerfile --target gateway -t "$GATEWAY_IMAGE" .
  docker build -f deploy/docker/Dockerfile --target route-table -t "$ROUTE_TABLE_IMAGE" .
  CLUSTER_CREATED=true
  kind create cluster --name "$CLUSTER_NAME" --image "$NODE_IMAGE" \
    --config "$TEMP_DIR/kind.yaml" --wait 120s
  kubectl config use-context "kind-$CLUSTER_NAME" >/dev/null
  kind load docker-image --name "$CLUSTER_NAME" "$GATEWAY_IMAGE" "$ROUTE_TABLE_IMAGE"

  create_secrets "$CERTIFICATES"
  helm upgrade --install "$RELEASE" deploy/helm/relaygate \
    --namespace "$NAMESPACE" \
    --set-string gateway.image.repository=relaygate-gateway \
    --set-string gateway.image.tag="$IMAGE_TAG" \
    --set gateway.image.pullPolicy=IfNotPresent \
    --set-string routeTable.image.repository=relaygate-route-table \
    --set-string routeTable.image.tag="$IMAGE_TAG" \
    --set routeTable.image.pullPolicy=IfNotPresent \
    --set routeTable.leaseTtlMs=5000 \
    --set gateway.log=debug \
    --set routeTable.log=debug \
    --set metrics.intervalMs=1000 \
    --set gateway.drainTimeoutMs=10000 \
    --set gateway.terminationGracePeriodSeconds=20 \
    --wait --timeout 180s
  apply_host_access
  apply_envoy_passthrough

  export RELAYGATE_GATEWAYS="$GATEWAYS"
  export RELAYGATE_CLUSTER_TOKEN="$CLUSTER_TOKEN"
  export RELAYGATE_SDK_TLS_CA_PATH="$CERTIFICATES/ca.crt"
  export RELAYGATE_SDK_TLS_SERVER_NAME=relaygate-gateway.internal
  export RELAYGATE_LOG=warn

  "$SERVER" check 127.0.0.1:28420 >"$ARTIFACTS/tls-valid.log" 2>&1
  assert_check_fails wrong-token env RELAYGATE_CLUSTER_TOKEN=wrong-token \
    "$SERVER" check 127.0.0.1:28420
  assert_check_fails wrong-name env RELAYGATE_SDK_TLS_SERVER_NAME=wrong.internal \
    "$SERVER" check 127.0.0.1:28420
  assert_check_fails wrong-ca env RELAYGATE_SDK_TLS_CA_PATH="$CERTIFICATES/wrong-ca.crt" \
    "$SERVER" check 127.0.0.1:28420
  assert_wrong_alpn_rejected
  record_pass KIND-01 'TLS CA/name/token/ALPN admission'

  run_probe chat chat
  record_pass KIND-02 'symmetric three-participant chat'
  record_pass KIND-04 'N:M single selection and survivor failover'

  start_listener 0 127.0.0.1:28420 "$DESTINATION_A" >/dev/null
  start_listener 1 127.0.0.1:28421 "$DESTINATION_B,$DESTINATION_SHARED" >/dev/null
  start_listener 2 127.0.0.1:28422 "$DESTINATION_C,$DESTINATION_SHARED,$DESTINATION_CONTINUITY" >/dev/null
  wait_for_destination "$DESTINATION_A"
  wait_for_destination "$DESTINATION_B"
  wait_for_destination "$DESTINATION_C"
  wait_for_destination "$DESTINATION_SHARED"
  wait_for_destination "$DESTINATION_CONTINUITY"

  run_probe matrix matrix
  record_pass KIND-03 'all local and directed one-hop paths'

  RELAYGATE_ADDR=127.0.0.1:28423 RELAYGATE_DESTINATION_ID="$DESTINATION_A" \
    run_probe envoy single
  record_pass KIND-09 'Envoy byte passthrough with Gateway TLS termination'

  RELAYGATE_CONTINUITY_ADDR=127.0.0.1:28420 \
    RELAYGATE_CONTINUITY_DESTINATION_ID="$DESTINATION_C" \
    RELAYGATE_CONTINUITY_STATE="$TEMP_DIR/rt-continuity.state" \
    "$PROBE" continuity >"$ARTIFACTS/rt-continuity.log" 2>&1 &
  RT_CONTINUITY_PID=$!
  BACKGROUND_PIDS+=("$RT_CONTINUITY_PID")
  wait_for_file "$TEMP_DIR/rt-continuity.state"
  kubectl -n "$NAMESPACE" rollout restart statefulset/relaygate-rt
  kubectl -n "$NAMESPACE" rollout status statefulset/relaygate-rt --timeout=180s
  RELAYGATE_CONTINUITY_STATE="$TEMP_DIR/rt-continuity.state" \
    "$PROBE" continuity-check | tee "$ARTIFACTS/rt-continuity-check.log"
  stop_process "$RT_CONTINUITY_PID"
  wait_for_destination "$DESTINATION_A"
  wait_for_destination "$DESTINATION_B"
  wait_for_destination "$DESTINATION_C"
  record_pass KIND-10 'RouteTable rolling restart continuity and recovery'

  kubectl -n "$NAMESPACE" scale statefulset/relaygate-rt --replicas=1
  kubectl -n "$NAMESPACE" wait --for=delete pod/relaygate-rt-1 --timeout=120s
  run_probe rt-isolation expect-shard-isolation "$DESTINATION_B" 1 "$DESTINATION_A"
  kubectl -n "$NAMESPACE" scale statefulset/relaygate-rt --replicas=2
  kubectl -n "$NAMESPACE" rollout status statefulset/relaygate-rt --timeout=180s
  wait_for_destination "$DESTINATION_B"
  record_pass KIND-06 'RouteTable shard loss isolation and current-state recovery'

  RELAYGATE_CONTINUITY_ADDR=127.0.0.1:28420 \
    RELAYGATE_CONTINUITY_DESTINATION_ID="$DESTINATION_B" \
    RELAYGATE_CONTINUITY_STATE="$TEMP_DIR/gateway-continuity.state" \
    "$PROBE" continuity >"$ARTIFACTS/gateway-old-pipe.log" 2>&1 &
  GATEWAY_PIPE_PID=$!
  BACKGROUND_PIDS+=("$GATEWAY_PIPE_PID")
  wait_for_file "$TEMP_DIR/gateway-continuity.state"
  GATEWAY_1_UID=$(kubectl -n "$NAMESPACE" get pod relaygate-gateway-1 -o jsonpath='{.metadata.uid}')
  kubectl -n "$NAMESPACE" delete pod relaygate-gateway-1 --grace-period=0 --force --wait=false
  wait_for_process_exit "$GATEWAY_PIPE_PID" 60
  if wait "$GATEWAY_PIPE_PID"; then
    echo 'old Pipe survived an abrupt Owner Gateway replacement' >&2
    return 1
  fi
  wait_for_replaced_pod relaygate-gateway-1 "$GATEWAY_1_UID" 180
  wait_for_destination "$DESTINATION_B"
  run_probe gateway-recovery matrix
  record_pass KIND-05 'Gateway loss closes old Pipe and fresh dial recovers'

  kubectl -n "$NAMESPACE" rollout restart statefulset/relaygate-gateway
  kubectl -n "$NAMESPACE" rollout status statefulset/relaygate-gateway --timeout=240s
  wait_for_destination "$DESTINATION_A"
  wait_for_destination "$DESTINATION_B"
  wait_for_destination "$DESTINATION_C"
  run_probe gateway-rolling matrix
  record_pass KIND-11 'Gateway rolling restart reconnect and republish'

  RELAYGATE_ADDR=127.0.0.1:28420 \
    RELAYGATE_DESTINATION_ID="$DESTINATION_A" \
    RELAYGATE_STORM_SESSIONS=100 \
    RELAYGATE_STORM_PAUSE_SECS=45 \
    "$PROBE" reconnect-storm >"$ARTIFACTS/reconnect-storm.log" 2>&1 &
  STORM_PID=$!
  BACKGROUND_PIDS+=("$STORM_PID")
  wait_for_log "$ARTIFACTS/reconnect-storm.log" 'relaygate reconnect storm ready' 180
  GATEWAY_0_UID=$(kubectl -n "$NAMESPACE" get pod relaygate-gateway-0 -o jsonpath='{.metadata.uid}')
  kubectl -n "$NAMESPACE" delete pod relaygate-gateway-0 --grace-period=0 --force --wait=false
  wait_for_replaced_pod relaygate-gateway-0 "$GATEWAY_0_UID" 180
  wait_for_process_exit "$STORM_PID" 240
  wait "$STORM_PID"
  record_pass KIND-12 '100-session reconnect storm recovery'

  RELAYGATE_SOAK_DURATION_SECS=${RELAYGATE_SOAK_DURATION_SECS:-60} \
    RELAYGATE_SOAK_CONCURRENCY=${RELAYGATE_SOAK_CONCURRENCY:-64} \
    run_probe soak soak
  record_pass KIND-13 'bounded Pipe soak'

  local pid
  for pid in "${BACKGROUND_PIDS[@]}"; do
    stop_process "$pid"
  done
  BACKGROUND_PIDS=()
  wait_for_cleanup_baseline
  record_pass KIND-07 'current-state cleanup baseline'

  assert_no_secret_or_payload_leak
  record_pass KIND-08 'secret and payload non-disclosure'
  capture_evidence
  printf 'RelayGate v0.2 Kind acceptance completed for %s\n' "$CLUSTER_NAME" \
    | tee -a "$ARTIFACTS/summary.txt"
}

trap cleanup EXIT INT TERM
main "$@"
