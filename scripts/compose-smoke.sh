#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file="$repo_root/docker-compose.yaml"
run_token=$$
generated_project=false
if [[ -n ${RELAYGATE_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME:-}} ]]; then
  project_name=${RELAYGATE_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME}}
else
  project_name="relaygate-smoke-$run_token"
  generated_project=true
fi
cohort_epoch="smoke-$run_token"
test_image="relaygate-compose-test:smoke-$run_token"
runtime_image="relaygate-smoke-runtime:${project_name}-${run_token}"
proxy_name="relaygate-compose-proxy-$run_token"
failover_name="relaygate-compose-failover-$run_token"
owns_project=false
owns_test_image=false
owns_runtime_image=false
gateways_paused=false

export RELAYGATE_COHORT_EPOCH=$cohort_epoch
export RELAYGATE_IMAGE=$runtime_image

cd "$repo_root"
compose=(docker compose --file "$compose_file" --project-name "$project_name")

fail() {
  printf 'compose smoke: %s\n' "$*" >&2
  return 1
}

remove_container() {
  local name=$1
  if docker container inspect "$name" >/dev/null 2>&1; then
    docker rm --force "$name" >/dev/null 2>&1 || true
  fi
}

remove_generated_project_images() {
  [[ "$generated_project" == true ]] || return 0
  while IFS= read -r image_id; do
    [[ -z "$image_id" ]] || docker image rm "$image_id" >/dev/null 2>&1 || true
  done < <(docker image ls --quiet --filter "label=com.docker.compose.project=$project_name" | sort -u)
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if (( status != 0 )) && [[ "$owns_project" == true ]]; then
    printf 'compose smoke: cluster logs after failure\n' >&2
    "${compose[@]}" logs >&2 2>&1 || true
  fi
  remove_container "$proxy_name"
  remove_container "$failover_name"
  if [[ "$gateways_paused" == true ]]; then
    docker unpause "$gateway_one" "$gateway_two" >/dev/null 2>&1 || true
  fi
  if [[ "$owns_project" == true ]]; then
    # Do not use `down -v`: it is too easy to turn an ordinary local cleanup
    # into data loss. The generated project name is unique to this invocation,
    # so only its labelled volumes are explicitly removed afterwards.
    "${compose[@]}" down --remove-orphans >/dev/null 2>&1 || true
    if [[ "$generated_project" == true ]]; then
      while IFS= read -r volume; do
        [[ -z "$volume" ]] || docker volume rm "$volume" >/dev/null 2>&1 || true
      done < <(docker volume ls --quiet --filter "label=com.docker.compose.project=$project_name")
    fi
  fi
  if [[ "$owns_test_image" == true ]]; then
    docker image rm "$test_image" >/dev/null 2>&1 || true
  fi
  if [[ "$owns_runtime_image" == true ]]; then
    docker image rm "$runtime_image" >/dev/null 2>&1 || true
    remove_generated_project_images
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in curl docker python3; do
  command -v "$command" >/dev/null 2>&1 || fail "required command not found: $command"
done
if [[ ! "$project_name" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  fail "project name must match ^[a-z0-9][a-z0-9_-]*$"
fi

existing=$(docker container ls --all --quiet --filter "label=com.docker.compose.project=$project_name")
existing+=$(docker volume ls --quiet --filter "label=com.docker.compose.project=$project_name")
existing+=$(docker network ls --quiet --filter "label=com.docker.compose.project=$project_name")
[[ -z "$existing" ]] || fail "refusing to replace existing Compose project $project_name"
docker image inspect "$test_image" >/dev/null 2>&1 && fail "refusing to replace existing test image $test_image"
docker image inspect "$runtime_image" >/dev/null 2>&1 && fail "refusing to replace existing runtime image $runtime_image"
owns_runtime_image=true

controller_admin_port() {
  case "$1" in
    controller-1) printf '27591\n' ;;
    controller-2) printf '27592\n' ;;
    controller-3) printf '27593\n' ;;
    *) fail "unknown controller service: $1" ;;
  esac
}

gateway_admin_port() {
  case "$1" in
    gateway-1) printf '27594\n' ;;
    gateway-2) printf '27595\n' ;;
    *) fail "unknown gateway service: $1" ;;
  esac
}

controller_status() {
  local port
  port=$(controller_admin_port "$1")
  curl --fail --silent --show-error "http://127.0.0.1:${port}/status" 2>/dev/null || true
}

gateway_status() {
  local port
  port=$(gateway_admin_port "$1")
  curl --fail --silent --show-error "http://127.0.0.1:${port}/status" 2>/dev/null || true
}

is_leader_with_presence() {
  local payload=$1 expected_committed_gateways=$2 expected_committed_routes=$3 expected_revalidated_gateways=$4 expected_eligible_routes=$5
  python3 -c '
import json, sys
s = json.loads(sys.argv[1])
p = s["presence"]
assert s["runtime_role"] == "controller"
assert s["cluster_epoch"] == sys.argv[2]
assert s["raft"]["role"] == "Leader" and s["raft"]["ready"] is True
assert p["state"] == "Current"
assert p["committed_gateways"] >= int(sys.argv[3])
assert p["committed_routes"] >= int(sys.argv[4])
assert p["revalidated_gateways"] >= int(sys.argv[5])
assert p["eligible_routes"] >= int(sys.argv[6])
' "$payload" "$cohort_epoch" "$expected_committed_gateways" "$expected_committed_routes" "$expected_revalidated_gateways" "$expected_eligible_routes" 2>/dev/null
}

is_gateway_revalidated() {
  local payload=$1
  python3 -c '
import json, sys
s = json.loads(sys.argv[1])
assert s["runtime_role"] == "gateway"
assert s["cluster_epoch"] == sys.argv[2]
assert "raft" not in s and "presence" not in s
assert s["gateway_control"]["state"] == "Revalidated"
' "$payload" "$cohort_epoch" 2>/dev/null
}

wait_for_leader() {
  local committed_gateways=$1 committed_routes=$2 revalidated_gateways=$3 eligible_routes=$4 excluded=${5:-}
  local service payload
  for ((attempt = 0; attempt < 240; attempt++)); do
    for service in controller-1 controller-2 controller-3; do
      [[ "$service" == "$excluded" ]] && continue
      payload=$(controller_status "$service")
      if is_leader_with_presence "$payload" "$committed_gateways" "$committed_routes" "$revalidated_gateways" "$eligible_routes"; then
        printf '%s\n' "$service"
        return 0
      fi
    done
    sleep 0.25
  done
  fail "no leader reached C(g=$committed_gateways,r=$committed_routes) V(g=$revalidated_gateways,r=$eligible_routes) within 60 seconds"
}

wait_for_gateways() {
  local gateway payload
  for ((attempt = 0; attempt < 240; attempt++)); do
    local ready=true
    for gateway in gateway-1 gateway-2; do
      payload=$(gateway_status "$gateway")
      if ! is_gateway_revalidated "$payload"; then
        ready=false
      fi
    done
    [[ "$ready" == true ]] && return 0
    sleep 0.25
  done
  fail "both gateways did not revalidate within 60 seconds"
}

wait_for_controller_ready() {
  local service=$1 port status body
  port=$(controller_admin_port "$service")
  for ((attempt = 0; attempt < 240; attempt++)); do
    body=$(mktemp "${TMPDIR:-/tmp}/relaygate-controller-ready.XXXXXX")
    status=$(curl --silent --show-error --output "$body" --write-out '%{http_code}' "http://127.0.0.1:${port}/healthz/ready" 2>/dev/null || true)
    if [[ "$status" == 200 ]] && python3 -c '
import json,sys
s=json.load(open(sys.argv[1]))
assert s["runtime_role"] == "controller"
assert s["status"] == "ready"
' "$body" 2>/dev/null; then
      rm -f "$body"
      return 0
    fi
    rm -f "$body"
    sleep 0.25
  done
  fail "$service did not regain Raft readiness within 60 seconds"
}

wait_for_quorum_loss() {
  local service=$1 port payload
  port=$(controller_admin_port "$service")
  for ((attempt = 0; attempt < 240; attempt++)); do
    payload=$(curl --silent --show-error "http://127.0.0.1:${port}/status" 2>/dev/null || true)
    if python3 -c '
import json,sys
s=json.loads(sys.argv[1])
assert s["runtime_role"] == "controller"
assert s["cluster_epoch"] == sys.argv[2]
assert s["raft"]["ready"] is False and s["raft"]["role"] != "Leader"
assert s["presence"]["state"] == "NoAuthority"
' "$payload" "$cohort_epoch" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  fail "$service did not fail closed after quorum loss within 60 seconds"
}

assert_storage_boundary() {
  local service container mounts
  for service in controller-1 controller-2 controller-3; do
    container=$("${compose[@]}" ps --quiet "$service")
    [[ -n "$container" ]] || fail "$service container was not found"
    mounts=$(docker inspect --format '{{range .Mounts}}{{println .Destination}}{{end}}' "$container")
    grep --fixed-strings --line-regexp --quiet '/var/lib/relaygate' <<<"$mounts" || fail "$service is missing its Raft data volume"
  done
  for service in gateway-1 gateway-2; do
    container=$("${compose[@]}" ps --quiet "$service")
    [[ -n "$container" ]] || fail "$service container was not found"
    mounts=$(docker inspect --format '{{range .Mounts}}{{println .Destination}}{{end}}' "$container")
    if grep --fixed-strings --line-regexp --quiet '/var/lib/relaygate' <<<"$mounts"; then
      fail "$service must not own a Raft data volume"
    fi
  done
}

wait_for_log_marker() {
  local name=$1 marker=$2
  for ((attempt = 0; attempt < 160; attempt++)); do
    docker logs "$name" 2>&1 | grep --fixed-strings --line-regexp --quiet "$marker" && return 0
    sleep 0.25
  done
  fail "$name did not emit $marker within 40 seconds"
}

membership_command() {
  local service=$1
  shift
  "${compose[@]}" exec --no-TTY "$service" /relaygate membership "$@" \
    -config /etc/relaygate/relaygate.yaml -timeout 5s
}

assert_membership_result() {
  local payload=$1 expected_changed=$2 expected_voters=$3
  python3 -c '
import json, sys
r = json.loads(sys.argv[1])
assert r["changed"] is (sys.argv[2] == "true")
members = r["members"]
assert len(members) == int(sys.argv[3])
assert members == sorted(members, key=lambda m: (m["node_id"], m["address"], m["suffrage"]))
assert all(m["suffrage"] == "Voter" for m in members)
' "$payload" "$expected_changed" "$expected_voters"
}

printf 'compose smoke: starting standard controller/gateway topology (%s)\n' "$project_name"
"${compose[@]}" config --quiet
owns_project=true
"${compose[@]}" up --detach --build

initial_leader=$(wait_for_leader 2 0 2 0)
wait_for_gateways
assert_storage_boundary
printf 'compose smoke: %s leads; both stateless gateways are revalidated\n' "$initial_leader"

membership=$(membership_command "$initial_leader" list)
assert_membership_result "$membership" false 3
membership=$(membership_command "$initial_leader" add -node-id "$initial_leader" -raft-address "$initial_leader:27400")
assert_membership_result "$membership" false 3
membership=$(membership_command "$initial_leader" remove -node-id "absent-$run_token")
assert_membership_result "$membership" false 3
for service in controller-1 controller-2 controller-3; do
  if [[ "$service" != "$initial_leader" ]]; then
    if membership_command "$service" list >/dev/null 2>&1; then
      fail "follower $service served leader-only membership state"
    fi
    break
  fi
done
printf 'compose smoke: local membership operator is leader-only and state-idempotent\n'

gateway_one=$("${compose[@]}" ps --quiet gateway-1)
gateway_two=$("${compose[@]}" ps --quiet gateway-2)
[[ -n "$gateway_one" && -n "$gateway_two" ]] || fail "gateway containers were not found"

docker build --file "$repo_root/Dockerfile.compose-test" --tag "$test_image" "$repo_root"
owns_test_image=true

# Controller-only ports must be closed inside a gateway namespace. Public
# Relay remains loopback-only and is exercised from that namespace below.
for closed_address in 127.0.0.1:27400 127.0.0.1:27410; do
  docker run --rm --network "container:$gateway_two" \
    --env "RELAYGATE_COMPOSE_CLOSED_ADDR=$closed_address" "$test_image" \
    -test.run '^TestComposePortClosed$' -test.v -test.count=1 -test.timeout=10s
done

docker run --rm --network "container:$gateway_one" \
  --env RELAYGATE_COMPOSE_RELAY_ADDR=127.0.0.1:27420 "$test_image"

docker run --detach --name "$proxy_name" --network "container:$gateway_two" \
  --env RELAYGATE_COMPOSE_PROXY_LISTEN_ADDR=0.0.0.0:17200 \
  --env RELAYGATE_COMPOSE_PROXY_TARGET_ADDR=127.0.0.1:27420 "$test_image" \
  -test.run '^TestComposeTCPProxy$' -test.v -test.count=1 -test.timeout=2m >/dev/null
wait_for_log_marker "$proxy_name" 'compose proxy listening'
docker run --rm --network "container:$gateway_one" \
  --env RELAYGATE_COMPOSE_CALLER_RELAY_ADDR=127.0.0.1:27420 \
  --env RELAYGATE_COMPOSE_LISTENER_RELAY_ADDR=gateway-2:17200 "$test_image" \
  -test.run '^TestComposeCrossGatewayRelaySmoke$' -test.v -test.count=1 -test.timeout=45s
proxy_exit=$(docker wait "$proxy_name")
docker logs "$proxy_name"
[[ "$proxy_exit" == 0 ]] || fail "cross-gateway proxy exited with status $proxy_exit"
docker rm "$proxy_name" >/dev/null

RELAYGATE_COMPOSE_PROJECT="$project_name" "$repo_root/scripts/compose-sdk-conformance.sh"

# Keep a gateway-local binding alive while leadership changes. Its committed
# record (C) must survive election; it is eligible (V) only after that gateway
# reconnects and replaces its complete snapshot at the new leader.
docker run --detach --name "$failover_name" --network "container:$gateway_two" \
  --env RELAYGATE_COMPOSE_RELAY_ADDR=127.0.0.1:27420 "$test_image" \
  -test.run '^TestComposeFailoverRedeclaresLiveBinding$' -test.v -test.count=1 -test.timeout=90s >/dev/null
wait_for_log_marker "$failover_name" 'compose failover binding ready'
wait_for_leader 2 1 2 1 >/dev/null

printf 'compose smoke: stopping leader %s\n' "$initial_leader"
"${compose[@]}" stop "$initial_leader"

# The new leader must retain committed current state and eventually revalidate
# both gateways. The transient V reset is covered deterministically by the
# authority tests; live gateways can reconnect before Compose observes it.
replacement_leader=""
for ((attempt = 0; attempt < 240; attempt++)); do
  for service in controller-1 controller-2 controller-3; do
    [[ "$service" == "$initial_leader" ]] && continue
    payload=$(controller_status "$service")
    if is_leader_with_presence "$payload" 2 1 0 0; then
      replacement_leader=$service
      break 2
    fi
  done
  sleep 0.1
done
[[ -n "$replacement_leader" ]] || fail "no replacement leader retained committed current state"
wait_for_gateways
wait_for_leader 2 1 2 1 "$initial_leader" >/dev/null
failover_exit=$(docker wait "$failover_name")
docker logs "$failover_name"
[[ "$failover_exit" == 0 ]] || fail "failover route did not recover"
docker rm "$failover_name" >/dev/null
printf 'compose smoke: C retained across failover; V revalidated and route opened\n'

printf 'compose smoke: restarting %s with its existing volume\n' "$initial_leader"
"${compose[@]}" start "$initial_leader"
wait_for_controller_ready "$initial_leader"

# A surviving single voter must reject authority work. Starting those exact
# stopped members again uses the same volumes and the same immutable epoch.
"${compose[@]}" stop controller-1 controller-2
wait_for_quorum_loss controller-3
if membership_command controller-3 list >/dev/null 2>&1; then
  fail "single controller served membership without quorum"
fi
printf 'compose smoke: quorum loss is fail closed\n'
"${compose[@]}" start controller-1 controller-2
# Readiness depends on observing a leader. Establish the quorum oracle first
# instead of imposing an arbitrary follower polling order during election.
wait_for_leader 2 0 0 0 >/dev/null
wait_for_controller_ready controller-1
wait_for_controller_ready controller-2
wait_for_gateways
wait_for_leader 2 0 2 0 >/dev/null

# Post-recovery current gateways must be able to declare and remove a new
# binding. This verifies the recovered same-epoch cluster is usable, not just
# elected.
docker run --rm --network "container:$gateway_one" \
  --env RELAYGATE_COMPOSE_RELAY_ADDR=127.0.0.1:27420 "$test_image"

# A coordinated controller outage is recoverable from the same three durable
# stores. The gateways remain stateless and rebuild only V after a new leader
# is elected; committed current state is read from the restored Raft FSM.
docker run --detach --name "$failover_name" --network "container:$gateway_two" \
  --env RELAYGATE_COMPOSE_RELAY_ADDR=127.0.0.1:27420 "$test_image" \
  -test.run '^TestComposeFailoverRedeclaresLiveBinding$' -test.v -test.count=1 -test.timeout=90s >/dev/null
wait_for_log_marker "$failover_name" 'compose failover binding ready'
wait_for_leader 2 1 2 1 >/dev/null

printf 'compose smoke: restarting the full controller cohort from existing volumes\n'
docker pause "$gateway_one" "$gateway_two" >/dev/null
gateways_paused=true
"${compose[@]}" stop controller-1 controller-2 controller-3
"${compose[@]}" start controller-1 controller-2 controller-3

restored_leader=""
observed_restored_c=false
for ((attempt = 0; attempt < 240; attempt++)); do
  for service in controller-1 controller-2 controller-3; do
    payload=$(controller_status "$service")
    if is_leader_with_presence "$payload" 2 1 0 0; then
      restored_leader=$service
      if python3 -c 'import json,sys; p=json.loads(sys.argv[1])["presence"]; assert p["revalidated_gateways"] < 2 or p["eligible_routes"] < 1' "$payload" 2>/dev/null; then
        observed_restored_c=true
      fi
      break 2
    fi
  done
  sleep 0.1
done
[[ -n "$restored_leader" ]] || fail "no leader restored the committed current route after full-cohort restart"
[[ "$observed_restored_c" == true ]] || fail "full-cohort restart did not expose durable C before V revalidation"
wait_for_controller_ready controller-1
wait_for_controller_ready controller-2
wait_for_controller_ready controller-3
docker unpause "$gateway_one" "$gateway_two" >/dev/null
gateways_paused=false
wait_for_gateways
wait_for_leader 2 1 2 1 >/dev/null
failover_exit=$(docker wait "$failover_name")
docker logs "$failover_name"
[[ "$failover_exit" == 0 ]] || fail "full-cohort restart route did not recover"
docker rm "$failover_name" >/dev/null

printf 'compose smoke: PASS (3 durable controllers, 2 stateless gateways, same/cross relay, Go/Rust SDK matrix, C/V failover, member/full-cohort persistent restart, quorum-loss recovery)\n'
