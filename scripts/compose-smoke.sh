#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file="$repo_root/docker-compose.yaml"
run_token=$$
project_name=${RELAYGATE_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME:-relaygate-smoke-$run_token}}
test_image="relaygate-compose-test:smoke-$run_token"
proxy_name="relaygate-compose-proxy-$run_token"
failover_name="relaygate-compose-failover-$run_token"

owns_project=false
owns_test_image=false

cd "$repo_root"

fail() {
  printf 'compose smoke: %s\n' "$*" >&2
  return 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}

remove_proxy() {
  if docker container inspect "$proxy_name" >/dev/null 2>&1; then
    docker rm --force "$proxy_name" >/dev/null 2>&1 || true
  fi
}

remove_failover() {
  if docker container inspect "$failover_name" >/dev/null 2>&1; then
    docker rm --force "$failover_name" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local exit_status=$?
  trap - EXIT INT TERM
  if (( exit_status != 0 )) && [[ "$owns_project" == true ]]; then
    printf 'compose smoke: cluster logs after failure\n' >&2
    "${compose[@]}" logs >&2 2>&1 || true
  fi
  remove_proxy
  remove_failover
  if [[ "$owns_project" == true ]]; then
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  if [[ "$owns_test_image" == true ]]; then
    docker image rm "$test_image" >/dev/null 2>&1 || true
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

require_command curl
require_command docker
require_command go
require_command python3

if [[ ! "$project_name" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  fail "project name must match ^[a-z0-9][a-z0-9_-]*$"
fi

compose=(docker compose --file "$compose_file" --project-name "$project_name")

existing_containers=$(docker container ls --all --quiet --filter "label=com.docker.compose.project=$project_name")
existing_volumes=$(docker volume ls --quiet --filter "label=com.docker.compose.project=$project_name")
existing_networks=$(docker network ls --quiet --filter "label=com.docker.compose.project=$project_name")
if [[ -n "$existing_containers$existing_volumes$existing_networks" ]]; then
  fail "refusing to replace existing Compose project $project_name"
fi
if docker image inspect "$test_image" >/dev/null 2>&1; then
  fail "refusing to replace existing test image $test_image"
fi

is_current_leader() {
  local payload=$1
  local expected_sessions=$2
  local expected_bindings=$3
  python3 -c '
import json
import sys

status = json.loads(sys.argv[1])
presence = status["presence"]
expected_sessions = int(sys.argv[2])
expected_bindings = int(sys.argv[3])
assert status["role"] == "Leader"
assert status["gateway_control"]["state"] == "Revalidated"
assert presence == {
    "state": "Current",
    "sessions": expected_sessions,
    "revalidated": expected_sessions,
    "bindings": expected_bindings,
}
' "$payload" "$expected_sessions" "$expected_bindings" 2>/dev/null
}

wait_for_leader() {
  local expected_sessions=$1
  local expected_bindings=$2
  local excluded_service=${3:-}
  local mapping service admin_port control_port relay_status
  for ((attempt = 0; attempt < 180; attempt++)); do
    for mapping in relaygate-1:27491:27411 relaygate-2:27492:27412 relaygate-3:27493:27413; do
      IFS=: read -r service admin_port control_port <<< "$mapping"
      if [[ "$service" == "$excluded_service" ]]; then
        continue
      fi
      relay_status=$(curl --fail --silent --show-error "http://127.0.0.1:${admin_port}/status" 2>/dev/null || true)
      if is_current_leader "$relay_status" "$expected_sessions" "$expected_bindings"; then
        printf '%s:%s\n' "$service" "$control_port"
        return 0
      fi
    done
    sleep 0.5
  done
  fail "no current leader reached sessions=$expected_sessions bindings=$expected_bindings within 90 seconds"
}

printf 'compose smoke: starting isolated project %s\n' "$project_name"
owns_project=true
"${compose[@]}" up --detach --build

initial_leader=$(wait_for_leader 3 0)
leader_service=${initial_leader%%:*}
printf 'compose smoke: initial leader is %s\n' "$leader_service"

docker build --file "$repo_root/Dockerfile.compose-test" --tag "$test_image" "$repo_root"
owns_test_image=true

caller_container=$("${compose[@]}" ps --quiet relaygate-1)
listener_container=$("${compose[@]}" ps --quiet relaygate-2)
if [[ -z "$caller_container" || -z "$listener_container" ]]; then
  fail "relaygate-1 and relaygate-2 are not running"
fi

docker run --rm \
  --network "container:$caller_container" \
  --env RELAYGATE_COMPOSE_RELAY_ADDR=127.0.0.1:27420 \
  "$test_image"

docker run --detach \
  --name "$proxy_name" \
  --network "container:$listener_container" \
  --env RELAYGATE_COMPOSE_PROXY_LISTEN_ADDR=0.0.0.0:17200 \
  --env RELAYGATE_COMPOSE_PROXY_TARGET_ADDR=127.0.0.1:27420 \
  "$test_image" \
  -test.run '^TestComposeTCPProxy$' -test.v -test.count=1 -test.timeout=2m >/dev/null

proxy_ready=false
for ((attempt = 0; attempt < 150; attempt++)); do
  if docker logs "$proxy_name" 2>&1 | grep --quiet 'compose proxy listening'; then
    proxy_ready=true
    break
  fi
  sleep 0.2
done
if [[ "$proxy_ready" != true ]]; then
  fail "cross-Gateway proxy did not become ready within 30 seconds"
fi

docker run --rm \
  --network "container:$caller_container" \
  --env RELAYGATE_COMPOSE_CALLER_RELAY_ADDR=127.0.0.1:27420 \
  --env RELAYGATE_COMPOSE_LISTENER_RELAY_ADDR=relaygate-2:17200 \
  "$test_image" \
  -test.run '^TestComposeCrossGatewayRelaySmoke$' -test.v -test.count=1 -test.timeout=45s

proxy_exit=$(docker wait "$proxy_name")
docker logs "$proxy_name"
if [[ "$proxy_exit" != 0 ]]; then
  fail "cross-Gateway proxy exited with status $proxy_exit"
fi
docker rm "$proxy_name" >/dev/null

RELAYGATE_COMPOSE_PROJECT="$project_name" "$repo_root/scripts/compose-sdk-conformance.sh"

failover_service=""
for service in relaygate-1 relaygate-2 relaygate-3; do
  if [[ "$service" != "$leader_service" ]]; then
    failover_service=$service
    break
  fi
done
failover_container=$("${compose[@]}" ps --quiet "$failover_service")
if [[ -z "$failover_container" ]]; then
  fail "surviving Gateway container was not found"
fi
docker run --detach \
  --name "$failover_name" \
  --network "container:$failover_container" \
  --env RELAYGATE_COMPOSE_RELAY_ADDR=127.0.0.1:27420 \
  "$test_image" \
  -test.run '^TestComposeFailoverRedeclaresLiveBinding$' -test.v -test.count=1 -test.timeout=90s >/dev/null

failover_ready=false
for ((attempt = 0; attempt < 150; attempt++)); do
  if docker logs "$failover_name" 2>&1 | grep --quiet 'compose failover binding ready'; then
    failover_ready=true
    break
  fi
  sleep 0.2
done
if [[ "$failover_ready" != true ]]; then
  fail "failover listener did not bind within 30 seconds"
fi

printf 'compose smoke: stopping leader %s\n' "$leader_service"
"${compose[@]}" stop "$leader_service"
replacement_leader=$(wait_for_leader 2 1 "$leader_service")
printf 'compose smoke: replacement leader is %s\n' "${replacement_leader%%:*}"

failover_exit=$(docker wait "$failover_name")
docker logs "$failover_name"
if [[ "$failover_exit" != 0 ]]; then
  fail "failover redeclare test exited with status $failover_exit"
fi
docker rm "$failover_name" >/dev/null

printf 'compose smoke: PASS (3-node current state, same/cross-Gateway relay, Go/Rust SDK matrix, live-binding failover redeclare)\n'
