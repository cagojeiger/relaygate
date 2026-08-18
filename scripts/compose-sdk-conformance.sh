#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file="$repo_root/docker-compose.yaml"
project_name=${RELAYGATE_COMPOSE_PROJECT:-${COMPOSE_PROJECT_NAME:-relaygate}}
relay_address=${RELAYGATE_SDK_RELAY_ADDRESS:-127.0.0.1:27420}
client_id=${RELAYGATE_SDK_CLIENT_ID:-local-development}
api_key_id=${RELAYGATE_SDK_API_KEY_ID:-primary}
api_key=${RELAYGATE_SDK_API_KEY:-relaygate-local-development-key}
run_token=$$

active_listener=""
active_caller=""
go_image=""
rust_image=""
remove_go_image=false
remove_rust_image=false

fail() {
  printf 'sdk conformance: %s\n' "$*" >&2
  return 1
}

container_logs() {
  local name=$1
  if [[ -n "$name" ]] && docker container inspect "$name" >/dev/null 2>&1; then
    printf 'logs for %s:\n' "$name" >&2
    docker logs "$name" >&2 2>&1 || true
  fi
}

remove_container() {
  local name=$1
  if [[ -n "$name" ]] && docker container inspect "$name" >/dev/null 2>&1; then
    docker rm --force "$name" >/dev/null 2>&1 || true
  fi
}

remove_generated_image() {
  local image=$1
  local remove=$2
  if [[ "$remove" == true && -n "$image" ]] && docker image inspect "$image" >/dev/null 2>&1; then
    docker image rm "$image" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    container_logs "$active_caller"
    container_logs "$active_listener"
  fi
  remove_container "$active_caller"
  remove_container "$active_listener"
  remove_generated_image "$go_image" "$remove_go_image"
  remove_generated_image "$rust_image" "$remove_rust_image"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ ! "$project_name" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  fail "project name must match ^[a-z0-9][a-z0-9_-]*$"
fi

# Explicit image variables are build output tags and are intentionally kept.
# Defaults are unique to this invocation and removed by the EXIT trap.
if [[ -n ${RELAYGATE_SDK_GO_IMAGE:-} ]]; then
  go_image=$RELAYGATE_SDK_GO_IMAGE
else
  go_image="relaygate-sdk-conformance-go:${project_name}-${run_token}"
  if docker image inspect "$go_image" >/dev/null 2>&1; then
    fail "refusing to replace existing generated image $go_image"
  fi
  remove_go_image=true
fi
if [[ -n ${RELAYGATE_SDK_RUST_IMAGE:-} ]]; then
  rust_image=$RELAYGATE_SDK_RUST_IMAGE
else
  rust_image="relaygate-sdk-conformance-rust:${project_name}-${run_token}"
  if docker image inspect "$rust_image" >/dev/null 2>&1; then
    fail "refusing to replace existing generated image $rust_image"
  fi
  remove_rust_image=true
fi

compose=(docker compose --file "$compose_file" --project-name "$project_name")
caller_gateway=$("${compose[@]}" ps --quiet gateway-1)
listener_gateway=$("${compose[@]}" ps --quiet gateway-2)
if [[ -z "$caller_gateway" || -z "$listener_gateway" ]]; then
  fail "project $project_name must have running gateway-1 and gateway-2 services"
fi
if [[ $(docker inspect --format '{{.State.Running}}' "$caller_gateway") != true || $(docker inspect --format '{{.State.Running}}' "$listener_gateway") != true ]]; then
  fail "gateway-1 and gateway-2 must both be running"
fi

docker build --file "$repo_root/Dockerfile.sdk-conformance" --target go-runtime --tag "$go_image" "$repo_root"
docker build --file "$repo_root/Dockerfile.sdk-conformance" --target rust-runtime --tag "$rust_image" "$repo_root"

image_for() {
  case $1 in
    go) printf '%s\n' "$go_image" ;;
    rust) printf '%s\n' "$rust_image" ;;
    *) fail "unsupported SDK language $1" ;;
  esac
}

assert_unused_name() {
  local name=$1
  if docker container inspect "$name" >/dev/null 2>&1; then
    fail "refusing to replace existing container $name"
  fi
}

create_role() {
  local name=$1
  local gateway=$2
  local image=$3
  local role=$4
  local case_name=$5
  docker create \
    --name "$name" \
    --network "container:$gateway" \
    --env "RELAYGATE_SDK_ROLE=$role" \
    --env "RELAYGATE_SDK_CASE=$case_name" \
    --env "RELAYGATE_SDK_RELAY_ADDRESS=$relay_address" \
    --env "RELAYGATE_SDK_CLIENT_ID=$client_id" \
    --env "RELAYGATE_SDK_API_KEY_ID=$api_key_id" \
    --env "RELAYGATE_SDK_API_KEY=$api_key" \
    "$image" >/dev/null
}

wait_for_ready() {
  local name=$1
  local marker=$2
  local state
  for ((attempt = 0; attempt < 180; attempt++)); do
    if docker logs "$name" 2>&1 | grep --fixed-strings --line-regexp --quiet "$marker"; then
      return 0
    fi
    state=$(docker inspect --format '{{.State.Status}}' "$name")
    if [[ "$state" != running && "$state" != created ]]; then
      fail "$name exited before $marker"
    fi
    sleep 0.25
  done
  fail "$name did not emit $marker within 45 seconds"
}

wait_for_exit() {
  local name=$1
  local state
  for ((attempt = 0; attempt < 220; attempt++)); do
    state=$(docker inspect --format '{{.State.Status}}' "$name")
    if [[ "$state" == exited || "$state" == dead ]]; then
      return 0
    fi
    sleep 0.25
  done
  fail "$name did not exit within 55 seconds"
}

verify_pass() {
  local name=$1
  local case_name=$2
  local exit_code
  exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$name")
  if [[ "$exit_code" != 0 ]]; then
    fail "$name exited with status $exit_code"
  fi
  if ! docker logs "$name" 2>&1 | grep --fixed-strings --line-regexp --quiet "SDK_PASS $case_name"; then
    fail "$name omitted SDK_PASS $case_name"
  fi
}

run_case() {
  local case_name=$1
  local caller_language=$2
  local listener_language=$3
  local listener_image caller_image listener_name caller_name

  listener_image=$(image_for "$listener_language")
  caller_image=$(image_for "$caller_language")
  listener_name="relaygate-sdk-${project_name}-${run_token}-${case_name}-listener"
  caller_name="relaygate-sdk-${project_name}-${run_token}-${case_name}-caller"
  assert_unused_name "$listener_name"
  assert_unused_name "$caller_name"

  create_role "$listener_name" "$listener_gateway" "$listener_image" listener "$case_name"
  active_listener=$listener_name
  docker start "$listener_name" >/dev/null
  wait_for_ready "$listener_name" "SDK_READY $case_name"

  create_role "$caller_name" "$caller_gateway" "$caller_image" caller "$case_name"
  active_caller=$caller_name
  docker start "$caller_name" >/dev/null
  wait_for_exit "$caller_name"
  wait_for_exit "$listener_name"
  verify_pass "$caller_name" "$case_name"
  verify_pass "$listener_name" "$case_name"

  printf 'SDK conformance %s (%s caller -> %s listener) passed\n' "$case_name" "$caller_language" "$listener_language"
  docker logs "$caller_name"
  docker logs "$listener_name"
  docker rm --force "$caller_name" >/dev/null
  active_caller=""
  docker rm --force "$listener_name" >/dev/null
  active_listener=""
}

run_case go-go go go
run_case go-rust go rust
run_case rust-go rust go
run_case rust-rust rust rust
