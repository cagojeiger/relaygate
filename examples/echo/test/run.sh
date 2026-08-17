#!/usr/bin/env bash
set -Eeuo pipefail

test_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
compose_file="$test_dir/../compose.yaml"
project_name=${RELAYGATE_ECHO_PROJECT:-relaygate-echo-test-$$}

if [[ ! "$project_name" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  printf 'echo test: invalid project name %q\n' "$project_name" >&2
  exit 2
fi

compose=(docker compose --file "$compose_file" --project-name "$project_name")

project_in_use() {
  docker ps -a --filter "label=com.docker.compose.project=$project_name" --format '{{.ID}}' | grep --quiet . ||
    docker volume ls --filter "label=com.docker.compose.project=$project_name" --format '{{.Name}}' | grep --quiet . ||
    docker network ls --filter "label=com.docker.compose.project=$project_name" --format '{{.Name}}' | grep --quiet . ||
    docker image ls --format '{{.Repository}}' | grep --fixed-strings --line-regexp --quiet "$project_name-relaygate" ||
    docker image ls --format '{{.Repository}}' | grep --fixed-strings --line-regexp --quiet "$project_name-echo-go" ||
    docker image ls --format '{{.Repository}}' | grep --fixed-strings --line-regexp --quiet "$project_name-echo-rust"
}

if project_in_use; then
  printf 'echo test: refusing to replace existing Compose project %q\n' "$project_name" >&2
  exit 2
fi

cleanup() {
  local status=$?
  local cleanup_output
  trap - EXIT INT TERM
  if ((status != 0)); then
    "${compose[@]}" logs >&2 2>&1 || true
  fi
  if ! cleanup_output=$("${compose[@]}" down --volumes --remove-orphans --rmi local 2>&1); then
    printf 'echo test: cleanup failed:\n%s\n' "$cleanup_output" >&2
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_ready() {
  local service=$1
  local marker=$2
  local container_id
  for ((attempt = 0; attempt < 240; attempt++)); do
    container_id=$("${compose[@]}" ps --quiet "$service")
    if [[ -n "$container_id" ]] &&
      [[ $(docker inspect --format '{{.State.Running}}' "$container_id") == true ]] &&
      docker logs "$container_id" 2>&1 | grep --fixed-strings --line-regexp --quiet "$marker"; then
      return 0
    fi
    sleep 0.25
  done
  printf 'echo test: %s did not emit %s\n' "$service" "$marker" >&2
  return 1
}

run_case() {
  local caller=$1
  local target=$2
  local case_name=$3
  local message="echo-${case_name}-$$"
  local output
  output=$("${compose[@]}" run --rm --no-deps "$caller" send "$target" "$message")
  if [[ "$output" != "ECHO_REPLY $message" ]]; then
    printf 'echo test: %s output %q, want %q\n' "$case_name" "$output" "ECHO_REPLY $message" >&2
    return 1
  fi
  printf 'ECHO_PASS %s\n' "$case_name"
}

assert_server_stable() {
  local service=$1
  local expected_id=$2
  local expected_restarts=$3
  local actual_id
  actual_id=$("${compose[@]}" ps --quiet "$service")
  if [[ "$actual_id" != "$expected_id" ]] ||
    [[ $(docker inspect --format '{{.State.Running}}' "$actual_id") != true ]] ||
    [[ $(docker inspect --format '{{.RestartCount}}' "$actual_id") != "$expected_restarts" ]]; then
    printf 'echo test: %s restarted or stopped after readiness\n' "$service" >&2
    return 1
  fi
}

"${compose[@]}" config --quiet
"${compose[@]}" up --build --detach relaygate echo-go echo-rust
wait_for_ready echo-go "ECHO_READY go"
wait_for_ready echo-rust "ECHO_READY rust"
go_server=$("${compose[@]}" ps --quiet echo-go)
rust_server=$("${compose[@]}" ps --quiet echo-rust)
go_restarts=$(docker inspect --format '{{.RestartCount}}' "$go_server")
rust_restarts=$(docker inspect --format '{{.RestartCount}}' "$rust_server")

run_case echo-go go go-go
run_case echo-go rust go-rust
run_case echo-rust go rust-go
run_case echo-rust rust rust-rust
assert_server_stable echo-go "$go_server" "$go_restarts"
assert_server_stable echo-rust "$rust_server" "$rust_restarts"

printf 'ECHO_PASS all\n'
