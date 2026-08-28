#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

assert_dockerignore_entry() {
  local entry="$1"
  if ! grep -Fqx "$entry" .dockerignore; then
    echo "::error::.dockerignore is missing: $entry" >&2
    exit 1
  fi
}

# A developer may copy the example to deploy/service.env before starting
# Compose. Keep both the concrete file and other populated env files out of
# the Docker build context while retaining *.env.example documentation.
assert_dockerignore_entry "deploy/service.env"
assert_dockerignore_entry "deploy/*.env"

compose_config="$(mktemp)"
kustomize_output="$(mktemp)"
trap 'rm -f "$compose_config" "$kustomize_output"' EXIT

docker compose \
  --env-file deploy/service.env.example \
  -f deploy/docker-compose.yml \
  config >"$compose_config"
grep -Fq -- '0.0.0.0:8080' "$compose_config"

kubectl kustomize deploy/kubernetes >"$kustomize_output"
grep -Fq -- 'image: ghcr.io/xnlemon/trpc-agent-service:0.1.0' "$kustomize_output"
grep -Fq -- '0.0.0.0:8080' "$kustomize_output"

echo "deployment manifests and build-context guards validated"
