#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "$#" -gt 1 ]]; then
  echo "usage: $0 [env-file]" >&2
  exit 2
fi

env_file="${1:-}"
if [[ -z "$env_file" ]]; then
  if [[ -f deploy/service.env ]]; then
    env_file=deploy/service.env
  else
    env_file=deploy/service.env.example
  fi
fi
if [[ ! -f "$env_file" ]]; then
  echo "environment file not found: $env_file" >&2
  exit 1
fi

compose=(docker compose --env-file "$env_file" -f deploy/docker-compose.yml)
"${compose[@]}" up -d --build --wait
"${compose[@]}" exec -T service /app/trpc-healthcheck http://127.0.0.1:8080/healthz
"${compose[@]}" exec -T service /app/trpc-healthcheck http://127.0.0.1:8080/readyz

echo "tRPC Agent Service is healthy and ready"
echo "stop it with: ${compose[*]} down"
