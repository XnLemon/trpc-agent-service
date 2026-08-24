#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# The PostgreSQL control-plane integration suite exercises repositories that
# live in sibling packages. One native Go profile instruments every package,
# so Codecov receives the same cross-package execution data as `go tool cover`
# without depending on a custom profile merger or running the integration
# schema setup twice.
coverage_packages="$(go list ./... | paste -sd, -)"
go test -coverpkg="$coverage_packages" -coverprofile=coverage.out ./...

go tool cover -func=coverage.out
