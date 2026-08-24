#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

go test ./... -coverprofile=coverage.out

# Cross-domain PostgreSQL integration tests intentionally live outside each
# domain package. Instrument their concrete repository packages explicitly and
# merge the resulting profile so the CI/Codecov gate measures the exercised
# implementation rather than only the test package itself.
control_plane_packages="./trpcservice/agent/postgres,./trpcservice/backend/postgres,./trpcservice/channels/postgres,./trpcservice/model/postgres,./trpcservice/tenant/postgres"
go test ./trpcservice/controlplane/postgres -coverpkg="$control_plane_packages" -coverprofile=coverage-controlplane.out
go run ./scripts/mergecover -out coverage-merged.out coverage.out coverage-controlplane.out
mv coverage-merged.out coverage.out
rm -f coverage-controlplane.out

go tool cover -func=coverage.out
