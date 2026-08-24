#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Cross-domain PostgreSQL integration tests intentionally live outside each
# domain package. Instrument their concrete repository packages explicitly and
# merge the resulting profile so the CI/Codecov gate measures the exercised
# implementation rather than only the test package itself. Exclude that suite
# from the ordinary package pass so it does not create its integration schema
# twice against the CI PostgreSQL service.
mapfile -t base_packages < <(go list ./... | grep -v '/trpcservice/controlplane/postgres$')
base_profile=coverage-base.out
control_profile=coverage-controlplane.out
go test "${base_packages[@]}" -coverprofile="$base_profile"

control_plane_packages="./trpcservice/agent/postgres,./trpcservice/backend/postgres,./trpcservice/channels/postgres,./trpcservice/model/postgres,./trpcservice/tenant/postgres"
go test ./trpcservice/controlplane/postgres -coverpkg="$control_plane_packages" -coverprofile="$control_profile"
go run ./scripts/mergecover -out coverage-merged.out "$base_profile" "$control_profile"
mv coverage-merged.out coverage.out

go tool cover -func=coverage.out
