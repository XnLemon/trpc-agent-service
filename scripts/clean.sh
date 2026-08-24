#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

rm -rf "$ROOT/bin" coverage.out coverage.html coverage-base.out coverage-controlplane.out
go clean ./...
echo "cleaned"
