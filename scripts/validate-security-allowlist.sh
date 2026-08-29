#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$ROOT/gitleaks.toml"
today="$(date -u +%F)"
found=0

if grep -q 'Owner:' "$CONFIG" && ! grep -q 'Issue: #[0-9]' "$CONFIG"; then
  echo "::error::security allowlist entries must include a tracking issue" >&2
  exit 1
fi

while IFS= read -r line; do
  case "$line" in
    \#*allowlist-expiry:*)
      found=1
      expiry="${line##*allowlist-expiry: }"
      expiry="${expiry%% *}"
      if [[ ! "$expiry" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "::error::invalid allowlist expiry: $expiry" >&2
        exit 1
      fi
      if [[ "$expiry" < "$today" || "$expiry" == "$today" ]]; then
        echo "::error::expired security allowlist entry: $expiry" >&2
        exit 1
      fi
      ;;
  esac
done < "$CONFIG"

if [[ "$found" -eq 0 ]]; then
  echo "security allowlist has no entries"
else
  echo "security allowlist expiry checks passed"
fi
