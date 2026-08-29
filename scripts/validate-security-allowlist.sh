#!/usr/bin/env bash
set -euo pipefail

ROOT="${SECURITY_ALLOWLIST_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
CONFIG="$ROOT/gitleaks.toml"
TRIVYIGNORE="$ROOT/.trivyignore"
today="$(date -u +%F)"
found=0
gitleaks_entries=0
shopt -s extglob

validate_metadata() {
  local metadata="$1"
  local location="$2"
  local owner issue reason expiry
  if [[ "$metadata" =~ Owner:[[:space:]]*([^[:space:]|]+) ]]; then
    owner="${BASH_REMATCH[1]}"
  else
    echo "::error::security exception near $location lacks an owner" >&2
    exit 1
  fi
  if [[ "$metadata" =~ Issue:[[:space:]]*(#[0-9]+) ]]; then
    issue="${BASH_REMATCH[1]}"
  else
    echo "::error::security exception near $location lacks a tracking issue" >&2
    exit 1
  fi
  if [[ "$metadata" =~ Reason:[[:space:]]*([^|]+) ]]; then
    reason="${BASH_REMATCH[1]}"
    reason="${reason##+([[:space:]])}"
    reason="${reason%%+([[:space:]])}"
  else
    reason=""
  fi
  if [[ -z "$reason" ]]; then
    echo "::error::security exception near $location lacks a rationale" >&2
    exit 1
  fi
  if [[ "$metadata" =~ allowlist-expiry:[[:space:]]*([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
    expiry="${BASH_REMATCH[1]}"
  else
    echo "::error::invalid allowlist expiry near $location" >&2
    exit 1
  fi
  if ! normalized_expiry="$(date -u -d "$expiry" +%F 2>/dev/null)" || [[ "$normalized_expiry" != "$expiry" ]]; then
    echo "::error::invalid calendar date near $location: $expiry" >&2
    exit 1
  fi
  if [[ "$expiry" < "$today" || "$expiry" == "$today" ]]; then
    echo "::error::expired security allowlist entry: $expiry" >&2
    exit 1
  fi
}

while IFS= read -r line; do
  line="${line%$'\r'}"
  if [[ "$line" =~ ^[[:space:]]*(commits|paths)[[:space:]]*=[[:space:]]*\[ && "$line" != *"[]"* ]]; then
    gitleaks_entries=1
  fi
  if [[ "$line" == \#*allowlist-expiry:* ]]; then
    found=1
    validate_metadata "$line" "$CONFIG"
  fi
done < "$CONFIG"

if [[ "$gitleaks_entries" -eq 1 && "$found" -eq 0 ]]; then
  echo "::error::active Gitleaks allowlist entries lack owner, rationale, issue, and expiry metadata" >&2
  exit 1
fi

if [[ -f "$TRIVYIGNORE" ]]; then
  trivy_found=0
  metadata=""
  line_no=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line_no=$((line_no + 1))
    trimmed="${line##+([[:space:]])}"
    trimmed="${trimmed%%+([[:space:]])}"
    [[ -z "$trimmed" ]] && continue
    if [[ "$trimmed" == \#* ]]; then
      if [[ "$trimmed" == *"Owner:"* || "$trimmed" == *"Issue:"* || "$trimmed" == *"allowlist-expiry:"* ]]; then
        metadata="$trimmed"
        validate_metadata "$metadata" "$TRIVYIGNORE:$line_no"
      fi
      continue
    fi
    if [[ ! "$trimmed" =~ ^(CVE|GHSA|RUSTSEC)-[A-Za-z0-9._:-]+$ ]]; then
      echo "::error::invalid Trivy vulnerability ID near $TRIVYIGNORE:$line_no" >&2
      exit 1
    fi
    if [[ -z "$metadata" ]]; then
      echo "::error::Trivy exception $trimmed lacks owner, issue, and expiry metadata" >&2
      exit 1
    fi
    trivy_found=1
    metadata=""
  done < "$TRIVYIGNORE"
  if [[ -n "$metadata" ]]; then
    echo "::error::Trivy exception metadata has no following vulnerability ID" >&2
    exit 1
  fi
  if [[ "$trivy_found" -eq 0 ]]; then
    echo "Trivy allowlist has no entries"
  else
    echo "Trivy allowlist metadata checks passed"
  fi
else
  echo "Trivy allowlist file not present"
fi

if [[ "$found" -eq 0 ]]; then
  echo "security allowlist has no entries"
else
  echo "security allowlist expiry checks passed"
fi
