#!/usr/bin/env bash
set -euo pipefail

ROOT="${SECURITY_ALLOWLIST_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
CONFIG="$ROOT/gitleaks.toml"
TRIVYIGNORE="$ROOT/.trivyignore"
today="$(date -u +%F)"
found=0
shopt -s extglob

if grep -q 'Owner:' "$CONFIG" && ! grep -q 'Issue: #[0-9]' "$CONFIG"; then
  echo "::error::security allowlist entries must include a tracking issue" >&2
  exit 1
fi

while IFS= read -r line; do
  line="${line%$'\r'}"
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
        owner="${metadata#*Owner: }"
        owner="${owner%% *}"
        issue="${metadata#*Issue: }"
        issue="${issue%% *}"
        expiry="${metadata#*allowlist-expiry: }"
        expiry="${expiry%% *}"
        if [[ -z "$owner" || ! "$issue" =~ ^#[0-9]+$ || ! "$expiry" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
          echo "::error::invalid Trivy exception metadata near $TRIVYIGNORE:$line_no" >&2
          exit 1
        fi
        if [[ "$expiry" < "$today" || "$expiry" == "$today" ]]; then
          echo "::error::expired Trivy exception: $expiry" >&2
          exit 1
        fi
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
