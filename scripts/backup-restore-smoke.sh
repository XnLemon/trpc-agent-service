#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  backup-restore-smoke.sh --check
  TRPC_BACKUP_DSN=... TRPC_BACKUP_FILE=... backup-restore-smoke.sh --backup
  TRPC_BACKUP_FILE=... TRPC_RESTORE_DSN=... backup-restore-smoke.sh --restore
  TRPC_BACKUP_DSN=... TRPC_RESTORE_DSN=... backup-restore-smoke.sh --rehearse

The rehearsal writes a custom-format PostgreSQL dump and restores it into the
explicit restore database. It never drops a database. Set
TRPC_RESTORE_ALLOW_CLEAN=1 only when the restore target is disposable.
EOF
}

require_tools() {
  local tool
  for tool in pg_dump pg_restore psql; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "::error::$tool is required" >&2
      return 1
    fi
  done
}

backup() {
  local dsn="${TRPC_BACKUP_DSN:-}"
  local file="${TRPC_BACKUP_FILE:-}"
  if [[ -z "$dsn" || -z "$file" ]]; then
    echo "::error::TRPC_BACKUP_DSN and TRPC_BACKUP_FILE are required" >&2
    return 1
  fi
  mkdir -p "$(dirname "$file")"
  pg_dump --format=custom --no-owner --file "$file" "$dsn"
  test -s "$file"
  pg_restore --list "$file" >/dev/null
  echo "backup created and validated"
}

restore() {
  local file="${TRPC_BACKUP_FILE:-}"
  local dsn="${TRPC_RESTORE_DSN:-}"
  if [[ -z "$file" || -z "$dsn" ]]; then
    echo "::error::TRPC_BACKUP_FILE and TRPC_RESTORE_DSN are required" >&2
    return 1
  fi
  if [[ ! -s "$file" ]]; then
    echo "::error::backup file is missing or empty" >&2
    return 1
  fi
  local -a clean_args=()
  if [[ "${TRPC_RESTORE_ALLOW_CLEAN:-0}" == "1" ]]; then
    clean_args+=(--clean --if-exists)
  fi
  pg_restore --exit-on-error --no-owner "${clean_args[@]}" --dbname "$dsn" "$file"
  psql --no-psqlrc --dbname "$dsn" --tuples-only --command 'SELECT 1' | tr -d '[:space:]' | grep -Fxq '1'
  echo "restore completed and connectivity verified"
}

main() {
  local mode="${1:-}"
  case "$mode" in
    --check)
      require_tools
      echo "backup and restore tools are available"
      ;;
    --backup)
      require_tools
      backup
      ;;
    --restore)
      require_tools
      restore
      ;;
    --rehearse)
      require_tools
      local temp_dir
      temp_dir="$(mktemp -d)"
      trap 'rm -rf -- "${temp_dir:-}"' EXIT
      TRPC_BACKUP_FILE="$temp_dir/rehearsal.dump" backup
      TRPC_BACKUP_FILE="$temp_dir/rehearsal.dump" restore
      echo "backup and restore rehearsal passed"
      ;;
    *)
      usage >&2
      return 2
      ;;
  esac
}

main "$@"
