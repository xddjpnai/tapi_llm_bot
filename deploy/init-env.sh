#!/usr/bin/env bash

set -euo pipefail

# Go to repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

ENV_FILE=".env"

echo "==> Creating/updating $ENV_FILE in: $PROJECT_DIR"

confirm_overwrite() {
  local file="$1"
  if [[ -f "$file" ]]; then
    read -r -p "File $file exists. Overwrite? [y/N]: " ans || true
    case "${ans:-}" in
      y|Y|yes|YES) ;;
      *) echo "Aborted."; exit 0 ;;
    esac
  fi
}

prompt_secret() {
  local var_name="$1"
  local prompt_text="$2"
  local current_value="${!var_name:-}"
  if [[ -n "$current_value" ]]; then
    # If already exported in current shell, reuse without prompting
    printf -v "$var_name" '%s' "$current_value"
    return 0
  fi
  while true; do
    read -r -s -p "$prompt_text: " input
    echo
    if [[ -z "$input" ]]; then
      echo "Value cannot be empty. Try again."
      continue
    fi
    if [[ "$input" =~ [[:space:]] ]]; then
      echo "Value must not contain spaces. Try again."
      continue
    fi
    printf -v "$var_name" '%s' "$input"
    break
  done
}

confirm_overwrite "$ENV_FILE"

# Read values (use existing env if already exported)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
SONAR_API_KEY="${SONAR_API_KEY:-}"

if [[ -z "$TELEGRAM_BOT_TOKEN" ]]; then
  prompt_secret TELEGRAM_BOT_TOKEN "Enter TELEGRAM_BOT_TOKEN"
fi
if [[ -z "$SONAR_API_KEY" ]]; then
  prompt_secret SONAR_API_KEY "Enter SONAR_API_KEY"
fi

cat > "$ENV_FILE" <<EOF
TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN
SONAR_API_KEY=$SONAR_API_KEY
EOF

chmod 600 "$ENV_FILE" || true

echo "==> $ENV_FILE written. Keys (masked):"
awk -F= '{v=$2; if(length(v)>6){m=substr(v,1,3)"***"substr(v,length(v)-2)} else {m="***"}; printf "%s=%s\n", $1, m}' "$ENV_FILE"

echo "==> Done. You can now run: bash deploy/deploy.sh"


