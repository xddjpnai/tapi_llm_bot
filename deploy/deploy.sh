#!/usr/bin/env bash
set -euo pipefail

# Minimal deploy runner for an already-cloned repo.
#
# Usage examples:
#   ./deploy.sh --bot-token "<TELEGRAM_BOT_TOKEN>" --sonar-key "<PERPLEXITY_SONAR_API_KEY>"
#   TELEGRAM_BOT_TOKEN=... SONAR_API_KEY=... ./deploy.sh
#
# Flags:
#   --bot-token  TELEGRAM_BOT_TOKEN value
#   --sonar-key  SONAR_API_KEY value
#   --no-build   Run compose without --build

BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
SONAR_KEY="${SONAR_API_KEY:-}"
NO_BUILD=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bot-token) BOT_TOKEN="$2"; shift 2 ;;
    --sonar-key) SONAR_KEY="$2"; shift 2 ;;
    --no-build) NO_BUILD=true; shift 1 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

write_env_file() {
  local bot_token sonar_key
  bot_token="${BOT_TOKEN}"
  sonar_key="${SONAR_KEY}"
  if [[ ! -f .env ]]; then
    cat > .env << EOF
TELEGRAM_BOT_TOKEN=${bot_token}
SONAR_API_KEY=${sonar_key}
EOF
    echo "[i] .env создан"
  else
    echo "[i] .env уже существует — пропускаю создание"
  fi
  if [[ -z "${bot_token}" || -z "${sonar_key}" ]]; then
    echo "[WARN] Значения токенов пустые. Отредактируйте .env перед запуском." >&2
  fi
}

ensure_prod_override() {
  if [[ ! -f docker-compose.prod.yml ]]; then
    cat > docker-compose.prod.yml << 'EOF'
services:
  postgres:
    volumes:
      - pgdata:/var/lib/postgresql/data
  redpanda:
    ports: []
  chart_service:
    ports: []
volumes:
  pgdata:
EOF
    echo "[i] docker-compose.prod.yml создан"
  else
    echo "[i] docker-compose.prod.yml уже существует — пропускаю"
  fi
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "[ERR] Docker не установлен. Установите Docker и перезапустите скрипт." >&2
    exit 1
  fi
}

main() {
  require_docker
  write_env_file
  ensure_prod_override

  local compose_cmd=(docker compose --env-file .env -f docker-compose.yml -f docker-compose.prod.yml up -d)
  if [[ "${NO_BUILD}" != true ]]; then
    compose_cmd+=(--build)
  fi
  echo "[i] Запускаю: ${compose_cmd[*]}"
  "${compose_cmd[@]}"
  echo "[✓] Готово. Логи: docker compose logs -f bot_gateway storage_service"
}

main "$@"
