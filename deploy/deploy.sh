#!/usr/bin/env bash

set -euo pipefail

# Move to repo root (script is in deploy/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

choose_compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  elif command_exists docker-compose; then
    echo "docker-compose"
  else
    echo "" # not found
  fi
}

echo "==> Checking and installing Docker if needed..."
if ! command_exists docker; then
  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
  fi

  if [[ "${ID:-}" == "ubuntu" || "${ID_LIKE:-}" == *debian* ]]; then
    sudo apt-get update -y
    sudo apt-get install -y ca-certificates curl gnupg lsb-release
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL "https://download.docker.com/linux/${ID:-ubuntu}/gpg" | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/${ID:-ubuntu} $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo apt-get update -y
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo systemctl enable --now docker
  else
    echo "Unsupported distro. Please install Docker CE and Compose manually." >&2
    exit 1
  fi
else
  echo "Docker is already installed."
fi

echo "==> Verifying Compose..."
COMPOSE_CMD="$(choose_compose_cmd)"
if [[ -z "$COMPOSE_CMD" ]]; then
  echo "Docker Compose is not available. Ensure docker-compose plugin or binary is installed." >&2
  exit 1
fi

if groups "$USER" 2>/dev/null | grep -q "\bdocker\b"; then
  SUDO=""
else
  SUDO="sudo"
fi

echo "==> Validating .env presence and required variables..."
if [[ ! -f .env ]]; then
  cat >&2 <<'EOF'
Missing .env file in the project root.
Create a .env file with required variables, for example:

TELEGRAM_BOT_TOKEN=your_telegram_bot_token
SONAR_API_KEY=your_sonar_api_key
EOF
  exit 1
fi

require_env() {
  local key="$1"
  if ! grep -Eq "^${key}=[^#\n]+" .env; then
    echo "Missing or empty ${key} in .env" >&2
    exit 1
  fi
}

# Required by docker-compose.yml
require_env TELEGRAM_BOT_TOKEN
require_env SONAR_API_KEY

echo "==> Building and starting services..."
$SUDO $COMPOSE_CMD pull || true
$SUDO $COMPOSE_CMD up -d --build

echo "==> Validating connectivity to Telegram..."
set +e
if command_exists curl; then
  curl -m 5 -sS https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe >/dev/null
  TG_RC=$?
else
  TG_RC=0
fi
set -e
if [[ "$TG_RC" -ne 0 ]]; then
  echo "Warning: Can't reach Telegram API from this host. Ensure outbound 443/TCP is allowed." >&2
fi

echo "==> Deployment complete. Useful commands:"
echo "   $COMPOSE_CMD ps"
echo "   $COMPOSE_CMD logs -f"
echo "   $COMPOSE_CMD down    # to stop and remove containers"


