#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./deploy.sh \
#     --repo "https://github.com/you/tapi_llm_bot.git" \
#     --branch "main" \
#     --bot-token "<TELEGRAM_BOT_TOKEN>" \
#     --sonar-key "<PERPLEXITY_SONAR_API_KEY>"
#
# Optional:
#     --workdir "/opt/tapi_llm_bot"
#
# This script installs Docker + Compose, clones/updates the repo,
# writes .env and docker-compose.prod.yml, and runs the stack.

REPO=""
BRANCH="main"
WORKDIR="/opt/tapi_llm_bot"
BOT_TOKEN=""
SONAR_KEY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    --bot-token) BOT_TOKEN="$2"; shift 2 ;;
    --sonar-key) SONAR_KEY="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -z "$REPO" || -z "$BOT_TOKEN" || -z "$SONAR_KEY" ]]; then
  echo "Missing required args. See script header for usage." >&2
  exit 1
fi

# Ensure required packages
if ! command -v docker >/dev/null 2>&1; then
  echo "Installing Docker..."
  sudo apt-get update -y
  sudo apt-get install -y ca-certificates curl gnupg lsb-release
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
  sudo usermod -aG docker "$USER" || true
fi

# Prepare workdir
sudo mkdir -p "$WORKDIR"
sudo chown "$USER":"$USER" "$WORKDIR"
cd "$WORKDIR"

# Clone or update
if [[ ! -d "$WORKDIR/.git" ]]; then
  git clone "$REPO" .
else
  git fetch --all --prune
fi

git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH" || true

# Create .env (do not commit)
cat > .env << EOF
TELEGRAM_BOT_TOKEN=$BOT_TOKEN
SONAR_API_KEY=$SONAR_KEY
EOF

# Production override to persist postgres and avoid exposing services
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

# Build and run
docker compose --env-file .env -f docker-compose.yml -f docker-compose.prod.yml up -d --build

echo "Deployment complete. Check logs: docker compose logs -f bot_gateway storage_service"
