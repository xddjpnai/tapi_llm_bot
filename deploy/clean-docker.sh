#!/usr/bin/env bash

set -euo pipefail

echo "==> Docker cleanup utility (cache, images, containers, networks, volumes)"
echo "    Usage: $0 [--deep]"
echo "    --deep: stop Docker and wipe /var/lib/docker & /var/lib/containerd (DANGEROUS)"

command_exists() { command -v "$1" >/dev/null 2>&1; }

choose_compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  elif command_exists docker-compose; then
    echo "docker-compose"
  else
    echo ""
  fi
}

DEEP=0
if [[ "${1:-}" == "--deep" ]]; then
  DEEP=1
fi

if groups "$USER" 2>/dev/null | grep -q "\bdocker\b"; then
  SUDO=""
else
  SUDO="sudo"
fi

COMPOSE_CMD="$(choose_compose_cmd)"

echo "==> Stopping and removing ALL containers (running and stopped)..."
$SUDO docker ps -aq | xargs -r $SUDO docker stop >/dev/null 2>&1 || true
$SUDO docker ps -aq | xargs -r $SUDO docker rm -f >/dev/null 2>&1 || true

if [[ -n "$COMPOSE_CMD" ]]; then
  echo "==> Bringing down compose project (with volumes), if present..."
  $SUDO $COMPOSE_CMD down -v || true
fi

echo "==> Pruning images, layers cache, networks, and UNUSED volumes..."
$SUDO docker system prune -a --volumes -f || true
echo "==> Pruning builder cache..."
$SUDO docker builder prune -a -f || true
echo "==> Pruning dangling volumes and networks (extra pass)..."
$SUDO docker volume prune -f || true
$SUDO docker network prune -f || true

if [[ "$DEEP" -eq 1 ]]; then
  echo "==> DEEP CLEAN: This will STOP Docker and WIPE /var/lib/docker and /var/lib/containerd"
  read -r -p "Proceed? [y/N]: " ans || true
  case "${ans:-}" in
    y|Y|yes|YES)
      $SUDO systemctl stop docker || true
      $SUDO systemctl stop containerd || true
      $SUDO rm -rf /var/lib/docker || true
      $SUDO rm -rf /var/lib/containerd || true
      $SUDO systemctl start containerd || true
      $SUDO systemctl start docker || true
      ;;
    *)
      echo "Skipped deep clean."
      ;;
  esac
fi

echo "==> Disk usage after cleanup:"
$SUDO docker system df || true
echo "==> Done."


