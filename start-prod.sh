#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
ENV_FILE="$ROOT_DIR/.env"
VENV_DIR="$ROOT_DIR/.venv"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

BACKEND_HOST="${BACKEND_HOST:-0.0.0.0}"
BACKEND_PORT="${BACKEND_PORT:-8010}"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${FRONTEND_PORT:-3010}"

if [[ -z "${NEXT_PUBLIC_BACKEND_URL:-}" ]]; then
  export NEXT_PUBLIC_BACKEND_URL="http://localhost:${BACKEND_PORT}"
fi

echo "==> Using backend:  ${BACKEND_HOST}:${BACKEND_PORT}"
echo "==> Using frontend: ${FRONTEND_HOST}:${FRONTEND_PORT}"
echo "==> NEXT_PUBLIC_BACKEND_URL=${NEXT_PUBLIC_BACKEND_URL}"

command -v python3 >/dev/null 2>&1 || { echo "python3 is required"; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "npm is required"; exit 1; }

if [[ ! -d "$BACKEND_DIR" || ! -d "$FRONTEND_DIR" ]]; then
  echo "Run this script from repository root: $ROOT_DIR"
  exit 1
fi

is_port_busy() {
  local port="$1"
  ss -ltn | awk -v p=":${port}" '$4 ~ p {found=1} END {exit(found ? 0 : 1)}'
}

if is_port_busy "$BACKEND_PORT"; then
  echo "Backend port ${BACKEND_PORT} is already in use."
  echo "Stop the existing process or run: BACKEND_PORT=<free_port> ./start-prod.sh"
  exit 1
fi

if is_port_busy "$FRONTEND_PORT"; then
  echo "Frontend port ${FRONTEND_PORT} is already in use."
  echo "Stop the existing process or run: FRONTEND_PORT=<free_port> ./start-prod.sh"
  exit 1
fi

echo "==> Preparing Python virtual environment"
if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "==> Installing backend dependencies"
python -m pip install --upgrade pip
if [[ -f "$BACKEND_DIR/requirements.txt" ]]; then
  python -m pip install -r "$BACKEND_DIR/requirements.txt"
fi
if [[ -f "$ROOT_DIR/requirements.txt" ]]; then
  python -m pip install -r "$ROOT_DIR/requirements.txt"
fi

echo "==> Installing frontend dependencies"
if [[ -f "$FRONTEND_DIR/package-lock.json" ]]; then
  npm --prefix "$FRONTEND_DIR" ci --include=dev
else
  npm --prefix "$FRONTEND_DIR" install --include=dev
fi

echo "==> Building frontend for production"
npm --prefix "$FRONTEND_DIR" run build

BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
  echo
  echo "==> Shutting down services"
  if [[ -n "$BACKEND_PID" ]] && kill -0 "$BACKEND_PID" 2>/dev/null; then
    kill "$BACKEND_PID" 2>/dev/null || true
    wait "$BACKEND_PID" 2>/dev/null || true
  fi
  if [[ -n "$FRONTEND_PID" ]] && kill -0 "$FRONTEND_PID" 2>/dev/null; then
    kill "$FRONTEND_PID" 2>/dev/null || true
    wait "$FRONTEND_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "==> Starting backend (uvicorn, production mode)"
(
  cd "$BACKEND_DIR"
  exec "$VENV_DIR/bin/python" -m uvicorn main:app --host "$BACKEND_HOST" --port "$BACKEND_PORT"
) &
BACKEND_PID=$!

echo "==> Starting frontend (next start, production mode)"
(
  cd "$FRONTEND_DIR"
  exec npm run start -- --hostname "$FRONTEND_HOST" --port "$FRONTEND_PORT"
) &
FRONTEND_PID=$!

echo "==> Backend PID:  $BACKEND_PID"
echo "==> Frontend PID: $FRONTEND_PID"
echo "==> Services are running. Press Ctrl+C to stop."

wait -n "$BACKEND_PID" "$FRONTEND_PID"
