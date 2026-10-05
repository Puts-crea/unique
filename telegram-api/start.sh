#!/bin/sh

set -e

echo "========================================"
echo "Starting Telegram Bot API + File Server"
echo "========================================"

echo "Starting internal file server on port 8090..."

python3 -m http.server 8090 \
  --directory /var/lib/telegram-bot-api \
  --bind :: &

echo "File server started."

echo "Starting Telegram Bot API..."

exec /docker-entrypoint.sh
