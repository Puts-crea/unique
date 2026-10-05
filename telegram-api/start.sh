#!/bin/sh

set -e

echo "========================================"
echo "Starting Telegram Bot API + File Server"
echo "========================================"

echo "Starting internal file server on port 8090..."

busybox httpd \
    -f \
    -p 8090 \
    -h /var/lib/telegram-bot-api &

echo "File server started."

echo "Starting Telegram Bot API..."

exec /docker-entrypoint.sh
