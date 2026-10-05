#!/bin/sh

set -e

echo "========================================"
echo "Starting Telegram Bot API + File Server"
echo "========================================"

# =========================================================
# FILE SERVER
# =========================================================

echo "Starting internal file server on port 8090..."

python3 -m http.server 8090 \
  --directory /var/lib/telegram-bot-api \
  --bind :: &

echo "File server started."


# =========================================================
# AUTO CLEANUP
# Видаляємо медіафайли старші за 6 годин
# Перевірка виконується раз на годину
# =========================================================

cleanup_old_files() {

    echo "Running Telegram media cleanup..."

    find /var/lib/telegram-bot-api \
        -type f \
        \( \
            -path "*/videos/*" \
            -o -path "*/documents/*" \
            -o -path "*/photos/*" \
            -o -path "*/audio/*" \
            -o -path "*/voice/*" \
            -o -path "*/animations/*" \
            -o -path "*/video_notes/*" \
            -o -path "*/stickers/*" \
        \) \
        -mmin +360 \
        -print \
        -delete 2>/dev/null || true

    echo "Cleanup finished."
}


cleanup_loop() {

    while true
    do
        # Перевіряємо файли раз на годину
        sleep 3600

        cleanup_old_files
    done
}


echo "Starting automatic cleanup..."
echo "Files older than 6 hours will be removed."

cleanup_loop &


# =========================================================
# TELEGRAM BOT API
# =========================================================

echo "Starting Telegram Bot API..."

exec /docker-entrypoint.sh
