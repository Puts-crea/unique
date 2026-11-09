import asyncio
import json
import os
import random
import sqlite3
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from collections import deque
from pathlib import Path
from urllib.parse import quote

import aiohttp

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)

from PIL import Image, ImageOps


# =========================================================
# НАЛАШТУВАННЯ
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

TELEGRAM_API_URL = os.getenv(
    "TELEGRAM_API_URL",
    "https://api.telegram.org",
).rstrip("/")

FILE_SERVER_URL = os.getenv(
    "FILE_SERVER_URL",
    "http://telegram-bot-api.railway.internal:8090",
).rstrip("/")

MAX_VIDEO_MB = 100
MAX_IMAGE_MB = 50
TMP_DIR = tempfile.gettempdir()
STATS_DB_PATH = os.getenv("STATS_DB_PATH", "/data/bot_stats.sqlite3")



# =========================================================
# SQLITE STATISTICS
# =========================================================

def stats_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stats_connect():
    connection = sqlite3.connect(
        STATS_DB_PATH,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def stats_init():
    """
    Ініціалізує базу статистики.
    За замовчуванням файл лежить у /data — Railway Volume
    потрібно змонтувати саме в цю папку.
    """

    global STATS_DB_PATH

    def initialize(path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(path, timeout=30) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA busy_timeout=30000")

            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    finished_at TEXT,
                    mode TEXT,
                    extension_seconds INTEGER,
                    total_files INTEGER NOT NULL DEFAULT 0,
                    processed INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'collecting'
                );

                CREATE TABLE IF NOT EXISTS operations (
                    event_key TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    file_size_bytes INTEGER,
                    video_duration_seconds REAL,
                    mode TEXT,
                    extension_seconds INTEGER,
                    processing_seconds REAL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error_stage TEXT,
                    error_type TEXT,
                    batch_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_operations_created
                    ON operations(created_at);
                CREATE INDEX IF NOT EXISTS idx_operations_user
                    ON operations(user_id);
                CREATE INDEX IF NOT EXISTS idx_operations_batch
                    ON operations(batch_id);
                CREATE INDEX IF NOT EXISTS idx_batches_created
                    ON batches(created_at);
                CREATE INDEX IF NOT EXISTS idx_users_last_seen
                    ON users(last_seen);
                """
            )

            stale_before = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat(timespec="seconds")

            connection.execute(
                """
                UPDATE operations
                SET status='failed',
                    error_stage=COALESCE(error_stage, 'processing'),
                    error_type=COALESCE(error_type, 'InterruptedAfterRestart')
                WHERE status='pending' AND created_at < ?
                """,
                (stale_before,),
            )

            connection.execute(
                """
                UPDATE batches
                SET status='interrupted',
                    finished_at=COALESCE(finished_at, ?)
                WHERE status!='finished' AND created_at < ?
                """,
                (
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    stale_before,
                ),
            )

    try:
        initialize(STATS_DB_PATH)
    except Exception as error:
        # Не зупиняємо бота, якщо Volume ще не підключений.
        fallback_path = "/tmp/bot_stats.sqlite3"
        print(
            f"WARNING: stats DB unavailable at {STATS_DB_PATH}: {error}. "
            f"Using temporary fallback {fallback_path}"
        )
        STATS_DB_PATH = fallback_path
        initialize(STATS_DB_PATH)

    print(f"Statistics database: {STATS_DB_PATH}")


def stats_touch_user(user_id: int):
    now = stats_now()

    try:
        with stats_connect() as connection:
            connection.execute(
                """
                INSERT INTO users(user_id, first_seen, last_seen)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id)
                DO UPDATE SET last_seen=excluded.last_seen
                """,
                (user_id, now, now),
            )
    except Exception as error:
        print(f"Stats user update error: {error}")


def stats_start_operation(
    event_key: str,
    user_id: int,
    media_type: str,
    file_size_bytes: int | None = None,
):
    now = stats_now()
    stats_touch_user(user_id)

    try:
        with stats_connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO operations(
                    event_key, user_id, created_at, media_type,
                    file_size_bytes, status
                )
                VALUES (?, ?, ?, ?, ?, 'pending')
                """,
                (
                    event_key,
                    user_id,
                    now,
                    media_type,
                    file_size_bytes,
                ),
            )
    except Exception as error:
        print(f"Stats operation start error: {error}")


def stats_update_operation(event_key: str, **values):
    allowed_fields = {
        "file_size_bytes",
        "video_duration_seconds",
        "mode",
        "extension_seconds",
        "processing_seconds",
        "status",
        "error_stage",
        "error_type",
        "batch_id",
    }

    updates = {
        key: value
        for key, value in values.items()
        if key in allowed_fields and value is not None
    }

    if not updates:
        return

    assignments = ", ".join(
        f"{field}=?" for field in updates
    )
    parameters = list(updates.values())
    parameters.append(event_key)

    try:
        with stats_connect() as connection:
            connection.execute(
                f"UPDATE operations SET {assignments} WHERE event_key=?",
                parameters,
            )
    except Exception as error:
        print(f"Stats operation update error: {error}")


def stats_create_batch(batch_id: str, user_id: int):
    try:
        with stats_connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO batches(
                    batch_id, user_id, created_at, status
                )
                VALUES (?, ?, ?, 'collecting')
                """,
                (batch_id, user_id, stats_now()),
            )
    except Exception as error:
        print(f"Stats batch create error: {error}")


def stats_update_batch(
    batch_id: str,
    total_files: int | None = None,
    mode: str | None = None,
    extension_seconds: int | None = None,
):
    fields = {}
    if total_files is not None:
        fields["total_files"] = total_files
    if mode is not None:
        fields["mode"] = mode
    if extension_seconds is not None:
        fields["extension_seconds"] = extension_seconds

    if not fields:
        return

    assignments = ", ".join(f"{key}=?" for key in fields)
    values = list(fields.values()) + [batch_id]

    try:
        with stats_connect() as connection:
            connection.execute(
                f"UPDATE batches SET {assignments} WHERE batch_id=?",
                values,
            )
    except Exception as error:
        print(f"Stats batch update error: {error}")


def stats_finish_batch(
    batch_id: str,
    processed: int,
    failed: int,
    mode: str,
    extension_seconds: int,
):
    try:
        with stats_connect() as connection:
            connection.execute(
                """
                UPDATE batches
                SET finished_at=?,
                    processed=?,
                    failed=?,
                    mode=?,
                    extension_seconds=?,
                    status='finished'
                WHERE batch_id=?
                """,
                (
                    stats_now(),
                    processed,
                    failed,
                    mode,
                    extension_seconds,
                    batch_id,
                ),
            )
    except Exception as error:
        print(f"Stats batch finish error: {error}")


def stats_format_duration(seconds) -> str:
    if seconds is None:
        return "—"

    total = max(0, int(seconds))
    minutes, remainder = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours:02d}:{minutes:02d}:{remainder:02d}"
    return f"{minutes:02d}:{remainder:02d}"


def stats_period_data(period: str):
    now = datetime.now(timezone.utc)

    if period == "7d":
        since = (now - timedelta(days=7)).isoformat(timespec="seconds")
        label = "Останні 7 днів"
    elif period == "30d":
        since = (now - timedelta(days=30)).isoformat(timespec="seconds")
        label = "Останні 30 днів"
    else:
        since = None
        label = "Весь час"

    where = "WHERE created_at >= ?" if since else ""
    params = (since,) if since else ()

    try:
        with stats_connect() as connection:
            totals = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS received,
                    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                    AVG(file_size_bytes) AS avg_size,
                    AVG(
                        CASE WHEN media_type='video'
                        THEN video_duration_seconds END
                    ) AS avg_video_duration,
                    AVG(
                        CASE WHEN status='success'
                        THEN processing_seconds END
                    ) AS avg_processing,
                    MAX(
                        CASE WHEN status='success'
                        THEN processing_seconds END
                    ) AS max_processing
                FROM operations
                {where}
                """,
                params,
            ).fetchone()

            media_rows = connection.execute(
                f"""
                SELECT media_type, COUNT(*) AS amount
                FROM operations
                {where}
                GROUP BY media_type
                """,
                params,
            ).fetchall()

            mode_rows = connection.execute(
                f"""
                SELECT mode, COUNT(*) AS uses
                FROM operations
                {where + (' AND' if where else 'WHERE')} mode IS NOT NULL
                GROUP BY mode
                ORDER BY uses DESC
                """,
                params,
            ).fetchall()

            # Рахуємо саме вибори подовження: один раз на пачку,
            # а не по одному разу на кожне відео в пачці.
            extension_where = (
                "WHERE created_at >= ? AND extension_seconds IS NOT NULL"
                if since
                else
                "WHERE extension_seconds IS NOT NULL"
            )
            extension_rows = connection.execute(
                f"""
                SELECT extension_seconds, COUNT(*) AS uses
                FROM batches
                {extension_where}
                GROUP BY extension_seconds
                """,
                params,
            ).fetchall()

            batch_where = "WHERE created_at >= ?" if since else ""
            batch_data = connection.execute(
                f"""
                SELECT COUNT(*) AS batches, AVG(total_files) AS avg_batch_size
                FROM batches
                {batch_where}
                """,
                params,
            ).fetchone()

            if since:
                user_data = connection.execute(
                    """
                    SELECT
                        SUM(CASE WHEN last_seen >= ? THEN 1 ELSE 0 END) AS active,
                        SUM(
                            CASE WHEN first_seen >= ? AND last_seen >= ?
                            THEN 1 ELSE 0 END
                        ) AS new_users,
                        SUM(
                            CASE WHEN first_seen < ? AND last_seen >= ?
                            THEN 1 ELSE 0 END
                        ) AS returning_users
                    FROM users
                    """,
                    (since, since, since, since, since),
                ).fetchone()
                active_users = int(user_data["active"] or 0)
                new_users = int(user_data["new_users"] or 0)
                returning_users = int(user_data["returning_users"] or 0)
            else:
                active_users = connection.execute(
                    "SELECT COUNT(*) FROM users"
                ).fetchone()[0]

                new_users = active_users

                returning_users = connection.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT user_id
                        FROM operations
                        GROUP BY user_id
                        HAVING COUNT(*) > 1
                    )
                    """
                ).fetchone()[0]

            error_where = (
                "WHERE created_at >= ? AND status='failed'"
                if since
                else "WHERE status='failed'"
            )
            error_rows = connection.execute(
                f"""
                SELECT COALESCE(error_stage, 'інше') AS stage,
                       COUNT(*) AS amount
                FROM operations
                {error_where}
                GROUP BY error_stage
                ORDER BY amount DESC
                """,
                params,
            ).fetchall()

            error_mode_rows = connection.execute(
                f"""
                SELECT COALESCE(mode, 'режим не вибрано') AS mode,
                       COUNT(*) AS amount
                FROM operations
                {error_where}
                GROUP BY mode
                ORDER BY amount DESC
                """,
                params,
            ).fetchall()

            error_type_rows = connection.execute(
                f"""
                SELECT COALESCE(error_type, 'невідомо') AS error_type,
                       COUNT(*) AS amount
                FROM operations
                {error_where}
                GROUP BY error_type
                ORDER BY amount DESC
                LIMIT 5
                """,
                params,
            ).fetchall()

        received = int(totals["received"] or 0)
        success = int(totals["success"] or 0)
        failed = int(totals["failed"] or 0)
        pending = int(totals["pending"] or 0)
        completed = success + failed
        success_rate = (success / completed * 100) if completed else 0

        mode_names = {
            "metadata": "⚡ Швидкий режим",
            "noise": "🎞 Легка зміна відео",
            "preview": "🖼 Метадані + прев’ю",
            "image_metadata": "📝 Фото: метадані",
            "image_pixels": "🖼 Фото: зміна пікселів",
        }

        mode_total = sum(int(row["uses"] or 0) for row in mode_rows)
        mode_lines = []
        for row in mode_rows:
            name = mode_names.get(row["mode"], row["mode"] or "Не вибрано")
            uses = int(row["uses"] or 0)
            percent = uses / mode_total * 100 if mode_total else 0
            mode_lines.append(
                f"• {name}: <b>{uses}</b> ({percent:.1f}%)"
            )

        extension_counts = {0: 0, 30: 0, 60: 0}
        for row in extension_rows:
            extension_counts[int(row["extension_seconds"] or 0)] = int(
                row["uses"] or 0
            )

        extension_total = sum(extension_counts.values())
        extension_lines = []
        for seconds, name in (
            (0, "Без подовження"),
            (30, "+30 секунд"),
            (60, "+60 секунд"),
        ):
            count = extension_counts[seconds]
            percent = count / extension_total * 100 if extension_total else 0
            extension_lines.append(
                f"• {name}: <b>{count}</b> ({percent:.1f}%)"
            )

        error_names = {
            "download": "Завантаження",
            "validation": "Перевірка файлу",
            "processing": "Обробка",
            "upload": "Відправлення результату",
            "інше": "Інше",
        }
        error_lines = [
            f"• {error_names.get(row['stage'], row['stage'])}: "
            f"<b>{int(row['amount'] or 0)}</b>"
            for row in error_rows
        ]

        error_mode_lines = [
            f"• {mode_names.get(row['mode'], row['mode'])}: "
            f"<b>{int(row['amount'] or 0)}</b>"
            for row in error_mode_rows
        ]

        error_type_lines = [
            f"• <code>{row['error_type']}</code>: "
            f"<b>{int(row['amount'] or 0)}</b>"
            for row in error_type_rows
        ]

        media_counts = {
            row["media_type"]: int(row["amount"] or 0)
            for row in media_rows
        }

        avg_size_mb = (
            float(totals["avg_size"]) / 1024 / 1024
            if totals["avg_size"] is not None else None
        )

        avg_duration = totals["avg_video_duration"]
        avg_processing = totals["avg_processing"]
        max_processing = totals["max_processing"]

        batch_count = int(batch_data["batches"] or 0)
        avg_batch_size = batch_data["avg_batch_size"]

        user_lines = (
            [
                f"👥 Активних користувачів: <b>{active_users}</b>",
                f"🆕 Нових за період: <b>{new_users}</b>",
                f"🔁 Повернулися: <b>{returning_users}</b>",
            ]
            if since
            else [
                f"👥 Користувачів за весь час: <b>{active_users}</b>",
                f"🔁 Повторно користувалися: <b>{returning_users}</b>",
            ]
        )

        lines = [
            f"📊 <b>Статистика — {label}</b>",
            "",
            f"📥 Отримано файлів: <b>{received}</b>",
            f"🎬 Відео: <b>{media_counts.get('video', 0)}</b>",
            f"🖼 Фото: <b>{media_counts.get('image', 0)}</b>",
            f"✅ Успішно: <b>{success}</b>",
            f"❌ Помилок: <b>{failed}</b>",
            f"⏳ У процесі: <b>{pending}</b>",
            f"🎯 Успішність: <b>{success_rate:.1f}%</b>",
            "",
            f"📦 Пачок: <b>{batch_count}</b>",
            (
                "📚 Середній розмір пачки: "
                f"<b>{float(avg_batch_size):.1f}</b> відео"
                if avg_batch_size is not None
                else "📚 Середній розмір пачки: —"
            ),
            "",
            *user_lines,
            "",
            "<b>Популярність режимів:</b>",
            *(mode_lines or ["• Поки немає даних"]),
            "",
            "<b>Вибір подовження (пачки):</b>",
            *extension_lines,
            "",
            "<b>Продуктивність:</b>",
            (
                f"⏱ Середній час обробки: "
                f"<b>{float(avg_processing):.1f} с</b>"
                if avg_processing is not None
                else "⏱ Середній час обробки: —"
            ),
            (
                f"🚀 Найдовша обробка: "
                f"<b>{float(max_processing):.1f} с</b>"
                if max_processing is not None
                else "🚀 Найдовша обробка: —"
            ),
            (
                f"📦 Середній розмір файлу: "
                f"<b>{avg_size_mb:.2f} МБ</b>"
                if avg_size_mb is not None
                else "📦 Середній розмір файлу: —"
            ),
            (
                f"🎬 Середня тривалість відео: "
                f"<b>{stats_format_duration(avg_duration)}</b>"
            ),
            "",
            "<b>Помилки за етапами:</b>",
            *(error_lines or ["• Помилок немає"]),
            "",
            "<b>Помилки за режимами:</b>",
            *(error_mode_lines or ["• Помилок немає"]),
            "",
            "<b>Типи помилок:</b>",
            *(error_type_lines or ["• Помилок немає"]),
        ]

        return "\n".join(lines)

    except Exception as error:
        print(f"Stats report error: {error}")
        return (
            "❌ Не вдалося прочитати статистику.\n\n"
            f"<code>{type(error).__name__}</code>"
        )


def stats_period_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="7 днів",
                    callback_data="stats|7d",
                ),
                InlineKeyboardButton(
                    text="30 днів",
                    callback_data="stats|30d",
                ),
                InlineKeyboardButton(
                    text="Весь час",
                    callback_data="stats|all",
                ),
            ]
        ]
    )



# =========================================================
# ALLOWED USERS / ADMINS
# =========================================================

def load_id_set(env_name: str) -> set[int]:
    raw = os.getenv(env_name, "").strip()

    if not raw:
        return set()

    result = set()

    for item in raw.split(","):
        item = item.strip()

        if not item:
            continue

        try:
            result.add(int(item))
        except ValueError:
            print(f"WARNING: invalid {env_name} value: {item}")

    return result


ALLOWED_USERS = load_id_set("ALLOWED_USERS")
ADMIN_USERS = load_id_set("ADMIN_USERS")


def is_allowed(user_id: int) -> bool:
    # Порожній ALLOWED_USERS = доступ відкритий усім.
    if not ALLOWED_USERS:
        return True

    return user_id in ALLOWED_USERS


# =========================================================
# TELEGRAM
# =========================================================

if TELEGRAM_API_URL != "https://api.telegram.org":

    telegram_api = TelegramAPIServer.from_base(
        TELEGRAM_API_URL
    )

    session = AiohttpSession(
        api=telegram_api,
        timeout=600,
    )

    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )

else:

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )


dp = Dispatcher()


# =========================================================
# ЧЕРГИ
# =========================================================

# Для кожного користувача своя черга.
# Один користувач може накидати скільки завгодно файлів.
# Вони будуть оброблятися строго по черзі.
user_queues: dict[int, deque] = {}

# Окремі пачки ВІДЕО.
# pending_downloads рахує також ті файли, які ще завантажуються.
video_batches: dict[int, dict] = {}

# Не даємо великій кількості паралельних завантажень перевантажити
# локальний Telegram Bot API / Railway file server.
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(2)
BATCH_IDLE_GRACE_SECONDS = 1.5

# Користувачі, в яких зараз реально працює FFmpeg/Pillow.
processing_users: set[int] = set()


def get_video_batch(user_id: int, chat_id: int | None = None) -> dict:
    batch = video_batches.get(user_id)

    # Після переходу в finalizing нові повідомлення вже належать
    # до наступної пачки, а не до тієї, що завершується.
    if batch is None or batch.get("state") == "finalizing":
        batch = {
            "files": [],
            "batch_id": uuid.uuid4().hex,
            "mode": None,
            "extend_seconds": None,
            "state": "choosing_mode",
            "settings_message_sent": False,
            "settings_message_id": None,
            "settings_lock": asyncio.Lock(),
            "chat_id": chat_id,
            "total_received": 0,
            "downloaded_count": 0,
            "pending_downloads": 0,
            "pending_message_ids": set(),
            "received_message_ids": set(),
            "processed": 0,
            "failed": 0,
            "started": 0,
            "active": False,
            "worker_running": False,
            "wake_event": asyncio.Event(),
        }
        video_batches[user_id] = batch

    if chat_id is not None:
        batch["chat_id"] = chat_id

    return batch


def finish_empty_video_batch(user_id: int, batch: dict):
    """Закриває пачку, якщо всі завантаження завершилися помилкою."""
    if (
        batch.get("pending_downloads", 0) != 0
        or batch.get("files")
        or batch.get("worker_running")
    ):
        return

    batch["state"] = "finalizing"
    stats_finish_batch(
        batch_id=batch["batch_id"],
        processed=batch.get("processed", 0),
        failed=batch.get("failed", 0),
        mode=batch.get("mode") or "not_selected",
        extension_seconds=batch.get("extend_seconds") or 0,
    )

    if video_batches.get(user_id) is batch:
        video_batches.pop(user_id, None)


async def update_video_batch_status(
    batch: dict,
    status,
    message: Message,
):
    """Створює або оновлює єдине повідомлення стану пачки."""

    async with batch["settings_lock"]:
        mode_names = {
            "metadata": "⚡ Швидкий режим",
            "noise": "🎞 Легка зміна відео",
            "preview": "🖼 Метадані + прев’ю",
        }

        pending_count = batch["pending_downloads"]
        waiting_count = len(batch["files"])

        if not batch["settings_message_sent"]:
            prompt_text = (
                "✅ <b>Відео додано в пачку.</b>\n\n"
                f"📦 Всього отримано: <b>{batch['total_received']}</b>\n"
                f"⬇️ Ще завантажується: <b>{pending_count}</b>\n\n"
                "Обери спосіб унікалізації. Налаштування застосуються "
                "до всієї пачки:"
            )

            if status:
                try:
                    prompt_message = await status.edit_text(
                        prompt_text,
                        reply_markup=batch_video_mode_keyboard(),
                    )
                except Exception as error:
                    print(f"Could not edit first batch status: {error}")
                    prompt_message = await message.answer(
                        prompt_text,
                        reply_markup=batch_video_mode_keyboard(),
                    )
            else:
                prompt_message = await message.answer(
                    prompt_text,
                    reply_markup=batch_video_mode_keyboard(),
                )

            batch["settings_message_sent"] = True
            batch["settings_message_id"] = prompt_message.message_id

            return

        if status:
            try:
                await status.delete()
            except Exception:
                pass

        state = batch["state"]
        if state == "choosing_mode":
            batch_text = (
                "✅ <b>Відео додано в пачку.</b>\n\n"
                f"📦 Всього отримано: <b>{batch['total_received']}</b>\n"
                f"⬇️ Ще завантажується: <b>{pending_count}</b>\n\n"
                "Обери спосіб унікалізації для всієї пачки:"
            )
            keyboard = batch_video_mode_keyboard()

        elif state == "choosing_extension":
            batch_text = (
                "✅ <b>Відео додано в пачку.</b>\n\n"
                f"📦 Всього отримано: <b>{batch['total_received']}</b>\n"
                f"⬇️ Ще завантажується: <b>{pending_count}</b>\n"
                f"Режим: <b>{mode_names[batch['mode']]}</b>\n\n"
                "Обери подовження для всієї пачки:"
            )
            keyboard = batch_extension_keyboard()

        elif state == "processing":
            extend_seconds = batch.get("extend_seconds") or 0
            extend_text = (
                "без подовження"
                if extend_seconds == 0
                else f"+{extend_seconds} сек"
            )
            batch_text = (
                "⚙️ <b>Пачка обробляється...</b>\n\n"
                f"📚 Всього отримано: <b>{batch['total_received']}</b>\n"
                f"⚙️ Готових у черзі: <b>{waiting_count}</b>\n"
                f"⬇️ Ще завантажується: <b>{pending_count}</b>\n"
                f"✅ Готово: <b>{batch['processed']}</b>\n"
                f"⏱ Подовження: <b>{extend_text}</b>"
            )
            keyboard = None
        else:
            return

        if batch.get("settings_message_id"):
            try:
                await bot.edit_message_text(
                    chat_id=batch["chat_id"],
                    message_id=batch["settings_message_id"],
                    text=batch_text,
                    reply_markup=keyboard,
                )
            except Exception as error:
                print(f"Batch status update error: {error}")


def get_user_queue(user_id: int) -> deque:
    if user_id not in user_queues:
        user_queues[user_id] = deque()

    return user_queues[user_id]


def queue_position(user_id: int, item_id: str) -> int | None:
    queue = get_user_queue(user_id)

    for index, item in enumerate(queue, start=1):
        if item["id"] == item_id:
            return index

    return None


def queue_count(user_id: int) -> int:
    return len(get_user_queue(user_id))


# =========================================================
# ДОПОМІЖНІ ФУНКЦІЇ
# =========================================================

def remove_file(path: str | None):
    if not path:
        return

    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def get_extension(
    filename: str | None,
    default: str,
) -> str:

    if not filename:
        return default

    suffix = (
        Path(filename)
        .suffix
        .lower()
        .replace(".", "")
    )

    return suffix or default


async def access_denied(message: Message):
    await message.answer(
        "⛔ <b>Доступ до бота обмежений.</b>\n\n"
        "Надішли адміністратору свій Telegram ID:\n"
        f"<code>{message.from_user.id}</code>"
    )


# =========================================================
# DOWNLOAD ЧЕРЕЗ LOCAL TELEGRAM BOT API
# =========================================================

def is_retryable_download_error(error: Exception) -> bool:
    text = str(error).lower()

    retry_phrases = (
        "wrong file_id",
        "temporarily unavailable",
        "temporary failure",
        "too many requests",
        "retry after",
        "internal server error",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "timed out",
        "timeout",
        "connection reset",
        "connection aborted",
        "broken pipe",
        "incomplete download",
        "empty file",
        "downloaded file is missing",
        "http 404",
        "http 408",
        "http 429",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
    )

    return (
        isinstance(
            error,
            (aiohttp.ClientError, asyncio.TimeoutError, OSError),
        )
        or any(phrase in text for phrase in retry_phrases)
    )


async def download_telegram_file(
    file_obj,
    destination: str,
):
    """Завантажує файл із повторними спробами при тимчасових помилках."""

    expected_size = int(getattr(file_obj, "file_size", 0) or 0)
    max_attempts = 3
    last_error = None

    for attempt in range(1, max_attempts + 1):
        remove_file(destination)

        try:
            async with DOWNLOAD_SEMAPHORE:
                telegram_file = await bot.get_file(file_obj.file_id)
                file_path = telegram_file.file_path

                if not file_path:
                    raise RuntimeError(
                        "Telegram не повернув file_path."
                    )

                print(
                    f"Telegram file_path (attempt {attempt}/"
                    f"{max_attempts}): {file_path}"
                )

                if os.path.isabs(file_path):
                    root = "/var/lib/telegram-bot-api/"

                    if not file_path.startswith(root):
                        raise RuntimeError(
                            "Отримано невідомий абсолютний шлях: "
                            f"{file_path}"
                        )

                    relative_path = file_path[len(root):]
                    safe_path = quote(relative_path, safe="/")
                    download_url = f"{FILE_SERVER_URL}/{safe_path}"

                    timeout = aiohttp.ClientTimeout(
                        total=900,
                        connect=60,
                    )

                    async with aiohttp.ClientSession(
                        timeout=timeout
                    ) as client:
                        async with client.get(download_url) as response:
                            if response.status != 200:
                                response_text = await response.text()
                                raise RuntimeError(
                                    "File server error: "
                                    f"HTTP {response.status}: "
                                    f"{response_text[:300]}"
                                )

                            with open(destination, "wb") as output:
                                async for chunk in response.content.iter_chunked(
                                    1024 * 1024
                                ):
                                    output.write(chunk)
                else:
                    await bot.download_file(
                        file_path,
                        destination=destination,
                    )

            if not os.path.exists(destination):
                raise RuntimeError(
                    "Downloaded file is missing after transfer."
                )

            actual_size = os.path.getsize(destination)
            if actual_size <= 0:
                raise RuntimeError("Incomplete download: empty file.")

            if expected_size and actual_size != expected_size:
                raise RuntimeError(
                    "Incomplete download: expected "
                    f"{expected_size} bytes, received {actual_size} bytes."
                )

            return

        except Exception as error:
            last_error = error
            remove_file(destination)

            if (
                attempt >= max_attempts
                or not is_retryable_download_error(error)
            ):
                raise

            delay = float(attempt)
            print(
                f"Telegram download attempt {attempt} failed: {error}. "
                f"Retrying in {delay:.0f}s."
            )
            await asyncio.sleep(delay)

    if last_error:
        raise last_error


# =========================================================
# FFMPEG
# =========================================================

async def run_ffmpeg(
    args: list[str]
) -> tuple[bool, str]:

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        _stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            text = stderr.decode(
                errors="ignore"
            )

            return False, text[-2500:]

        return True, "ok"

    except FileNotFoundError:
        return False, "FFmpeg не встановлено."

    except Exception as e:
        return False, str(e)


async def get_video_duration(video_path: str) -> float:
    """
    Повертає тривалість відео в секундах через ffprobe.
    Потрібно, щоб blur + countdown починалися рівно
    після завершення оригінального відео.
    """

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, _stderr = await proc.communicate()

        if proc.returncode != 0:
            return 0.0

        value = stdout.decode(errors="ignore").strip()

        duration = float(value)

        if duration > 0:
            return duration

    except Exception:
        pass

    return 0.0


# =========================================================
# IMAGE METADATA
# =========================================================

def image_metadata_mode(
    input_path: str,
    output_path: str,
) -> tuple[bool, str]:

    try:
        with Image.open(input_path) as source:

            img = ImageOps.exif_transpose(
                source
            )

            has_alpha = (
                img.mode in ("RGBA", "LA")
                or "transparency" in img.info
            )

            if has_alpha:
                img = img.convert("RGBA")

                img.save(
                    output_path,
                    format="PNG",
                    optimize=True,
                )

            else:
                img = img.convert("RGB")

                img.save(
                    output_path,
                    format="JPEG",
                    quality=96,
                    optimize=True,
                    progressive=True,
                )

        return True, "ok"

    except Exception as e:
        return False, str(e)


# =========================================================
# IMAGE PIXELS
# =========================================================

def image_pixel_mode(
    input_path: str,
    output_path: str,
) -> tuple[bool, str]:

    try:
        with Image.open(input_path) as source:

            img = ImageOps.exif_transpose(
                source
            )

            has_alpha = (
                img.mode in ("RGBA", "LA")
                or "transparency" in img.info
            )

            if has_alpha:
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")

            pixels = img.load()

            width, height = img.size
            total_pixels = width * height

            changes = max(
                500,
                min(
                    25000,
                    total_pixels // 400,
                ),
            )

            for _ in range(changes):

                x = random.randrange(width)
                y = random.randrange(height)

                pixel = pixels[x, y]

                if img.mode == "RGBA":

                    r, g, b, a = pixel
                    rgb = [r, g, b]
                    channel = random.randrange(3)

                    rgb[channel] = max(
                        0,
                        min(
                            255,
                            rgb[channel]
                            + random.choice((-1, 1)),
                        ),
                    )

                    pixels[x, y] = (
                        rgb[0],
                        rgb[1],
                        rgb[2],
                        a,
                    )

                else:

                    r, g, b = pixel
                    rgb = [r, g, b]
                    channel = random.randrange(3)

                    rgb[channel] = max(
                        0,
                        min(
                            255,
                            rgb[channel]
                            + random.choice((-1, 1)),
                        ),
                    )

                    pixels[x, y] = tuple(rgb)

            if has_alpha:

                img.save(
                    output_path,
                    format="PNG",
                    optimize=True,
                )

            else:

                img.save(
                    output_path,
                    format="JPEG",
                    quality=96,
                    optimize=True,
                    progressive=True,
                )

        return True, "ok"

    except Exception as e:
        return False, str(e)


# =========================================================
# INLINE KEYBOARDS
# =========================================================

def video_mode_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚡ Швидкий режим",
                    callback_data=f"vm|{item_id}|metadata",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎞 Легка зміна відео",
                    callback_data=f"vm|{item_id}|noise",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🖼 Метадані + прев’ю",
                    callback_data=f"vm|{item_id}|preview",
                )
            ],
        ]
    )


def extension_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Без подовження",
                    callback_data=f"ve|{item_id}|0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="+30 секунд",
                    callback_data=f"ve|{item_id}|30",
                ),
                InlineKeyboardButton(
                    text="+60 секунд",
                    callback_data=f"ve|{item_id}|60",
                ),
            ],
        ]
    )


def batch_video_mode_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚡ Швидкий режим",
                    callback_data="batch_vm|metadata",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎞 Легка зміна відео",
                    callback_data="batch_vm|noise",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🖼 Метадані + прев’ю",
                    callback_data="batch_vm|preview",
                )
            ],
        ]
    )


def batch_extension_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Без подовження",
                    callback_data="batch_ve|0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="+30 секунд",
                    callback_data="batch_ve|30",
                ),
                InlineKeyboardButton(
                    text="+60 секунд",
                    callback_data="batch_ve|60",
                ),
            ],
        ]
    )


def image_mode_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📝 Очистити метадані",
                    callback_data=f"im|{item_id}|metadata",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🖼 Змінити пікселі",
                    callback_data=f"im|{item_id}|pixels",
                )
            ],
        ]
    )


# =========================================================
# РОБОТА З ЧЕРГОЮ
# =========================================================

def active_item(user_id: int):
    queue = get_user_queue(user_id)
    return queue[0] if queue else None


def item_is_active(user_id: int, item_id: str) -> bool:
    item = active_item(user_id)
    return bool(item and item["id"] == item_id)


async def show_next_item(user_id: int):
    """Показує кнопки для першого файлу в черзі."""

    if user_id in processing_users:
        return

    queue = get_user_queue(user_id)

    if not queue:
        return

    item = queue[0]

    # Якщо вже чекаємо вибір користувача — повторно не спамимо.
    if item.get("status") not in {"waiting", "ready"}:
        return

    item["status"] = "choosing_mode"

    total = len(queue)

    if item["type"] == "video":
        await bot.send_message(
            chat_id=item["chat_id"],
            text=(
                "🎬 <b>Наступне відео готове до обробки.</b>\n\n"
                f"📦 Файлів у черзі: <b>{total}</b>\n"
                "Обери спосіб унікалізації:"
            ),
            reply_markup=video_mode_keyboard(item["id"]),
        )

    else:
        await bot.send_message(
            chat_id=item["chat_id"],
            text=(
                "🖼 <b>Наступне зображення готове до обробки.</b>\n\n"
                f"📦 Файлів у черзі: <b>{total}</b>\n"
                "Обери спосіб обробки:"
            ),
            reply_markup=image_mode_keyboard(item["id"]),
        )


async def finish_current_item(
    user_id: int,
    remove_input: bool = True,
):
    queue = get_user_queue(user_id)

    if not queue:
        processing_users.discard(user_id)
        return

    item = queue.popleft()

    if remove_input:
        remove_file(item.get("path"))

    processing_users.discard(user_id)

    if not queue:
        user_queues.pop(user_id, None)
        return

    # Наступний файл запускаємо автоматично.
    await show_next_item(user_id)


# =========================================================
# VIDEO PROCESSING BUILDERS
# =========================================================

def common_reencode_filter(extra_filters: list[str]) -> str:
    filters = list(extra_filters)

    # На всякий випадок робимо парні розміри для H.264/yuv420p.
    filters.append("scale=trunc(iw/2)*2:trunc(ih/2)*2")

    return ",".join(filters)


async def process_video_file_legacy(
    input_path: str,
    output_path: str,
    mode: str,
    extend_seconds: int,
) -> tuple[bool, str]:

    unique = uuid.uuid4().hex

    # -----------------------------------------------------
    # ШВИДКИЙ РЕЖИМ БЕЗ ПОДОВЖЕННЯ
    # Тут можна залишити stream copy.
    # -----------------------------------------------------
    if mode == "metadata" and extend_seconds == 0:

        args = [
            "-i",
            input_path,

            "-map",
            "0:v:0",

            "-map",
            "0:a?",

            "-c",
            "copy",

            "-metadata",
            f"title=Unique_{unique}",

            "-metadata",
            f"comment=ID_{unique}",

            "-metadata",
            f"description=Processed_{unique}",

            "-movflags",
            "+faststart",

            output_path,
        ]

        return await run_ffmpeg(args)

    # -----------------------------------------------------
    # ВСІ ІНШІ ВАРІАНТИ ПОТРЕБУЮТЬ ВІДЕО-ФІЛЬТРА
    # -----------------------------------------------------
    filters: list[str] = []

    if mode == "noise":
        filters.append("noise=alls=2:allf=t+u")

    elif mode == "preview":
        # Додаємо рівно 1 копію першого кадру перед відео.
        # Розмивається лише цей новий перший кадр.
        filters.extend([
            "tpad=start=1:start_mode=clone",
            (
                "boxblur="
                "luma_radius='min(100,min(w,h)/2-1)':"
                "luma_power=1:"
                "chroma_radius='min(100,min(cw,ch)/2-1)':"
                "chroma_power=1:"
                "enable='eq(n,0)'"
            ),
            "gblur=sigma=100:enable='eq(n,0)'",
        ])

    elif mode != "metadata":
        return False, "Невідомий режим відео."

    if extend_seconds > 0:

        original_duration = await get_video_duration(
            input_path
        )

        if original_duration <= 0:
            return False, (
                "Не вдалося визначити тривалість відео "
                "для countdown."
            )

        # Невеликий запас проти похибки контейнера/PTS.
        countdown_start = max(
            0.0,
            original_duration - 0.05
        )

        # Останній кадр заморожуємо на +30/+60 секунд.
        filters.append(
            f"tpad=stop_mode=clone:stop_duration={extend_seconds}"
        )

        # Розмиваємо ТІЛЬКИ додану частину.
        filters.append(
            (
                "boxblur="
                "luma_radius='min(100,min(w,h)/2-1)':"
                "luma_power=1:"
                "chroma_radius='min(100,min(cw,ch)/2-1)':"
                "chroma_power=1:"
                f"enable='gte(t,{countdown_start:.3f})'"
            )
        )

        filters.append(
            (
                "gblur=sigma=100:"
                f"enable='gte(t,{countdown_start:.3f})'"
            )
        )

        # Чорна напівпрозора підкладка по центру.
        # Розмір залежить від висоти кадру, тому працює
        # і для вертикальних, і для горизонтальних відео.
        filters.append(
            (
                "drawbox="
                "x=iw/2-(ih*0.19):"
                "y=ih/2-(ih*0.07):"
                "w=ih*0.38:"
                "h=ih*0.14:"
                "color=black@0.82:"
                "t=fill:"
                f"enable='gte(t,{countdown_start:.3f})'"
            )
        )

        # Countdown у секундах:
        # +30 -> 30, 29, 28 ... 00
        # +60 -> 60, 59, 58 ... 00
        countdown_expr = (
            "%{eif\\:"
            f"max(0\\,{extend_seconds}-floor(t-{countdown_start:.3f}))"
            "\\:d\\:2}"
        )

        filters.append(
            (
                "drawtext="
                f"text='{countdown_expr}':"
                "fontcolor=white:"
                "fontsize=h*0.075:"
                "x=(w-text_w)/2:"
                "y=(h-text_h)/2:"
                "shadowcolor=black@0.40:"
                "shadowx=2:"
                "shadowy=2:"
                f"enable='gte(t,{countdown_start:.3f})'"
            )
        )

    vf = common_reencode_filter(filters)

    args = [
        "-i",
        input_path,

        "-map",
        "0:v:0",

        "-map",
        "0:a?",

        "-vf",
        vf,

        "-c:v",
        "libx264",

        "-preset",
        "veryfast",

        "-crf",
        "23" if mode == "noise" else "18",

        "-pix_fmt",
        "yuv420p",
    ]

    # Зберігаємо поведінку старих режимів:
    # noise перекодовував аудіо в AAC;
    # metadata/preview копіюють аудіо як є.
    if mode == "noise":
        args.extend([
            "-c:a",
            "aac",
            "-b:a",
            "192k",
        ])
    else:
        args.extend([
            "-c:a",
            "copy",
        ])

    args.extend([
        "-metadata",
        f"title=Unique_{unique}",

        "-metadata",
        (
            f"comment=Preview_{unique}"
            if mode == "preview"
            else f"comment=ID_{unique}"
        ),

        "-metadata",
        f"description=Processed_{unique}",

        "-movflags",
        "+faststart",

        output_path,
    ])

    return await run_ffmpeg(args)



async def get_video_stream_info(video_path: str) -> dict | None:
    """Отримує параметри відеопотоку, потрібні для безпечного append без re-encode."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries",
            (
                "stream=codec_name,width,height,pix_fmt,profile,level,"
                "r_frame_rate,avg_frame_rate,time_base,sample_aspect_ratio,"
                "field_order,color_range,color_space,color_transfer,color_primaries"
            ),
            "-of", "json",
            video_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await proc.communicate()
        if proc.returncode != 0:
            return None

        payload = json.loads(stdout.decode(errors="ignore") or "{}")
        streams = payload.get("streams") or []
        return streams[0] if streams else None

    except Exception as e:
        print(f"ffprobe stream info error: {e}")
        return None


def fast_append_profile(stream: dict) -> dict | None:
    """
    Повертає параметри для сумісного H.264 tail-фрагмента.
    Для нестандартних форматів використовується старий fallback.
    """
    if not stream:
        return None

    if stream.get("codec_name") != "h264":
        return None

    if stream.get("pix_fmt") != "yuv420p":
        return None

    if stream.get("field_order") not in (None, "unknown", "progressive"):
        return None

    width = int(stream.get("width") or 0)
    height = int(stream.get("height") or 0)
    if width < 2 or height < 2 or width % 2 or height % 2:
        return None

    fps = (
        stream.get("avg_frame_rate")
        or stream.get("r_frame_rate")
        or ""
    )
    if not fps or fps in {"0/0", "N/A"}:
        return None

    try:
        from fractions import Fraction
        fps_value = Fraction(fps)
        if fps_value <= 0 or fps_value > 120:
            return None
    except Exception:
        return None

    profile_raw = (stream.get("profile") or "").lower()
    if "baseline" in profile_raw:
        profile = "baseline"
    elif profile_raw == "main":
        profile = "main"
    elif profile_raw == "high":
        profile = "high"
    else:
        # Не намагаємося вгадувати профіль, якщо він нестандартний.
        return None

    level_value = stream.get("level")
    level = None
    try:
        level_number = int(level_value)
        if level_number > 0:
            level = f"{level_number // 10}.{level_number % 10}"
    except (TypeError, ValueError):
        pass

    time_base = stream.get("time_base") or ""
    track_timescale = None
    try:
        denominator = int(time_base.split("/", 1)[1])
        if 1000 <= denominator <= 1000000:
            track_timescale = denominator
    except (IndexError, TypeError, ValueError):
        pass

    sar = stream.get("sample_aspect_ratio") or "1:1"
    if sar in {"N/A", "0:1", "0/1"}:
        sar = "1:1"
    sar = sar.replace(":", "/")

    return {
        "width": width,
        "height": height,
        "fps": str(fps_value),
        "profile": profile,
        "level": level,
        "time_base_timescale": track_timescale,
        "sar": sar,
        "color_range": stream.get("color_range"),
        "color_space": stream.get("color_space"),
        "color_transfer": stream.get("color_transfer"),
        "color_primaries": stream.get("color_primaries"),
    }


async def append_compressed_tail(
    base_path: str,
    output_path: str,
    extend_seconds: int,
) -> tuple[bool, str]:
    """
    Додає стиснутий заморожений фрагмент до готового відео:
    - оригінальний відеопотік не перекодовується;
    - додаткові 30/60 секунд кодуються окремо з високим CRF;
    - оригінальне аудіо додається назад і не подовжується.
    """
    stream = await get_video_stream_info(base_path)
    profile = fast_append_profile(stream)

    if not profile:
        return False, "Формат відео не підтримує безпечне швидке об'єднання."

    duration = await get_video_duration(base_path)
    if duration <= 0:
        return False, "Не вдалося визначити тривалість відео."

    os.makedirs(TMP_DIR, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory(
            prefix="compressed_tail_",
            dir=TMP_DIR,
        ) as work_dir:
            frame_path = os.path.join(work_dir, "last_frame.png")
            base_video_path = os.path.join(work_dir, "base_video.mp4")
            tail_path = os.path.join(work_dir, "tail.mp4")
            concat_list_path = os.path.join(work_dir, "concat.txt")
            joined_video_path = os.path.join(work_dir, "joined_video.mp4")

            # Витягуємо саме останній доступний кадр, не застосовуючи
            # автоматичний поворот до закодованих розмірів відеопотоку.
            ok, error = await run_ffmpeg([
                "-sseof", "-1",
                "-noautorotate",
                "-i", base_path,
                "-map", "0:v:0",
                "-vf", "reverse",
                "-frames:v", "1",
                "-update", "1",
                frame_path,
            ])
            if not ok or not os.path.exists(frame_path):
                return False, error or "Не вдалося витягнути останній кадр."

            # Оригінальний відеопотік відокремлюємо без перекодування.
            ok, error = await run_ffmpeg([
                "-i", base_path,
                "-map", "0:v:0",
                "-c:v", "copy",
                "-an",
                "-map_metadata", "0",
                "-movflags", "+faststart",
                base_video_path,
            ])
            if not ok:
                return False, error

            filters = [
                f"setsar={profile['sar']}",
                (
                    "boxblur="
                    "luma_radius='min(100,min(w,h)/2-1)':"
                    "luma_power=1:"
                    "chroma_radius='min(100,min(cw,ch)/2-1)':"
                    "chroma_power=1"
                ),
                "gblur=sigma=100",
                (
                    "drawbox="
                    "x=iw/2-(ih*0.19):"
                    "y=ih/2-(ih*0.07):"
                    "w=ih*0.38:"
                    "h=ih*0.14:"
                    "color=black@0.82:"
                    "t=fill"
                ),
            ]

            countdown_expr = (
                "%{eif\\:"
                f"max(0\\,{extend_seconds}-floor(t))"
                "\\:d\\:2}"
            )
            filters.append(
                (
                    "drawtext="
                    f"text='{countdown_expr}':"
                    "fontcolor=white:"
                    "fontsize=h*0.075:"
                    "x=(w-text_w)/2:"
                    "y=(h-text_h)/2:"
                    "shadowcolor=black@0.40:"
                    "shadowx=2:"
                    "shadowy=2"
                )
            )

            tail_args = [
                "-loop", "1",
                "-framerate", profile["fps"],
                "-i", frame_path,
                "-t", str(extend_seconds),
                "-vf", ",".join(filters),
                "-an",
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-tune", "stillimage",
                "-crf", "35",
                "-pix_fmt", "yuv420p",
                "-profile:v", profile["profile"],
                "-r", profile["fps"],
                "-fps_mode", "cfr",
            ]

            if profile["level"]:
                tail_args.extend(["-level:v", profile["level"]])

            for key, option in (
                ("color_range", "-color_range"),
                ("color_space", "-colorspace"),
                ("color_transfer", "-color_trc"),
                ("color_primaries", "-color_primaries"),
            ):
                value = profile.get(key)
                if value and value not in {"unknown", "N/A"}:
                    tail_args.extend([option, value])

            if profile["time_base_timescale"]:
                tail_args.extend([
                    "-video_track_timescale",
                    str(profile["time_base_timescale"]),
                ])

            tail_args.extend([
                "-movflags", "+faststart",
                tail_path,
            ])

            ok, error = await run_ffmpeg(tail_args)
            if not ok:
                return False, error

            # Concat demuxer об'єднує два відеопотоки без re-encode.
            with open(concat_list_path, "w", encoding="utf-8") as file:
                file.write(f"file '{base_video_path}'\n")
                file.write(f"file '{tail_path}'\n")

            ok, error = await run_ffmpeg([
                "-f", "concat",
                "-safe", "0",
                "-i", concat_list_path,
                "-map", "0:v:0",
                "-c:v", "copy",
                "-an",
                "-movflags", "+faststart",
                joined_video_path,
            ])
            if not ok:
                return False, error

            # Повертаємо оригінальне аудіо. Воно не перекодовується
            # і, як і раніше, закінчується на кінці вихідного звуку.
            mux_args = [
                "-i", joined_video_path,
                "-i", base_path,
                "-map", "0:v:0",
                "-map", "1:a?",
                "-c:v", "copy",
                "-c:a", "copy",
                "-map_metadata", "1",
                "-movflags", "+faststart",
                output_path,
            ]
            ok, error = await run_ffmpeg(mux_args)
            if not ok:
                return False, error

            output_duration = await get_video_duration(output_path)
            expected_duration = duration + extend_seconds
            if output_duration < expected_duration - 1.0:
                return False, (
                    "Фінальне відео має неочікувану тривалість "
                    "після об'єднання."
                )

            return True, "ok"

    except Exception as e:
        return False, str(e)


async def process_video_file(
    input_path: str,
    output_path: str,
    mode: str,
    extend_seconds: int,
) -> tuple[bool, str]:
    """
    Для +30/+60 спочатку обробляє основне відео без подовження,
    а потім додає окремо стиснутий фрагмент. Якщо швидке об'єднання
    несумісне з вихідним форматом, використовує попередній FFmpeg-режим.
    """
    if extend_seconds <= 0:
        return await process_video_file_legacy(
            input_path=input_path,
            output_path=output_path,
            mode=mode,
            extend_seconds=0,
        )

    # У metadata-режимі перевіряємо сумісність ДО створення проміжного
    # файлу, щоб не витрачати час на зайвий remux для непідтримуваних кодеків.
    if mode == "metadata":
        original_stream = await get_video_stream_info(input_path)
        if not fast_append_profile(original_stream):
            return await process_video_file_legacy(
                input_path=input_path,
                output_path=output_path,
                mode=mode,
                extend_seconds=extend_seconds,
            )

    os.makedirs(TMP_DIR, exist_ok=True)
    base_path = os.path.join(
        TMP_DIR,
        f"base_{uuid.uuid4().hex}.mp4",
    )

    try:
        ok, error = await process_video_file_legacy(
            input_path=input_path,
            output_path=base_path,
            mode=mode,
            extend_seconds=0,
        )
        if not ok:
            return False, error

        ok, error = await append_compressed_tail(
            base_path=base_path,
            output_path=output_path,
            extend_seconds=extend_seconds,
        )
        if ok:
            return True, "ok"

        print(
            "Compressed tail append failed; "
            f"using legacy full re-encode fallback: {error[-1000:]}"
        )

        return await process_video_file_legacy(
            input_path=input_path,
            output_path=output_path,
            mode=mode,
            extend_seconds=extend_seconds,
        )

    finally:
        try:
            if os.path.exists(base_path):
                os.remove(base_path)
        except OSError:
            pass


# =========================================================
# /ID
# ПРАЦЮЄ ДЛЯ ВСІХ
# =========================================================

@dp.message(Command("id"))
async def cmd_id(message: Message):

    await message.answer(
        f"🆔 <b>Твій Telegram ID:</b>\n"
        f"<code>{message.from_user.id}</code>"
    )


# =========================================================
# /BROADCAST
# ТІЛЬКИ ДЛЯ АДМІНІВ
# =========================================================

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):

    uid = message.from_user.id

    if uid not in ADMIN_USERS:
        return

    if not ALLOWED_USERS:
        await message.answer(
            "❌ ALLOWED_USERS порожній.\n"
            "Немає кому робити розсилку."
        )
        return

    full_text = message.text or ""
    parts = full_text.split(maxsplit=1)

    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "Напиши текст після команди.\n\n"
            "Наприклад:\n"
            "<code>/broadcast Бот оновлено ✅</code>"
        )
        return

    broadcast_text = parts[1].strip()

    success = 0
    failed = 0

    status = await message.answer(
        f"📤 Починаю розсилку для "
        f"{len(ALLOWED_USERS)} користувачів..."
    )

    for user_id in ALLOWED_USERS:
        try:
            await bot.send_message(
                chat_id=user_id,
                text=broadcast_text,
                parse_mode=None,
            )
            success += 1

        except Exception as e:
            failed += 1
            print(
                f"Broadcast error for {user_id}: {e}"
            )

        await asyncio.sleep(0.05)

    await status.edit_text(
        "✅ <b>Розсилка завершена</b>\n\n"
        f"Успішно: <b>{success}</b>\n"
        f"Помилок: <b>{failed}</b>"
    )


# =========================================================
# /STATS — ТІЛЬКИ ДЛЯ АДМІНІВ
# =========================================================

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    uid = message.from_user.id

    if uid not in ADMIN_USERS:
        return

    await message.answer(
        stats_period_data("7d"),
        reply_markup=stats_period_keyboard(),
    )


@dp.callback_query(F.data.startswith("stats|"))
async def choose_stats_period(callback: CallbackQuery):
    uid = callback.from_user.id

    if uid not in ADMIN_USERS:
        await callback.answer(
            "Ця статистика доступна тільки адміністратору.",
            show_alert=True,
        )
        return

    try:
        _, period = callback.data.split("|", 1)
    except ValueError:
        await callback.answer("Некоректний період.", show_alert=True)
        return

    if period not in {"7d", "30d", "all"}:
        await callback.answer("Невідомий період.", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_text(
        stats_period_data(period),
        reply_markup=stats_period_keyboard(),
    )


# =========================================================
# /START
# =========================================================

@dp.message(CommandStart())
async def cmd_start(message: Message):

    if not is_allowed(
        message.from_user.id
    ):
        await access_denied(message)
        return

    stats_touch_user(message.from_user.id)

    await message.answer(
        "👋 <b>Унікалізатор готовий</b>\n\n"
        "Надсилай відео або фото.\n"
        "Можеш накидати одразу кілька відео — "
        "вони утворять одну пачку. "
        "Режим і подовження вибираються один раз "
        "для всієї пачки. 👇"
    )


# =========================================================
# ПРИЙОМ MEDIA
# =========================================================

@dp.message(
    F.video |
    F.photo |
    F.document
)
async def on_media(message: Message):

    if not is_allowed(message.from_user.id):
        await access_denied(message)
        return

    uid = message.from_user.id

    file_obj = None
    media_type = None
    original_name = None
    extension = None

    if message.video:
        file_obj = message.video
        media_type = "video"
        original_name = message.video.file_name or "video.mp4"
        extension = get_extension(original_name, "mp4")

    elif message.photo:
        file_obj = message.photo[-1]
        media_type = "image"
        original_name = "image.jpg"
        extension = "jpg"

    elif message.document:
        file_obj = message.document
        mime = (message.document.mime_type or "").lower()
        original_name = message.document.file_name or "file"
        extension = get_extension(original_name, "")

        if mime.startswith("video/"):
            media_type = "video"
            extension = extension or "mp4"
        elif mime.startswith("image/"):
            media_type = "image"
            extension = extension or "jpg"
        elif extension in {"mp4", "mov", "m4v", "webm", "avi", "mkv"}:
            media_type = "video"
        elif extension in {"jpg", "jpeg", "png", "webp"}:
            media_type = "image"
        else:
            await message.answer(
                "❌ Підтримуються тільки відео та зображення."
            )
            return

    if not file_obj:
        return

    item_id = uuid.uuid4().hex[:12]
    stats_id = f"{message.chat.id}:{message.message_id}"
    initial_size = int(file_obj.file_size or 0)

    stats_start_operation(
        event_key=stats_id,
        user_id=uid,
        media_type=media_type,
        file_size_bytes=initial_size or None,
    )

    if file_obj.file_size:
        max_file_mb = MAX_VIDEO_MB if media_type == "video" else MAX_IMAGE_MB
        max_bytes = max_file_mb * 1024 * 1024

        if file_obj.file_size > max_bytes:
            size_mb = file_obj.file_size / 1024 / 1024
            stats_update_operation(
                stats_id,
                status="failed",
                error_stage="validation",
                error_type="FileTooLarge",
            )
            await message.answer(
                "❌ Файл завеликий.\n\n"
                f"Розмір: {size_mb:.1f} МБ\n"
                f"Максимум: {max_file_mb} МБ."
            )
            return

    local_path = os.path.join(
        TMP_DIR,
        f"input_{uid}_{item_id}.{extension}"
    )

    # Резервуємо відео в пачці ДО першого await і ДО завантаження.
    # Тому обробник пачки одразу бачить усі повідомлення, що вже прийшли.
    video_batch = None
    if media_type == "video":
        video_batch = get_video_batch(uid, message.chat.id)
        video_batch["pending_downloads"] += 1
        video_batch["total_received"] += 1
        video_batch["pending_message_ids"].add(message.message_id)
        video_batch["received_message_ids"].add(message.message_id)
        video_batch["wake_event"].set()

        stats_create_batch(video_batch["batch_id"], uid)
        stats_update_operation(
            stats_id,
            batch_id=video_batch["batch_id"],
        )
        stats_update_batch(
            batch_id=video_batch["batch_id"],
            total_files=video_batch["total_received"],
            mode=video_batch.get("mode"),
            extension_seconds=video_batch.get("extend_seconds"),
        )

    status = None
    try:
        status = await message.answer("⏳ Завантажую файл...")
    except Exception as error:
        print(f"Could not send upload status message: {error}")

    try:
        await download_telegram_file(file_obj, local_path)

        if not os.path.exists(local_path):
            raise RuntimeError("Файл не був завантажений.")

    except Exception as error:
        remove_file(local_path)

        stats_update_operation(
            stats_id,
            batch_id=(video_batch["batch_id"] if video_batch else None),
            status="failed",
            error_stage="download",
            error_type=type(error).__name__,
        )

        if video_batch:
            video_batch["pending_downloads"] = max(
                0,
                video_batch["pending_downloads"] - 1,
            )
            video_batch["pending_message_ids"].discard(message.message_id)
            video_batch["failed"] += 1
            video_batch["wake_event"].set()
            finish_empty_video_batch(uid, video_batch)

        error_text = (
            "❌ <b>Помилка завантаження</b>\n\n"
            f"<code>{str(error)}</code>"
        )
        try:
            if status:
                await status.edit_text(error_text)
            else:
                await message.answer(error_text)
        except Exception as send_error:
            print(f"Could not report download error: {send_error}")
        return

    actual_size_bytes = os.path.getsize(local_path)
    downloaded_size = actual_size_bytes / 1024 / 1024

    stats_update_operation(
        stats_id,
        file_size_bytes=actual_size_bytes,
    )

    print(f"Downloaded: {downloaded_size:.2f} MB")

    # =====================================================
    # VIDEO -> ОДНА ПАЧКА, ОДНІ НАЛАШТУВАННЯ
    # =====================================================

    if media_type == "video":
        batch = video_batch
        duration = await get_video_duration(local_path)

        item = {
            "id": item_id,
            "stats_id": stats_id,
            "file_size_bytes": actual_size_bytes,
            "path": local_path,
            "type": "video",
            "original_name": original_name,
            "chat_id": message.chat.id,
            "message_id": message.message_id,
        }

        batch["files"].append(item)
        batch["files"].sort(key=lambda x: x.get("message_id", 0))
        batch["downloaded_count"] += 1
        batch["pending_downloads"] = max(
            0,
            batch["pending_downloads"] - 1,
        )
        batch["pending_message_ids"].discard(message.message_id)
        batch["wake_event"].set()

        stats_update_operation(
            stats_id,
            batch_id=batch["batch_id"],
            video_duration_seconds=duration or None,
        )
        stats_update_batch(
            batch_id=batch["batch_id"],
            total_files=batch["total_received"],
            mode=batch.get("mode"),
            extension_seconds=batch.get("extend_seconds"),
        )

        await update_video_batch_status(
            batch=batch,
            status=status,
            message=message,
        )

        return

    # =====================================================
    # IMAGE -> СТАРА ІНДИВІДУАЛЬНА ЧЕРГА
    # =====================================================

    queue = get_user_queue(uid)
    item = {
        "id": item_id,
        "stats_id": stats_id,
        "file_size_bytes": actual_size_bytes,
        "path": local_path,
        "type": media_type,
        "original_name": original_name,
        "chat_id": message.chat.id,
        "status": "waiting",
        "mode": None,
        "extend_seconds": 0,
    }

    queue.append(item)
    position = len(queue)

    if position == 1 and uid not in processing_users:
        if status:
            await status.edit_text(
                "✅ <b>Файл додано.</b>\n\n"
                f"Розмір: {downloaded_size:.1f} МБ\n"
                "Зараз покажу налаштування обробки."
            )
        await show_next_item(uid)
    else:
        text = (
            "✅ <b>Додано в чергу.</b>\n\n"
            f"📦 Позиція: <b>{position}</b>\n"
            f"📚 Всього в черзі: <b>{len(queue)}</b>"
        )
        if status:
            await status.edit_text(text)
        else:
            await message.answer(text)


# =========================================================
# VIDEO BATCH: ВИБІР РЕЖИМУ ДЛЯ ВСІЄЇ ПАЧКИ
# =========================================================

@dp.callback_query(F.data.startswith("batch_vm|"))
async def choose_batch_video_mode(
    callback: CallbackQuery
):

    uid = callback.from_user.id

    if not is_allowed(uid):
        await callback.answer(
            "Доступ обмежений.",
            show_alert=True,
        )
        return

    batch = video_batches.get(uid)

    if not batch:
        await callback.answer(
            "Пачка вже завершена або не знайдена.",
            show_alert=True,
        )
        return

    if batch.get("state") != "choosing_mode":
        await callback.answer(
            "Режим для цієї пачки вже вибрано.",
            show_alert=True,
        )
        return

    try:
        _, mode = callback.data.split("|", 1)
    except ValueError:
        await callback.answer(
            "Некоректна кнопка.",
            show_alert=True,
        )
        return

    if mode not in {"metadata", "noise", "preview"}:
        await callback.answer(
            "Невідомий режим.",
            show_alert=True,
        )
        return

    batch["settings_message_sent"] = True
    batch["settings_message_id"] = callback.message.message_id
    batch["chat_id"] = callback.message.chat.id
    batch["mode"] = mode
    batch["state"] = "choosing_extension"

    stats_update_batch(
        batch_id=batch["batch_id"],
        mode=mode,
    )

    mode_names = {
        "metadata": "⚡ Швидкий режим",
        "noise": "🎞 Легка зміна відео",
        "preview": "🖼 Метадані + прев’ю",
    }

    await callback.answer()

    try:
        await callback.message.edit_text(
            f"✅ Для всієї пачки обрано:\n"
            f"<b>{mode_names[mode]}</b>\n\n"
            "Тепер обери подовження.\n"
            "Це налаштування також застосовується "
            "<b>до всієї пачки</b>.\n\n"
            "При +30/+60 сек останній кадр "
            "сильно розмивається, поверх по центру йде "
            "countdown на чорній підкладці, а доданий "
            "відрізок залишається у тиші.",
            reply_markup=batch_extension_keyboard(),
        )
    except Exception as error:
        print(f"Could not update batch mode message: {error}")
        fallback = await callback.message.answer(
            f"✅ Для всієї пачки обрано: <b>{mode_names[mode]}</b>\n\n"
            "Тепер обери подовження:",
            reply_markup=batch_extension_keyboard(),
        )
        batch["settings_message_id"] = fallback.message_id


# =========================================================
# VIDEO BATCH: +0 / +30 / +60 ДЛЯ ВСІЄЇ ПАЧКИ
# =========================================================

@dp.callback_query(F.data.startswith("batch_ve|"))
async def choose_batch_extension(
    callback: CallbackQuery
):

    uid = callback.from_user.id

    if not is_allowed(uid):
        await callback.answer(
            "Доступ обмежений.",
            show_alert=True,
        )
        return

    batch = video_batches.get(uid)

    if not batch:
        await callback.answer(
            "Пачка вже завершена або не знайдена.",
            show_alert=True,
        )
        return

    if batch.get("state") != "choosing_extension":
        await callback.answer(
            "Подовження вже вибрано.",
            show_alert=True,
        )
        return

    try:
        _, seconds_raw = callback.data.split("|", 1)
        extend_seconds = int(seconds_raw)
    except (ValueError, TypeError):
        await callback.answer(
            "Некоректна кнопка.",
            show_alert=True,
        )
        return

    if extend_seconds not in {0, 30, 60}:
        await callback.answer(
            "Некоректна тривалість.",
            show_alert=True,
        )
        return

    batch["settings_message_sent"] = True
    batch["settings_message_id"] = callback.message.message_id
    batch["chat_id"] = callback.message.chat.id
    batch["extend_seconds"] = extend_seconds
    batch["state"] = "processing"

    stats_update_batch(
        batch_id=batch["batch_id"],
        total_files=batch["total_received"],
        mode=batch["mode"],
        extension_seconds=extend_seconds,
    )

    processing_users.add(uid)
    batch["worker_running"] = True

    extend_text = (
        "без подовження"
        if extend_seconds == 0
        else f"+{extend_seconds} сек"
    )

    mode_names = {
        "metadata": "⚡ Швидкий режим",
        "noise": "🎞 Легка зміна відео",
        "preview": "🖼 Метадані + прев’ю",
    }

    await callback.answer()

    try:
        await callback.message.edit_text(
            "⏳ <b>Починаю обробку пачки</b>\n\n"
            f"Режим: <b>{mode_names[batch['mode']]}</b>\n"
            f"Подовження: <b>{extend_text}</b>\n"
            f"📦 Відео: <b>{batch['total_received']}</b>\n"
            f"⬇️ Ще завантажується: <b>{batch['pending_downloads']}</b>\n\n"
            "Файли будуть оброблятися по черзі."
        )
    except Exception as error:
        print(f"Could not update batch start message: {error}")
        try:
            fallback = await callback.message.answer(
                "⏳ <b>Починаю обробку пачки...</b>"
            )
            batch["settings_message_id"] = fallback.message_id
        except Exception as send_error:
            print(f"Could not send batch start message: {send_error}")

    # Не блокуємо обробку нових повідомлень.
    asyncio.create_task(
        process_video_batch(uid, batch)
    )


async def process_video_batch(
    uid: int,
    batch: dict,
):
    chat_id = batch["chat_id"]
    status_message_id = batch.get("settings_message_id")

    try:
        while True:
            files = batch["files"]
            files.sort(key=lambda x: x.get("message_id", 0))

            item = None
            if files:
                first_item = files[0]
                first_message_id = first_item.get("message_id", 0)
                earlier_download_pending = any(
                    pending_id < first_message_id
                    for pending_id in batch["pending_message_ids"]
                )

                if not earlier_download_pending:
                    item = files.pop(0)

            if item is None:
                if batch["pending_downloads"] > 0:
                    # Чекаємо, поки завершиться хоча б одне завантаження.
                    # clear() виконується без await після перевірки стану,
                    # тому сигнал від завершення завантаження не загубиться.
                    batch["wake_event"].clear()
                    await batch["wake_event"].wait()
                    continue

                # Черга порожня і незавершених завантажень немає.
                # Даємо коротке вікно, щоб наступні Telegram updates із тієї
                # самої серії повідомлень встигли зареєструватися в цій пачці.
                if not batch["files"]:
                    batch["wake_event"].clear()
                    try:
                        await asyncio.wait_for(
                            batch["wake_event"].wait(),
                            timeout=BATCH_IDLE_GRACE_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        pass

                    if batch["files"] or batch["pending_downloads"] > 0:
                        continue

                    # Закриваємо пачку синхронно до наступного await.
                    batch["state"] = "finalizing"
                    break

                # Є готовий файл, але перед ним ще завантажується попередній.
                batch["wake_event"].clear()
                await batch["wake_event"].wait()
                continue

            local_path = item["path"]
            mode = batch["mode"]
            extend_seconds = batch["extend_seconds"]
            stats_id = item.get("stats_id")

            batch["active"] = True
            batch["started"] += 1
            current_number = sum(
                1
                for message_id in batch["received_message_ids"]
                if message_id <= item.get("message_id", 0)
            )
            current_number = max(current_number, batch["started"])

            stats_update_operation(
                stats_id,
                mode=mode,
                extension_seconds=extend_seconds,
            )

            total_now = batch["total_received"]
            waiting_now = len(batch["files"]) + batch["pending_downloads"]

            if status_message_id:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=(
                            "⚙️ <b>Обробляю пачку...</b>\n\n"
                            f"Зараз: <b>{current_number}/{total_now}</b>\n"
                            f"Залишилось у черзі: <b>{waiting_now}</b>\n"
                            f"З них ще завантажується: "
                            f"<b>{batch['pending_downloads']}</b>"
                        ),
                    )
                except Exception:
                    pass

            out_path = os.path.join(
                TMP_DIR,
                f"output_{uid}_{uuid.uuid4().hex}.mp4"
            )

            processing_started = time.perf_counter()
            processing_seconds = 0.0
            processing_completed = False

            try:
                ok, error = await process_video_file(
                    input_path=local_path,
                    output_path=out_path,
                    mode=mode,
                    extend_seconds=extend_seconds,
                )

                processing_seconds = time.perf_counter() - processing_started
                processing_completed = bool(ok)

                if not ok:
                    batch["failed"] += 1
                    stats_update_operation(
                        stats_id,
                        mode=mode,
                        extension_seconds=extend_seconds,
                        processing_seconds=processing_seconds,
                        status="failed",
                        error_stage="processing",
                        error_type="FFmpegError",
                    )
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"❌ <b>Помилка у файлі {current_number}</b>\n\n"
                                f"<code>{error}</code>\n\n"
                                "Переходжу до наступного відео."
                            ),
                        )
                    except Exception as send_error:
                        print(f"Could not report processing error: {send_error}")
                    continue

                if not os.path.exists(out_path):
                    batch["failed"] += 1
                    stats_update_operation(
                        stats_id,
                        mode=mode,
                        extension_seconds=extend_seconds,
                        processing_seconds=processing_seconds,
                        status="failed",
                        error_stage="processing",
                        error_type="OutputFileMissing",
                    )
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"❌ Файл {current_number}: "
                                "FFmpeg не створив результат.\n"
                                "Переходжу до наступного."
                            ),
                        )
                    except Exception as send_error:
                        print(f"Could not report missing output: {send_error}")
                    continue

                output_file = FSInputFile(
                    out_path,
                    filename=f"unique_{uuid.uuid4().hex[:8]}.mp4",
                )

                await bot.send_video(
                    chat_id=chat_id,
                    video=output_file,
                    caption=(
                        f"✅ <b>Готово {current_number}/"
                        f"{batch['total_received']}</b>"
                    ),
                    supports_streaming=True,
                )

                stats_update_operation(
                    stats_id,
                    mode=mode,
                    extension_seconds=extend_seconds,
                    processing_seconds=processing_seconds,
                    status="success",
                )
                batch["processed"] += 1

            except Exception as error:
                batch["failed"] += 1
                stats_update_operation(
                    stats_id,
                    mode=mode,
                    extension_seconds=extend_seconds,
                    processing_seconds=processing_seconds,
                    status="failed",
                    error_stage=(
                        "upload" if processing_completed else "processing"
                    ),
                    error_type=type(error).__name__,
                )
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"❌ <b>Помилка у файлі {current_number}</b>\n\n"
                            f"<code>{str(error)}</code>\n\n"
                            "Переходжу до наступного."
                        ),
                    )
                except Exception as send_error:
                    print(f"Could not report file error: {send_error}")

            finally:
                remove_file(out_path)
                remove_file(local_path)
                batch["active"] = False
                batch["wake_event"].set()

        processed = batch["processed"]
        failed = batch["failed"]
        total = batch["total_received"]

        stats_finish_batch(
            batch_id=batch["batch_id"],
            processed=processed,
            failed=failed,
            mode=batch.get("mode") or "not_selected",
            extension_seconds=batch.get("extend_seconds") or 0,
        )

        if status_message_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message_id,
                    text=(
                        "✅ <b>Пачку завершено</b>\n\n"
                        f"Всього: <b>{total}</b>\n"
                        f"Готово: <b>{processed}</b>\n"
                        f"Помилок: <b>{failed}</b>"
                    ),
                )
            except Exception:
                pass

        if video_batches.get(uid) is batch:
            video_batches.pop(uid, None)

    except Exception as error:
        print(f"Video batch worker crashed for user {uid}: {error}")
        batch["worker_running"] = False
        # Не видаляємо пачку при неочікуваній помилці: це краще, ніж
        # непомітно втратити файли, які ще очікують обробки.
        try:
            if status_message_id:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message_id,
                    text=(
                        "⚠️ <b>Обробка пачки призупинилася через помилку.</b>\n"
                        "Файли не видалені. Звернися до адміністратора."
                    ),
                )
        except Exception:
            pass

    finally:
        batch["worker_running"] = False
        current_batch = video_batches.get(uid)
        if not current_batch or current_batch.get("state") != "processing":
            processing_users.discard(uid)


# =========================================================
# IMAGE PROCESSING
# =========================================================

@dp.callback_query(F.data.startswith("im|"))
async def process_image(
    callback: CallbackQuery
):

    uid = callback.from_user.id

    if not is_allowed(uid):
        await callback.answer(
            "Доступ обмежений.",
            show_alert=True,
        )
        return

    try:
        _, item_id, mode = callback.data.split("|", 2)
    except ValueError:
        await callback.answer("Некоректна кнопка.", show_alert=True)
        return

    if mode not in {"metadata", "pixels"}:
        await callback.answer("Невідомий режим.", show_alert=True)
        return

    if not item_is_active(uid, item_id):
        await callback.answer(
            "Цей файл уже не активний.",
            show_alert=True,
        )
        return

    if uid in processing_users:
        await callback.answer(
            "Файл уже обробляється.",
            show_alert=True,
        )
        return

    item = active_item(uid)

    if item["type"] != "image":
        await callback.answer("Це не зображення.", show_alert=True)
        return

    item["status"] = "processing"
    processing_users.add(uid)

    stats_id = item.get("stats_id")
    stats_mode = (
        "image_metadata" if mode == "metadata" else "image_pixels"
    )
    stats_update_operation(
        stats_id,
        mode=stats_mode,
        extension_seconds=0,
    )

    await callback.answer()

    local_path = item["path"]
    out_path = None
    processing_started = time.perf_counter()
    processing_seconds = 0.0
    processing_completed = False
    stats_recorded = False

    try:
        with Image.open(local_path) as img:

            has_alpha = (
                img.mode in ("RGBA", "LA")
                or "transparency" in img.info
            )

        output_extension = (
            "png"
            if has_alpha
            else "jpg"
        )

        out_path = os.path.join(
            TMP_DIR,
            f"image_{uid}_{uuid.uuid4().hex}.{output_extension}"
        )

        await callback.message.edit_text(
            "🖼 Обробляю зображення..."
        )

        if mode == "metadata":

            ok, error = await asyncio.to_thread(
                image_metadata_mode,
                local_path,
                out_path,
            )

        else:

            ok, error = await asyncio.to_thread(
                image_pixel_mode,
                local_path,
                out_path,
            )

        processing_seconds = time.perf_counter() - processing_started
        processing_completed = bool(ok)

        if not ok:

            stats_update_operation(
                stats_id,
                mode=stats_mode,
                extension_seconds=0,
                processing_seconds=processing_seconds,
                status="failed",
                error_stage="processing",
                error_type="ImageProcessingError",
            )
            stats_recorded = True

            await callback.message.edit_text(
                "❌ Помилка:\n\n"
                f"<code>{error}</code>"
            )
            return

        output_file = FSInputFile(
            out_path,
            filename=(
                f"unique_"
                f"{uuid.uuid4().hex[:8]}"
                f".{output_extension}"
            ),
        )

        await callback.message.answer_document(
            document=output_file,
            caption="✅ <b>Готово!</b>",
        )

        stats_update_operation(
            stats_id,
            mode=stats_mode,
            extension_seconds=0,
            processing_seconds=processing_seconds,
            status="success",
        )
        stats_recorded = True

        await callback.message.delete()

    except Exception as e:

        if not stats_recorded:
            stats_update_operation(
                stats_id,
                mode=stats_mode,
                extension_seconds=0,
                processing_seconds=processing_seconds,
                status="failed",
                error_stage=(
                    "upload" if processing_completed else "processing"
                ),
                error_type=type(e).__name__,
            )

        await callback.message.edit_text(
            "❌ <b>Помилка</b>\n\n"
            f"<code>{str(e)}</code>"
        )

    finally:

        if out_path:
            remove_file(out_path)

        await finish_current_item(uid)


# =========================================================
# OTHER
# =========================================================

@dp.message()
async def unsupported(
    message: Message
):

    if not is_allowed(
        message.from_user.id
    ):
        await access_denied(message)
        return

    await message.answer(
        "Надішли мені відео або фото.\n\n"
        f"🎬 Відео — до {MAX_VIDEO_MB} МБ\n"
        f"🖼 JPG / PNG / WEBP — до {MAX_IMAGE_MB} МБ\n\n"
        "Можна надсилати кілька відео підряд — "
        "вони обробляться однією пачкою "
        "з однаковими налаштуваннями."
    )


# =========================================================
# START
# =========================================================

async def main():

    if not BOT_TOKEN:
        raise SystemExit(
            "BOT_TOKEN не встановлений."
        )

    stats_init()

    print(
        "===================================="
    )
    print("MEDIA UNIQUE BOT")
    print(
        "===================================="
    )
    print(
        f"Max video: {MAX_VIDEO_MB} MB | "
        f"Max image: {MAX_IMAGE_MB} MB"
    )
    print(
        f"Allowed users: "
        f"{len(ALLOWED_USERS) if ALLOWED_USERS else 'ALL'}"
    )
    print(
        f"Admin users: "
        f"{len(ADMIN_USERS)}"
    )
    print(
        f"Telegram API: {TELEGRAM_API_URL}"
    )
    print(
        f"File server: {FILE_SERVER_URL}"
    )
    print(
        "===================================="
    )

    try:

        await dp.start_polling(
            bot,
            allowed_updates=(
                dp.resolve_used_update_types()
            ),
        )

    finally:

        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
