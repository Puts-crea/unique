import asyncio
import os
import random
import tempfile
import uuid
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

MAX_FILE_MB = 50
TMP_DIR = tempfile.gettempdir()


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

# Користувачі, в яких зараз реально працює FFmpeg/Pillow.
processing_users: set[int] = set()


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

async def download_telegram_file(
    file_obj,
    destination: str,
):

    telegram_file = await bot.get_file(
        file_obj.file_id
    )

    file_path = telegram_file.file_path

    if not file_path:
        raise RuntimeError(
            "Telegram не повернув file_path."
        )

    print(f"Telegram file_path: {file_path}")

    if os.path.isabs(file_path):

        root = "/var/lib/telegram-bot-api/"

        if not file_path.startswith(root):
            raise RuntimeError(
                "Отримано невідомий абсолютний шлях: "
                f"{file_path}"
            )

        relative_path = file_path[len(root):]

        safe_path = quote(
            relative_path,
            safe="/",
        )

        download_url = (
            f"{FILE_SERVER_URL}/{safe_path}"
        )

        print(
            f"Downloading through file server: "
            f"{download_url}"
        )

        timeout = aiohttp.ClientTimeout(
            total=900,
            connect=60,
        )

        async with aiohttp.ClientSession(
            timeout=timeout
        ) as client:

            async with client.get(
                download_url
            ) as response:

                if response.status != 200:

                    text = await response.text()

                    raise RuntimeError(
                        "File server error: "
                        f"HTTP {response.status}: "
                        f"{text[:300]}"
                    )

                with open(
                    destination,
                    "wb",
                ) as output:

                    async for chunk in (
                        response.content.iter_chunked(
                            1024 * 1024
                        )
                    ):
                        output.write(chunk)

        return

    await bot.download_file(
        file_path,
        destination=destination,
    )


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


async def process_video_file(
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
        # Останній кадр заморожується на +30/+60 секунд.
        # Аудіодоріжку НЕ подовжуємо, тому додана частина тиха.
        filters.append(
            f"tpad=stop_mode=clone:stop_duration={extend_seconds}"
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
# /START
# =========================================================

@dp.message(CommandStart())
async def cmd_start(message: Message):

    if not is_allowed(
        message.from_user.id
    ):
        await access_denied(message)
        return

    await message.answer(
        "👋 <b>Унікалізатор готовий</b>\n\n"
        "Надсилай відео або фото.\n"
        "Можеш накидати одразу кілька файлів — "
        "вони підуть у чергу та обробляться по черзі. 👇"
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

    if not is_allowed(
        message.from_user.id
    ):
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

        original_name = (
            message.video.file_name
            or "video.mp4"
        )

        extension = get_extension(
            original_name,
            "mp4",
        )

    elif message.photo:

        file_obj = message.photo[-1]
        media_type = "image"
        original_name = "image.jpg"
        extension = "jpg"

    elif message.document:

        file_obj = message.document

        mime = (
            message.document.mime_type
            or ""
        ).lower()

        original_name = (
            message.document.file_name
            or "file"
        )

        extension = get_extension(
            original_name,
            "",
        )

        if mime.startswith("video/"):

            media_type = "video"
            extension = extension or "mp4"

        elif mime.startswith("image/"):

            media_type = "image"
            extension = extension or "jpg"

        elif extension in {
            "mp4",
            "mov",
            "m4v",
            "webm",
            "avi",
            "mkv",
        }:
            media_type = "video"

        elif extension in {
            "jpg",
            "jpeg",
            "png",
            "webp",
        }:
            media_type = "image"

        else:
            await message.answer(
                "❌ Підтримуються тільки "
                "відео та зображення."
            )
            return

    if not file_obj:
        return

    if file_obj.file_size:

        max_bytes = (
            MAX_FILE_MB
            * 1024
            * 1024
        )

        if file_obj.file_size > max_bytes:

            size_mb = (
                file_obj.file_size
                / 1024
                / 1024
            )

            await message.answer(
                "❌ Файл завеликий.\n\n"
                f"Розмір: {size_mb:.1f} МБ\n"
                f"Максимум: {MAX_FILE_MB} МБ."
            )
            return

    item_id = uuid.uuid4().hex[:12]

    local_path = os.path.join(
        TMP_DIR,
        f"input_{uid}_{item_id}.{extension}"
    )

    status = await message.answer(
        "⏳ Завантажую файл..."
    )

    try:
        await download_telegram_file(
            file_obj,
            local_path,
        )

    except Exception as e:

        remove_file(local_path)

        await status.edit_text(
            "❌ <b>Помилка завантаження</b>\n\n"
            f"<code>{str(e)}</code>"
        )
        return

    if not os.path.exists(local_path):

        await status.edit_text(
            "❌ Файл не був завантажений."
        )
        return

    downloaded_size = (
        os.path.getsize(local_path)
        / 1024
        / 1024
    )

    print(
        f"Downloaded: "
        f"{downloaded_size:.2f} MB"
    )

    queue = get_user_queue(uid)

    item = {
        "id": item_id,
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
        await status.edit_text(
            "✅ <b>Файл додано.</b>\n\n"
            f"Розмір: {downloaded_size:.1f} МБ\n"
            "Зараз покажу налаштування обробки."
        )

        await show_next_item(uid)

    else:
        await status.edit_text(
            "✅ <b>Додано в чергу.</b>\n\n"
            f"📦 Позиція: <b>{position}</b>\n"
            f"📚 Всього в черзі: <b>{len(queue)}</b>"
        )


# =========================================================
# VIDEO: ВИБІР РЕЖИМУ
# =========================================================

@dp.callback_query(F.data.startswith("vm|"))
async def choose_video_mode(
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

    if mode not in {"metadata", "noise", "preview"}:
        await callback.answer("Невідомий режим.", show_alert=True)
        return

    if not item_is_active(uid, item_id):
        await callback.answer(
            "Цей файл уже не активний.",
            show_alert=True,
        )
        return

    item = active_item(uid)

    if item.get("status") != "choosing_mode":
        await callback.answer(
            "Режим уже вибрано.",
            show_alert=True,
        )
        return

    item["mode"] = mode
    item["status"] = "choosing_extension"

    mode_names = {
        "metadata": "⚡ Швидкий режим",
        "noise": "🎞 Легка зміна відео",
        "preview": "🖼 Метадані + прев’ю",
    }

    await callback.answer()

    await callback.message.edit_text(
        f"✅ Обрано: <b>{mode_names[mode]}</b>\n\n"
        "Подовжити відео останнім кадром?\n"
        "Додана частина буде без звуку.",
        reply_markup=extension_keyboard(item_id),
    )


# =========================================================
# VIDEO: ВИБІР +0 / +30 / +60 СЕКУНД
# =========================================================

@dp.callback_query(F.data.startswith("ve|"))
async def choose_video_extension(
    callback: CallbackQuery
):

    uid = callback.from_user.id

    try:
        _, item_id, seconds_raw = callback.data.split("|", 2)
        extend_seconds = int(seconds_raw)
    except (ValueError, TypeError):
        await callback.answer("Некоректна кнопка.", show_alert=True)
        return

    if extend_seconds not in {0, 30, 60}:
        await callback.answer("Некоректна тривалість.", show_alert=True)
        return

    if not item_is_active(uid, item_id):
        await callback.answer(
            "Цей файл уже не активний.",
            show_alert=True,
        )
        return

    item = active_item(uid)

    if item.get("status") != "choosing_extension":
        await callback.answer(
            "Налаштування вже вибрані.",
            show_alert=True,
        )
        return

    item["extend_seconds"] = extend_seconds
    item["status"] = "processing"

    processing_users.add(uid)

    await callback.answer()

    queue = get_user_queue(uid)
    total = len(queue)

    extend_text = (
        "без подовження"
        if extend_seconds == 0
        else f"+{extend_seconds} сек"
    )

    await callback.message.edit_text(
        "⏳ <b>Обробляю відео...</b>\n\n"
        f"Подовження: <b>{extend_text}</b>\n"
        f"📦 Зараз: <b>1/{total}</b> у поточній черзі"
    )

    await process_active_video(
        uid,
        callback.message.chat.id,
    )


async def process_active_video(
    uid: int,
    chat_id: int,
):

    item = active_item(uid)

    if not item:
        processing_users.discard(uid)
        return

    local_path = item["path"]
    mode = item["mode"]
    extend_seconds = item["extend_seconds"]

    out_path = os.path.join(
        TMP_DIR,
        f"output_{uid}_{uuid.uuid4().hex}.mp4"
    )

    try:
        ok, error = await process_video_file(
            input_path=local_path,
            output_path=out_path,
            mode=mode,
            extend_seconds=extend_seconds,
        )

        if not ok:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "❌ <b>FFmpeg помилка</b>\n\n"
                    f"<code>{error}</code>"
                ),
            )
            return

        if not os.path.exists(out_path):
            await bot.send_message(
                chat_id=chat_id,
                text="❌ FFmpeg не створив файл.",
            )
            return

        size_mb = (
            os.path.getsize(out_path)
            / 1024
            / 1024
        )

        await bot.send_message(
            chat_id=chat_id,
            text=(
                "📤 Відправляю готове відео...\n"
                f"Розмір: {size_mb:.1f} МБ"
            ),
        )

        output_file = FSInputFile(
            out_path,
            filename=(
                f"unique_"
                f"{uuid.uuid4().hex[:8]}.mp4"
            ),
        )

        await bot.send_video(
            chat_id=chat_id,
            video=output_file,
            caption="✅ <b>Готово!</b>",
            supports_streaming=True,
        )

    except Exception as e:

        await bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ <b>Помилка обробки</b>\n\n"
                f"<code>{str(e)}</code>"
            ),
        )

    finally:
        remove_file(out_path)
        await finish_current_item(uid)


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

    await callback.answer()

    local_path = item["path"]
    out_path = None

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

        if not ok:

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

        await callback.message.delete()

    except Exception as e:

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
        "🎬 Відео — до 50 МБ\n"
        "🖼 JPG / PNG / WEBP\n\n"
        "Можна надсилати кілька файлів підряд — "
        "вони стануть у чергу."
    )


# =========================================================
# START
# =========================================================

async def main():

    if not BOT_TOKEN:
        raise SystemExit(
            "BOT_TOKEN не встановлений."
        )

    print(
        "===================================="
    )
    print("MEDIA UNIQUE BOT")
    print(
        "===================================="
    )
    print(
        f"Max file: {MAX_FILE_MB} MB"
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
