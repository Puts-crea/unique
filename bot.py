import asyncio
import os
import random
import tempfile
import uuid
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
# ALLOWED USERS
# =========================================================

def load_allowed_users() -> set[int]:
    raw = os.getenv("ALLOWED_USERS", "").strip()

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
            print(
                f"WARNING: invalid ALLOWED_USERS value: {item}"
            )

    return result


ALLOWED_USERS = load_allowed_users()


def is_allowed(user_id: int) -> bool:
    # Якщо змінна порожня — доступ відкритий усім.
    # Коли додамо ID — доступ буде тільки whitelist.
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
# ПАМ'ЯТЬ
# =========================================================

user_files = {}
processing_users = set()


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


def cleanup_user_file(user_id: int):
    data = user_files.get(user_id)

    if data:
        remove_file(data.get("path"))

    user_files.pop(user_id, None)


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

            return False, text[-2000:]

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
        "Чекаю на твої файли 👇"
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

    cleanup_user_file(uid)

    unique_id = uuid.uuid4().hex

    local_path = os.path.join(
        TMP_DIR,
        f"input_{uid}_{unique_id}.{extension}"
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

    user_files[uid] = {
        "path": local_path,
        "type": media_type,
        "original_name": original_name,
    }

    if media_type == "video":

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⚡ Швидкий режим",
                        callback_data="video_metadata",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🎞 Легка зміна відео",
                        callback_data="video_noise",
                    )
                ],
            ]
        )

        await status.edit_text(
            "✅ <b>Відео отримано.</b>\n\n"
            f"Розмір: {downloaded_size:.1f} МБ\n\n"
            "Обери спосіб обробки:",
            reply_markup=kb,
        )

    else:

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📝 Очистити метадані",
                        callback_data="image_metadata",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🖼 Змінити пікселі",
                        callback_data="image_pixels",
                    )
                ],
            ]
        )

        await status.edit_text(
            "✅ <b>Зображення отримано.</b>\n\n"
            "Обери спосіб обробки:",
            reply_markup=kb,
        )


# =========================================================
# VIDEO PROCESSING
# =========================================================

@dp.callback_query(
    F.data.in_({
        "video_metadata",
        "video_noise",
    })
)
async def process_video(
    callback: CallbackQuery
):

    uid = callback.from_user.id

    await callback.answer()

    if not is_allowed(uid):

        await callback.message.edit_text(
            "⛔ <b>Доступ до бота обмежений.</b>\n\n"
            "Твій Telegram ID:\n"
            f"<code>{uid}</code>"
        )

        return

    if uid in processing_users:

        await callback.answer(
            "Файл уже обробляється.",
            show_alert=True,
        )
        return

    data = user_files.get(uid)

    if not data:

        await callback.message.edit_text(
            "❌ Файл загубився. "
            "Надішли його ще раз."
        )
        return

    local_path = data["path"]

    if not os.path.exists(local_path):

        await callback.message.edit_text(
            "❌ Тимчасовий файл "
            "вже видалений."
        )
        return

    processing_users.add(uid)

    out_path = os.path.join(
        TMP_DIR,
        f"output_{uid}_{uuid.uuid4().hex}.mp4"
    )

    try:

        if callback.data == "video_metadata":

            await callback.message.edit_text(
                "⚡ Обробляю відео..."
            )

            unique = uuid.uuid4().hex

            args = [
                "-i",
                local_path,

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

                out_path,
            ]

        else:

            await callback.message.edit_text(
                "🎞 Перекодовую відео...\n\n"
                "Це може зайняти деякий час."
            )

            unique = uuid.uuid4().hex

            args = [
                "-i",
                local_path,

                "-map",
                "0:v:0",

                "-map",
                "0:a?",

                "-vf",
                "noise=alls=2:allf=t+u",

                "-c:v",
                "libx264",

                "-preset",
                "veryfast",

                "-crf",
                "23",

                "-pix_fmt",
                "yuv420p",

                "-c:a",
                "aac",

                "-b:a",
                "192k",

                "-metadata",
                f"title=Unique_{unique}",

                "-metadata",
                f"comment=ID_{unique}",

                "-movflags",
                "+faststart",

                out_path,
            ]

        ok, error = await run_ffmpeg(
            args
        )

        if not ok:

            await callback.message.edit_text(
                "❌ <b>FFmpeg помилка</b>\n\n"
                f"<code>{error}</code>"
            )
            return

        if not os.path.exists(out_path):

            await callback.message.edit_text(
                "❌ FFmpeg не створив файл."
            )
            return

        size_mb = (
            os.path.getsize(out_path)
            / 1024
            / 1024
        )

        await callback.message.edit_text(
            "📤 Відправляю готове відео...\n\n"
            f"Розмір: {size_mb:.1f} МБ"
        )

        output_file = FSInputFile(
            out_path,
            filename=(
                f"unique_"
                f"{uuid.uuid4().hex[:8]}.mp4"
            ),
        )

        await callback.message.answer_video(
            video=output_file,
            caption="✅ <b>Готово!</b>",
            supports_streaming=True,
        )

        await callback.message.delete()

        cleanup_user_file(uid)

    except Exception as e:

        await callback.message.edit_text(
            "❌ <b>Помилка обробки</b>\n\n"
            f"<code>{str(e)}</code>"
        )

    finally:

        remove_file(out_path)
        processing_users.discard(uid)


# =========================================================
# IMAGE PROCESSING
# =========================================================

@dp.callback_query(
    F.data.in_({
        "image_metadata",
        "image_pixels",
    })
)
async def process_image(
    callback: CallbackQuery
):

    uid = callback.from_user.id

    await callback.answer()

    if not is_allowed(uid):

        await callback.message.edit_text(
            "⛔ <b>Доступ до бота обмежений.</b>\n\n"
            "Твій Telegram ID:\n"
            f"<code>{uid}</code>"
        )

        return

    if uid in processing_users:

        await callback.answer(
            "Файл уже обробляється.",
            show_alert=True,
        )
        return

    data = user_files.get(uid)

    if not data:

        await callback.message.edit_text(
            "❌ Файл загубився."
        )
        return

    local_path = data["path"]

    processing_users.add(uid)

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

        if callback.data == "image_metadata":

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

        cleanup_user_file(uid)

    except Exception as e:

        await callback.message.edit_text(
            "❌ <b>Помилка</b>\n\n"
            f"<code>{str(e)}</code>"
        )

    finally:

        if out_path:
            remove_file(out_path)

        processing_users.discard(uid)


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
        "🖼 JPG / PNG / WEBP"
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
