import asyncio
import os
import random
import tempfile
import uuid
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
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

# Адреса нашого власного Telegram Bot API.
# На Railway потім поставимо:
# http://telegram-api.railway.internal:8081
TELEGRAM_API_URL = os.getenv(
    "TELEGRAM_API_URL",
    "https://api.telegram.org",
).rstrip("/")

# Максимальний розмір вхідного файлу
MAX_FILE_MB = 50

# Якщо порожньо — ботом можуть користуватися всі.
ALLOWED_USERS = set()

# Наприклад:
# ALLOWED_USERS = {123456789}

TMP_DIR = tempfile.gettempdir()


# =========================================================
# TELEGRAM BOT
# =========================================================

if TELEGRAM_API_URL != "https://api.telegram.org":
    telegram_api = TelegramAPIServer.from_base(
        TELEGRAM_API_URL
    )

    session = AiohttpSession(
        api=telegram_api,
        timeout=300,
    )

    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )

    print(
        f"Використовується власний Telegram API: "
        f"{TELEGRAM_API_URL}"
    )

else:
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )

    print("Використовується стандартний Telegram API")


dp = Dispatcher()


# =========================================================
# ТИМЧАСОВА ПАМ'ЯТЬ
# =========================================================

# Тут зберігаємо інформацію про останній файл користувача.
#
# Формат:
#
# user_files[user_id] = {
#     "path": "/tmp/...",
#     "type": "video" або "image",
#     "original_name": "video.mp4"
# }

user_files = {}

# Щоб користувач випадково не запустив одну й ту саму
# обробку кілька разів одночасно.
processing_users = set()


# =========================================================
# ДОПОМІЖНІ ФУНКЦІЇ
# =========================================================

def is_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True

    return user_id in ALLOWED_USERS


def remove_file(path: str | None):
    """Безпечно видаляє тимчасовий файл."""

    if not path:
        return

    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def cleanup_user_file(user_id: int):
    """Видаляє попередній файл користувача."""

    data = user_files.get(user_id)

    if data:
        remove_file(data.get("path"))

    user_files.pop(user_id, None)


def get_extension(filename: str | None, default: str) -> str:
    """Отримує розширення файлу."""

    if not filename:
        return default

    suffix = Path(filename).suffix.lower().replace(".", "")

    if not suffix:
        return default

    return suffix


async def run_ffmpeg(args: list[str]) -> tuple[bool, str]:
    """
    Запускає FFmpeg.

    Повертає:
    True, "ok"
    або
    False, текст помилки
    """

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            error_text = stderr.decode(
                errors="ignore"
            )

            return False, error_text[-1500:]

        return True, "ok"

    except FileNotFoundError:
        return (
            False,
            "FFmpeg не встановлено на сервері."
        )

    except Exception as e:
        return False, str(e)


# =========================================================
# ОБРОБКА ЗОБРАЖЕНЬ
# =========================================================

def image_metadata_mode(
    input_path: str,
    output_path: str,
) -> tuple[bool, str]:
    """
    Перезаписує картинку без старих EXIF/metadata.

    Саме зображення візуально не змінюється.
    """

    try:
        with Image.open(input_path) as source:
            img = ImageOps.exif_transpose(source)

            # Якщо є прозорість — PNG.
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


def image_pixel_mode(
    input_path: str,
    output_path: str,
) -> tuple[bool, str]:
    """
    Вносить дуже маленькі зміни в окремі пікселі.

    Зміни +/- 1 у кольоровому каналі практично
    непомітні оку.
    """

    try:
        with Image.open(input_path) as source:
            img = ImageOps.exif_transpose(source)

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

            # Приблизно 1 змінений піксель на кожні 500.
            # Але не більше 20 000 змін.
            changes = max(
                300,
                min(
                    20000,
                    total_pixels // 500
                )
            )

            for _ in range(changes):

                x = random.randrange(width)
                y = random.randrange(height)

                pixel = pixels[x, y]

                if img.mode == "RGBA":
                    r, g, b, a = pixel

                    channel = random.randint(0, 2)

                    rgb = [r, g, b]

                    rgb[channel] = max(
                        0,
                        min(
                            255,
                            rgb[channel]
                            + random.choice((-1, 1))
                        )
                    )

                    pixels[x, y] = (
                        rgb[0],
                        rgb[1],
                        rgb[2],
                        a,
                    )

                else:
                    r, g, b = pixel

                    channel = random.randint(0, 2)

                    rgb = [r, g, b]

                    rgb[channel] = max(
                        0,
                        min(
                            255,
                            rgb[channel]
                            + random.choice((-1, 1))
                        )
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
# START
# =========================================================

@dp.message(CommandStart())
async def cmd_start(message: Message):

    if not is_allowed(message.from_user.id):
        await message.answer(
            "⛔ Доступ заборонено."
        )
        return

    await message.answer(
        "👋 <b>Унікалізатор медіа</b>\n\n"

        "Надішли мені:\n\n"

        "🎬 <b>Відео</b> — до 50 МБ\n"
        "🖼 <b>Фото</b> — JPG, PNG, WEBP\n\n"

        "Для відео доступно:\n"
        "⚡ швидка зміна без перекодування\n"
        "🎞 легка зміна відеопотоку\n\n"

        "Для зображень:\n"
        "📝 очищення метаданих\n"
        "🖼 мінімальна зміна пікселів"
    )


# =========================================================
# ПРИЙОМ ФАЙЛІВ
# =========================================================

@dp.message(F.video | F.photo | F.document)
async def on_media(message: Message):

    if not is_allowed(message.from_user.id):
        await message.answer(
            "⛔ Доступ заборонено."
        )
        return

    uid = message.from_user.id

    file_obj = None
    media_type = None
    original_name = None
    extension = None

    # -----------------------------------------------------
    # VIDEO
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # PHOTO
    # -----------------------------------------------------

    elif message.photo:

        # Telegram надсилає декілька розмірів фото.
        # [-1] — найбільший.
        file_obj = message.photo[-1]

        media_type = "image"

        original_name = "image.jpg"
        extension = "jpg"

    # -----------------------------------------------------
    # DOCUMENT
    # -----------------------------------------------------

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

        # Відео як document
        if mime.startswith("video/"):

            media_type = "video"

            if not extension:
                extension = "mp4"

        # Фото як document
        elif mime.startswith("image/"):

            media_type = "image"

            if not extension:
                extension = "jpg"

        # Деякі Telegram-клієнти можуть
        # не передати MIME правильно.
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
                "❌ Цей тип файлу не підтримується.\n\n"
                "Надішли відео або JPG / PNG / WEBP."
            )

            return

    if not file_obj:
        return

    # -----------------------------------------------------
    # ПЕРЕВІРКА РОЗМІРУ
    # -----------------------------------------------------

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
                f"❌ Файл завеликий.\n\n"
                f"Розмір: {size_mb:.1f} МБ\n"
                f"Максимум: {MAX_FILE_MB} МБ."
            )

            return

    # Видаляємо попередній файл цього користувача.
    cleanup_user_file(uid)

    unique_id = uuid.uuid4().hex

    local_path = os.path.join(
        TMP_DIR,
        f"input_{uid}_{unique_id}.{extension}"
    )

    status = await message.answer(
        "⏳ Завантажую файл..."
    )

    # -----------------------------------------------------
    # DOWNLOAD
    # -----------------------------------------------------

    try:

        await bot.download(
            file_obj,
            destination=local_path,
        )

    except Exception as e:

        await status.edit_text(
            "❌ <b>Помилка завантаження</b>\n\n"
            f"<code>{str(e)}</code>"
        )

        remove_file(local_path)

        return

    # -----------------------------------------------------
    # ЗАПАМ'ЯТОВУЄМО ФАЙЛ
    # -----------------------------------------------------

    user_files[uid] = {
        "path": local_path,
        "type": media_type,
        "original_name": original_name,
    }

    # -----------------------------------------------------
    # КНОПКИ ДЛЯ ВІДЕО
    # -----------------------------------------------------

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
            "Обери спосіб обробки:\n\n"

            "⚡ <b>Швидкий</b> — змінює контейнер "
            "і метадані без перекодування.\n\n"

            "🎞 <b>Легка зміна</b> — перекодовує "
            "відео та додає дуже слабкий шум.",
            reply_markup=kb,
        )

    # -----------------------------------------------------
    # КНОПКИ ДЛЯ ФОТО
    # -----------------------------------------------------

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
            "Обери спосіб обробки:\n\n"

            "📝 <b>Метадані</b> — перезаписує "
            "файл без старих EXIF.\n\n"

            "🖼 <b>Пікселі</b> — додатково "
            "вносить мінімальні непомітні зміни.",
            reply_markup=kb,
        )


# =========================================================
# ОБРОБКА ВІДЕО
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

    if uid in processing_users:

        await callback.answer(
            "Файл уже обробляється.",
            show_alert=True,
        )

        return

    data = user_files.get(uid)

    if not data:

        await callback.message.edit_text(
            "❌ Файл загубився.\n"
            "Надішли відео ще раз."
        )

        return

    if data.get("type") != "video":

        await callback.message.edit_text(
            "❌ Це не відеофайл."
        )

        return

    local_path = data["path"]

    if not os.path.exists(local_path):

        user_files.pop(uid, None)

        await callback.message.edit_text(
            "❌ Тимчасовий файл уже видалений.\n"
            "Надішли відео ще раз."
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
                "0",

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
                "Цей режим працює довше."
            )

            unique = uuid.uuid4().hex

            args = [
                "-i",
                local_path,

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

        ok, error = await run_ffmpeg(args)

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
            f"📤 Відправляю готове відео...\n"
            f"Розмір: {size_mb:.1f} МБ"
        )

        output_file = FSInputFile(
            out_path,
            filename=f"unique_{uuid.uuid4().hex[:8]}.mp4",
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
# ОБРОБКА ЗОБРАЖЕНЬ
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

    if uid in processing_users:

        await callback.answer(
            "Файл уже обробляється.",
            show_alert=True,
        )

        return

    data = user_files.get(uid)

    if not data:

        await callback.message.edit_text(
            "❌ Файл загубився.\n"
            "Надішли зображення ще раз."
        )

        return

    if data.get("type") != "image":

        await callback.message.edit_text(
            "❌ Це не зображення."
        )

        return

    local_path = data["path"]

    if not os.path.exists(local_path):

        user_files.pop(uid, None)

        await callback.message.edit_text(
            "❌ Тимчасовий файл уже видалений."
        )

        return

    processing_users.add(uid)

    # Перевіряємо, чи є прозорість.
    try:

        with Image.open(local_path) as check_img:

            has_alpha = (
                check_img.mode in ("RGBA", "LA")
                or "transparency" in check_img.info
            )

    except Exception as e:

        processing_users.discard(uid)

        await callback.message.edit_text(
            "❌ Не вдалося відкрити зображення.\n\n"
            f"<code>{str(e)}</code>"
        )

        return

    if has_alpha:

        output_extension = "png"

    else:

        output_extension = "jpg"

    out_path = os.path.join(
        TMP_DIR,
        f"image_{uid}_{uuid.uuid4().hex}.{output_extension}"
    )

    try:

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
                "❌ <b>Помилка обробки фото</b>\n\n"
                f"<code>{error}</code>"
            )

            return

        if not os.path.exists(out_path):

            await callback.message.edit_text(
                "❌ Зображення не було створено."
            )

            return

        # Відправляємо як document,
        # щоб Telegram повторно не стискав картинку.

        output_file = FSInputFile(
            out_path,
            filename=(
                f"unique_"
                f"{uuid.uuid4().hex[:8]}."
                f"{output_extension}"
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

        remove_file(out_path)

        processing_users.discard(uid)


# =========================================================
# НЕПІДТРИМУВАНІ ПОВІДОМЛЕННЯ
# =========================================================

@dp.message()
async def unsupported(message: Message):

    if not is_allowed(message.from_user.id):
        return

    await message.answer(
        "Надішли мені відео або зображення.\n\n"
        "🎬 Відео — до 50 МБ\n"
        "🖼 JPG / PNG / WEBP"
    )


# =========================================================
# ЗАПУСК
# =========================================================

async def main():

    if not BOT_TOKEN:

        raise SystemExit(
            "❌ BOT_TOKEN не встановлений."
        )

    print("====================================")
    print("MEDIA UNIQUE BOT")
    print("====================================")
    print(f"Max file: {MAX_FILE_MB} MB")
    print(f"Telegram API: {TELEGRAM_API_URL}")
    print("====================================")

    try:

        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
        )

    finally:

        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
