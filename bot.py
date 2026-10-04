import asyncio
import os
import subprocess
import tempfile
import uuid

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)

# ============ НАЛАШТУВАННЯ ============
BOT_TOKEN = os.getenv("BOT_TOKEN", "ВСТАВТЕ_СЮДИ_ВАШ_ТОКЕН")
ALLOWED_USERS = set()  # порожньо = всі можуть користуватись
# Якщо хочете обмежити доступ, впишіть ID:
# ALLOWED_USERS = {123456789}

MAX_FILE_MB = 50  # ліміт Telegram Bot API

# ============ БОТ ============
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# тимчасова памʼять: хто в якому режимі
user_mode = {}
user_files = {}

TMP_DIR = tempfile.gettempdir()


def is_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS


async def run_ffmpeg(args: list) -> tuple:
    """Запускає ffmpeg, повертає (успіх, повідомлення)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            return False, stderr.decode(errors="ignore")[-800:]
        return True, "ok"
    except FileNotFoundError:
        return False, "FFmpeg не встановлено на сервері. Встановіть: apt install ffmpeg"


# ============ ХЕНДЛЕРИ ============
@dp.message(CommandStart())
async def cmd_start(message: Message):
    if not is_allowed(message.from_user.id):
        await message.answer("⛔ Доступ заборонено.")
        return
    await message.answer(
        "👋 Надішли мені відео (до 50 МБ).\n\n"
        "Я запропоную два режими:\n"
        "• <b>Змінити метадані</b> — швидко, без перекодування\n"
        "• <b>Додати легкий шум</b> — повільніше, змінює відео"
    )


@dp.message(F.video | F.document)
async def on_video(message: Message):
    if not is_allowed(message.from_user.id):
        await message.answer("⛔ Доступ заборонено.")
        return

    file_obj = message.video or message.document

    if file_obj.file_size and file_obj.file_size > MAX_FILE_MB * 1024 * 1024:
        await message.answer(f"❌ Файл завеликий. Максимум {MAX_FILE_MB} МБ.")
        return

    uid = message.from_user.id
    ext = "mp4"
    if message.document and message.document.file_name:
        ext = message.document.file_name.rsplit(".", 1)[-1].lower()

    local_path = os.path.join(TMP_DIR, f"in_{uid}_{uuid.uuid4().hex}.{ext}")

    status = await message.answer("⏳ Завантажую файл...")
    try:
        await bot.download(file_obj, destination=local_path)
    except Exception as e:
        await status.edit_text(f"❌ Помилка завантаження: {e}")
        return

    user_files[uid] = local_path

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Змінити метадані", callback_data="mode_metadata")],
        [InlineKeyboardButton(text="🎞 Додати шум", callback_data="mode_noise")],
    ])
    await status.edit_text(
        "Файл отримано. Обери режим обробки:",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("mode_"))
async def on_mode(callback: CallbackQuery):
    uid = callback.from_user.id
    mode = callback.data.replace("mode_", "")
    user_mode[uid] = mode

    local_path = user_files.get(uid)
    if not local_path or not os.path.exists(local_path):
        await callback.message.edit_text("❌ Файл загубився. Надішли відео ще раз.")
        return

    await callback.message.edit_text("⏳ Обробляю... це може зайняти кілька секунд.")

    out_path = local_path.rsplit(".", 1)[0] + "_out.mp4"

    if mode == "metadata":
        args = [
            "-i", local_path,
            "-c", "copy",
            "-metadata", f"title=Unique_{uuid.uuid4().hex[:8]}",
            "-metadata", "comment=Processed",
            "-metadata", "date=2026",
            out_path,
        ]
    elif mode == "noise":
        args = [
            "-i", local_path,
            "-vf", "noise=alls=4:allf=t+u",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "copy",
            "-metadata", f"title=Unique_{uuid.uuid4().hex[:8]}",
            out_path,
        ]
    else:
        await callback.message.edit_text("❌ Невідомий режим.")
        return

    ok, err = await run_ffmpeg(args)

    if not ok:
        await callback.message.edit_text(f"❌ Помилка FFmpeg:\n<code>{err}</code>")
        return

    if not os.path.exists(out_path):
        await callback.message.edit_text("❌ Файл не створився. Можливо, формат не підтримується.")
        return

    try:
        await callback.message.answer_video(
            FSInputFile(out_path, filename=f"unique_{os.path.basename(local_path)}"),
            caption="✅ Готово!",
        )
        await callback.message.delete()
    except Exception as e:
        await callback.message.edit_text(f"❌ Помилка відправки: {e}")

    for p in (local_path, out_path):
        try:
            os.remove(p)
        except OSError:
            pass
    user_files.pop(uid, None)
    user_mode.pop(uid, None)


async def main():
    if BOT_TOKEN.startswith("ВСТАВТЕ"):
        raise SystemExit("❌ Впишіть BOT_TOKEN у змінну середовища або прямо в код.")
    print("Бот запущено. Ctrl+C для зупинки.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
