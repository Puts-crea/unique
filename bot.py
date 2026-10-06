import os
import io
import json
import time
import uuid
import shlex
import shutil
import random
import asyncio
import tempfile
import subprocess
from pathlib import Path
from typing import Optional

import aiohttp
from PIL import Image

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer


# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TELEGRAM_API_URL = os.getenv("TELEGRAM_API_URL", "").strip()
FILE_SERVER_URL = os.getenv("FILE_SERVER_URL", "").strip()

# список айді через кому: 12345,67890
ALLOWED_USERS_RAW = os.getenv("ALLOWED_USERS", "").strip()
ALLOWED_USERS = {
    int(x.strip())
    for x in ALLOWED_USERS_RAW.split(",")
    if x.strip().isdigit()
}

MAX_FILE_SIZE_MB = 50
MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024
PENDING_TTL = 60 * 60  # 1 година


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")


# =========================
# BOT / DP
# =========================
if TELEGRAM_API_URL:
    api = TelegramAPIServer.from_base(TELEGRAM_API_URL, is_local=True)
    session = AiohttpSession(api=api)
    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
else:
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

dp = Dispatcher()

# task_id -> data
PENDING_TASKS = {}


# =========================
# HELPERS
# =========================
def user_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS


def safe_unlink(path: str | Path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


def safe_rmtree(path: str | Path):
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def cleanup_pending():
    now = time.time()
    to_delete = []
    for task_id, data in PENDING_TASKS.items():
        if now - data["created_at"] > PENDING_TTL:
            safe_rmtree(data["tmpdir"])
            to_delete.append(task_id)
    for task_id in to_delete:
        PENDING_TASKS.pop(task_id, None)


def get_ext(filename: Optional[str], default_ext: str) -> str:
    if not filename:
        return default_ext
    ext = Path(filename).suffix.lower()
    return ext if ext else default_ext


def is_video_document(message: Message) -> bool:
    if not message.document:
        return False
    mime = (message.document.mime_type or "").lower()
    name = (message.document.file_name or "").lower()
    video_exts = {
        ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm", ".mpeg", ".mpg"
    }
    return mime.startswith("video/") or Path(name).suffix.lower() in video_exts


def is_image_document(message: Message) -> bool:
    if not message.document:
        return False
    mime = (message.document.mime_type or "").lower()
    name = (message.document.file_name or "").lower()
    image_exts = {".jpg", ".jpeg", ".png", ".webp"}
    return mime.startswith("image/") or Path(name).suffix.lower() in image_exts


def video_kb(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚡ Швидко", callback_data=f"proc|vf|{task_id}"
                ),
                InlineKeyboardButton(
                    text="🎞 Глибше", callback_data=f"proc|vd|{task_id}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🖼 Preview blur", callback_data=f"proc|vb|{task_id}"
                )
            ],
        ]
    )


def image_kb(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📝 Метадані", callback_data=f"proc|im|{task_id}"
                ),
                InlineKeyboardButton(
                    text="🖼 Пікселі", callback_data=f"proc|ip|{task_id}"
                ),
            ]
        ]
    )


async def run_command(cmd: list[str]) -> tuple[int, str, str]:
    def _run():
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode, result.stdout, result.stderr

    return await asyncio.to_thread(_run)


async def download_file_via_http(url: str, dst_path: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=120) as resp:
            resp.raise_for_status()
            with open(dst_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 1024):
                    if chunk:
                        f.write(chunk)


async def download_telegram_file(file_id: str, dst_path: str):
    tg_file = await bot.get_file(file_id)

    # 1) пробуємо стандартний download
    try:
        await bot.download(tg_file, destination=dst_path)
        return
    except Exception:
        pass

    # 2) fallback через FILE_SERVER_URL
    if FILE_SERVER_URL and tg_file.file_path:
        url = f"{FILE_SERVER_URL.rstrip('/')}/{tg_file.file_path.lstrip('/')}"
        await download_file_via_http(url, dst_path)
        return

    raise RuntimeError("Не вдалося завантажити файл")


def clamp(val: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, val))


def parse_fps(fps_str: str) -> float:
    if not fps_str:
        return 30.0
    if "/" in fps_str:
        a, b = fps_str.split("/", 1)
        try:
            a = float(a)
            b = float(b)
            if b != 0:
                return a / b
        except Exception:
            return 30.0
    try:
        return float(fps_str)
    except Exception:
        return 30.0


async def probe_video(video_path: str) -> dict:
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate",
        "-of", "json",
        video_path,
    ]
    code, out, err = await run_command(cmd)
    if code != 0:
        raise RuntimeError(f"ffprobe error: {err}")

    data = json.loads(out)
    streams = data.get("streams", [])
    if not streams:
        raise RuntimeError("Не знайдено відеопотік")

    st = streams[0]
    width = int(st.get("width", 1080))
    height = int(st.get("height", 1920))
    fps = parse_fps(st.get("r_frame_rate", "30/1"))
    if fps <= 0 or fps > 120:
        fps = 30.0

    return {"width": width, "height": height, "fps": fps}


# =========================
# IMAGE PROCESSING
# =========================
async def process_image_metadata_only(input_path: str, output_path: str):
    def _work():
        with Image.open(input_path) as img:
            fmt = (img.format or "").upper()
            if fmt not in {"JPEG", "PNG", "WEBP"}:
                fmt = "PNG"

            save_kwargs = {}
            out_img = img.copy()

            if fmt == "JPEG":
                if out_img.mode in ("RGBA", "LA", "P"):
                    out_img = out_img.convert("RGB")
                save_kwargs.update(quality=95, optimize=True)
            elif fmt == "PNG":
                save_kwargs.update(optimize=True)
            elif fmt == "WEBP":
                if out_img.mode not in ("RGB", "RGBA"):
                    out_img = out_img.convert("RGBA")
                save_kwargs.update(quality=95, method=6)

            out_img.save(output_path, format=fmt, **save_kwargs)

    await asyncio.to_thread(_work)


async def process_image_pixels(input_path: str, output_path: str):
    def _work():
        with Image.open(input_path) as img:
            fmt = (img.format or "").upper()
            if fmt not in {"JPEG", "PNG", "WEBP"}:
                fmt = "PNG"

            out_img = img.copy()
            if out_img.mode not in ("RGB", "RGBA"):
                if fmt == "JPEG":
                    out_img = out_img.convert("RGB")
                else:
                    out_img = out_img.convert("RGBA")

            pixels = out_img.load()
            width, height = out_img.size

            # дуже дрібні зміни
            changes = max(10, min(50, (width * height) // 200000))
            for _ in range(changes):
                x = random.randint(0, width - 1)
                y = random.randint(0, height - 1)
                px = pixels[x, y]

                if isinstance(px, int):
                    pixels[x, y] = clamp(px + random.choice([-1, 1]), 0, 255)
                else:
                    vals = list(px)
                    channels = min(3, len(vals))
                    ch = random.randint(0, channels - 1)
                    vals[ch] = clamp(vals[ch] + random.choice([-1, 1]), 0, 255)
                    pixels[x, y] = tuple(vals)

            save_kwargs = {}
            if fmt == "JPEG":
                if out_img.mode in ("RGBA", "LA", "P"):
                    out_img = out_img.convert("RGB")
                save_kwargs.update(quality=95, optimize=True)
            elif fmt == "PNG":
                save_kwargs.update(optimize=True)
            elif fmt == "WEBP":
                save_kwargs.update(quality=95, method=6)

            out_img.save(output_path, format=fmt, **save_kwargs)

    await asyncio.to_thread(_work)


# =========================
# VIDEO PROCESSING
# =========================
async def process_video_fast(input_path: str, output_path: str):
    cmd = [
        "ffmpeg",
        "-y",
        "-i", input_path,
        "-map", "0",
        "-c", "copy",
        "-map_metadata", "-1",
        "-movflags", "+faststart",
        output_path,
    ]
    code, out, err = await run_command(cmd)
    if code != 0:
        raise RuntimeError(f"ffmpeg fast error:\n{err}")


async def process_video_deep(input_path: str, output_path: str):
    vf = (
        "scale=trunc(iw/2)*2:trunc(ih/2)*2,"
        "eq=contrast=1.001:brightness=0.001:saturation=1.002,"
        "noise=alls=2:allf=t+u"
    )
    cmd = [
        "ffmpeg",
        "-y",
        "-i", input_path,
        "-vf", vf,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-map_metadata", "-1",
        "-movflags", "+faststart",
        output_path,
    ]
    code, out, err = await run_command(cmd)
    if code != 0:
        raise RuntimeError(f"ffmpeg deep error:\n{err}")


async def process_video_preview_blur(input_path: str, output_path: str, workdir: str):
    info = await probe_video(input_path)
    width = info["width"]
    height = info["height"]
    fps = info["fps"]

    first_frame = str(Path(workdir) / "first_frame.png")
    intro_video = str(Path(workdir) / "intro.mp4")

    # 1 кадр з першого фрейму + сильне розмиття
    extract_cmd = [
        "ffmpeg",
        "-y",
        "-i", input_path,
        "-vf", "select=eq(n\\,0),boxblur=100:1,gblur=sigma=100",
        "-frames:v", "1",
        first_frame,
    ]
    code, out, err = await run_command(extract_cmd)
    if code != 0:
        raise RuntimeError(f"extract first frame error:\n{err}")

    # робимо 1-frame intro відео
    intro_duration = 1.0 / fps
    intro_cmd = [
        "ffmpeg",
        "-y",
        "-loop", "1",
        "-i", first_frame,
        "-t", f"{intro_duration:.6f}",
        "-r", f"{fps:.6f}",
        "-vf", f"scale={width}:{height},format=yuv420p",
        "-an",
        "-c:v", "libx264",
        "-preset", "veryfast",
        intro_video,
    ]
    code, out, err = await run_command(intro_cmd)
    if code != 0:
        raise RuntimeError(f"intro video error:\n{err}")

    # склеюємо blurred intro + оригінальне відео
    # аудіо беремо з оригіналу без змін
    concat_cmd = [
        "ffmpeg",
        "-y",
        "-i", intro_video,
        "-i", input_path,
        "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]",
        "-map", "[v]",
        "-map", "1:a?",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "18",
        "-c:a", "copy",
        "-map_metadata", "-1",
        "-movflags", "+faststart",
        output_path,
    ]
    code, out, err = await run_command(concat_cmd)

    # якщо copy аудіо не злетіло — fallback у AAC
    if code != 0:
        concat_cmd_fallback = [
            "ffmpeg",
            "-y",
            "-i", intro_video,
            "-i", input_path,
            "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]",
            "-map", "[v]",
            "-map", "1:a?",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "18",
            "-c:a", "aac",
            "-b:a", "128k",
            "-map_metadata", "-1",
            "-movflags", "+faststart",
            output_path,
        ]
        code, out, err = await run_command(concat_cmd_fallback)
        if code != 0:
            raise RuntimeError(f"preview blur concat error:\n{err}")


# =========================
# TASK CREATION
# =========================
async def create_task_from_video(message: Message, file_id: str, original_name: str, file_size: int):
    if file_size > MAX_FILE_SIZE:
        await message.answer(f"❌ Максимальний розмір відео — {MAX_FILE_SIZE_MB} МБ.")
        return

    cleanup_pending()

    task_id = uuid.uuid4().hex
    tmpdir = tempfile.mkdtemp(prefix="uniqbot_")
    ext = get_ext(original_name, ".mp4")
    input_path = str(Path(tmpdir) / f"input{ext}")

    await download_telegram_file(file_id, input_path)

    PENDING_TASKS[task_id] = {
        "type": "video",
        "tmpdir": tmpdir,
        "input_path": input_path,
        "original_name": original_name or f"video{ext}",
        "created_at": time.time(),
        "user_id": message.from_user.id,
    }

    await message.answer(
        "Обери режим для відео:",
        reply_markup=video_kb(task_id),
    )


async def create_task_from_image(message: Message, file_id: str, original_name: str, file_size: int):
    if file_size > MAX_FILE_SIZE:
        await message.answer(f"❌ Максимальний розмір зображення — {MAX_FILE_SIZE_MB} МБ.")
        return

    cleanup_pending()

    task_id = uuid.uuid4().hex
    tmpdir = tempfile.mkdtemp(prefix="uniqbot_")
    ext = get_ext(original_name, ".jpg")
    input_path = str(Path(tmpdir) / f"input{ext}")

    await download_telegram_file(file_id, input_path)

    PENDING_TASKS[task_id] = {
        "type": "image",
        "tmpdir": tmpdir,
        "input_path": input_path,
        "original_name": original_name or f"image{ext}",
        "created_at": time.time(),
        "user_id": message.from_user.id,
    }

    await message.answer(
        "Обери режим для зображення:",
        reply_markup=image_kb(task_id),
    )


# =========================
# COMMANDS
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    if ALLOWED_USERS and not user_allowed(message.from_user.id):
        await message.answer(
            "👋 Надішли мені свій <b>/id</b> адміну для доступу."
        )
        return

    await message.answer(
        "👋 <b>Унікалізатор готовий</b>\n\n"
        "Надсилай відео або фото.\n"
        "Чекаю на твої файли 👇"
    )


@dp.message(Command("id"))
async def cmd_id(message: Message):
    await message.answer(
        f"🆔 <b>Твій Telegram ID:</b>\n<code>{message.from_user.id}</code>"
    )


# =========================
# MEDIA HANDLERS
# =========================
@dp.message(F.video)
async def handle_video(message: Message):
    if not user_allowed(message.from_user.id):
        await message.answer("⛔️ Немає доступу. Надішли /id адміну.")
        return

    await create_task_from_video(
        message=message,
        file_id=message.video.file_id,
        original_name=message.video.file_name or "video.mp4",
        file_size=message.video.file_size or 0,
    )


@dp.message(F.photo)
async def handle_photo(message: Message):
    if not user_allowed(message.from_user.id):
        await message.answer("⛔️ Немає доступу. Надішли /id адміну.")
        return

    largest = message.photo[-1]
    await create_task_from_image(
        message=message,
        file_id=largest.file_id,
        original_name="photo.jpg",
        file_size=largest.file_size or 0,
    )


@dp.message(F.document)
async def handle_document(message: Message):
    if not user_allowed(message.from_user.id):
        await message.answer("⛔️ Немає доступу. Надішли /id адміну.")
        return

    if is_video_document(message):
        await create_task_from_video(
            message=message,
            file_id=message.document.file_id,
            original_name=message.document.file_name or "video.mp4",
            file_size=message.document.file_size or 0,
        )
        return

    if is_image_document(message):
        await create_task_from_image(
            message=message,
            file_id=message.document.file_id,
            original_name=message.document.file_name or "image.png",
            file_size=message.document.file_size or 0,
        )
        return

    await message.answer(
        "Надішли мені відео або фото.\n\n"
        f"🎬 Відео — до {MAX_FILE_SIZE_MB} МБ\n"
        "🖼 JPG / PNG / WEBP"
    )


@dp.message()
async def fallback_handler(message: Message):
    if message.text in ("/start", "/id"):
        return

    await message.answer(
        "Надішли мені відео або фото.\n\n"
        f"🎬 Відео — до {MAX_FILE_SIZE_MB} МБ\n"
        "🖼 JPG / PNG / WEBP"
    )


# =========================
# CALLBACKS
# =========================
@dp.callback_query(F.data.startswith("proc|"))
async def process_callback(callback: CallbackQuery):
    try:
        _, mode, task_id = callback.data.split("|", 2)
    except Exception:
        await callback.answer("Некоректна дія", show_alert=True)
        return

    task = PENDING_TASKS.get(task_id)
    if not task:
        await callback.answer("Задача вже протухла або не знайдена", show_alert=True)
        return

    if callback.from_user.id != task["user_id"]:
        await callback.answer("Це не твоя задача", show_alert=True)
        return

    await callback.answer("Обробляю...")
    status_msg = await callback.message.answer("⏳ Обробляю файл...")

    tmpdir = task["tmpdir"]
    input_path = task["input_path"]
    original_name = task["original_name"]

    try:
        if task["type"] == "video":
            output_path = str(Path(tmpdir) / "result.mp4")

            if mode == "vf":
                await process_video_fast(input_path, output_path)
                out_name = f"unique_fast_{Path(original_name).stem}.mp4"

            elif mode == "vd":
                await process_video_deep(input_path, output_path)
                out_name = f"unique_deep_{Path(original_name).stem}.mp4"

            elif mode == "vb":
                await process_video_preview_blur(input_path, output_path, tmpdir)
                out_name = f"unique_preview_{Path(original_name).stem}.mp4"

            else:
                raise RuntimeError("Невідомий режим для відео")

            await callback.message.answer_document(
                document=FSInputFile(output_path, filename=out_name),
                caption="✅ Готово",
            )

        elif task["type"] == "image":
            ext = Path(original_name).suffix.lower()
            if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
                ext = ".png"

            output_path = str(Path(tmpdir) / f"result{ext}")

            if mode == "im":
                await process_image_metadata_only(input_path, output_path)
                out_name = f"image_meta_{Path(original_name).stem}{ext}"

            elif mode == "ip":
                await process_image_pixels(input_path, output_path)
                out_name = f"image_pixels_{Path(original_name).stem}{ext}"

            else:
                raise RuntimeError("Невідомий режим для зображення")

            await callback.message.answer_document(
                document=FSInputFile(output_path, filename=out_name),
                caption="✅ Готово",
            )

        else:
            raise RuntimeError("Невідомий тип задачі")

        await status_msg.edit_text("✅ Готово")

    except Exception as e:
        await status_msg.edit_text(f"❌ Помилка:\n<code>{str(e)[:3500]}</code>")

    finally:
        safe_rmtree(tmpdir)
        PENDING_TASKS.pop(task_id, None)


# =========================
# MAIN
# =========================
async def main():
    print("====================================")
    print("MEDIA UNIQUE BOT")
    print("====================================")
    print(f"Max file: {MAX_FILE_SIZE_MB} MB")
    print(f"Telegram API: {TELEGRAM_API_URL or 'https://api.telegram.org'}")
    print(f"File server: {FILE_SERVER_URL or '-'}")
    print("====================================")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
