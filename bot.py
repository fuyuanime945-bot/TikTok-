
# bot.py
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration ----------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else None

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "45"))
MAX_CONCURRENT_DOWNLOADS = int(
    os.getenv("MAX_CONCURRENT_DOWNLOADS", "2")
)
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "120"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "bot.sqlite3")
DOWNLOAD_API_URL = os.getenv("DOWNLOAD_API_URL", "").strip()
DOWNLOAD_API_TOKEN = os.getenv("DOWNLOAD_API_TOKEN", "").strip()
CAPTION_SOURCE_URL = (
    os.getenv("CAPTION_SOURCE_URL", "false").lower() == "true"
)

MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_URL_LENGTH = 2048
MAX_RETRIES = 3

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is missing. Configure your .env file.")

if MAX_FILE_SIZE_MB < 1 or MAX_CONCURRENT_DOWNLOADS < 1:
    raise SystemExit("File-size and concurrency limits must be positive.")

if DOWNLOAD_TIMEOUT < 5:
    raise SystemExit("DOWNLOAD_TIMEOUT must be at least 5 seconds.")

if DOWNLOAD_API_URL:
    parsed_api = urlsplit(DOWNLOAD_API_URL)
    if (
        parsed_api.scheme != "https"
        or not parsed_api.hostname
        or parsed_api.username
        or parsed_api.password
        or parsed_api.query
        or parsed_api.fragment
    ):
        raise SystemExit(
            "DOWNLOAD_API_URL must be a fixed HTTPS endpoint "
            "without credentials, query parameters, or fragments."
        )

# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("tiktok_bot")

# Never log tokens, provider response bodies, or complete user URLs.
router = Router()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
active_urls: set[str] = set()
active_lock = asyncio.Lock()
started_at = time.monotonic()
http_session: aiohttp.ClientSession | None = None
db: aiosqlite.Connection | None = None


# ---------- UI ----------

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📖 Help / အကူအညီ",
                    callback_data="help",
                ),
                InlineKeyboardButton(
                    text="ℹ️ About",
                    callback_data="about",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📊 Status",
                    callback_data="status",
                )
            ],
        ]
    )


WELCOME = (
    "မင်္ဂလာပါ 👋 Welcome!\n\n"
    "🎬 TikTok video link ကို ဒီမှာပို့လိုက်ပါ။\n"
    "Send a supported public TikTok video URL to get started.\n\n"
    "⚠️ Download availability depends on the configured provider. "
    "Please respect creators' rights and platform rules."
)

HELP_TEXT = (
    "📖 **အသုံးပြုပုံ / How to use**\n\n"
    "1. Public TikTok video link ကို copy လုပ်ပါ။\n"
    "2. ဒီ bot ထဲကို URL ပို့ပါ။\n"
    "3. Provider က support လုပ်ရင် video ကို ပြန်ပို့ပေးပါမယ်။\n\n"
    "• Public video links only\n"
    "• Private videos, login bypass, and CAPTCHA bypass မလုပ်ပါ။\n"
    "• Large files and unsupported links may fail.\n"
    "• Only download or share content you have permission to use."
)

ABOUT_TEXT = (
    "ℹ️ **TikTok Downloader Bot**\n\n"
    "Python 3 + aiogram 3 + SQLite\n"
    "Asynchronous processing, configurable limits, and temporary "
    "file cleanup.\n\n"
    "A compatible authorized download provider is required."
)


# ---------- URL validation ----------

TIKTOK_HOSTS = {
    "tiktok.com",
    "www.tiktok.com",
    "m.tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
}


def validate_tiktok_url(raw: str) -> str | None:
    """Return a normalized URL or None. Never fetch user URLs directly."""
    raw = raw.strip()

    if not raw or len(raw) > MAX_URL_LENGTH:
        return None

    # Reject whitespace, control characters and embedded credentials.
    if re.search(r"[\s\x00-\x1f\x7f]", raw):
        return None

    candidate = raw
    if candidate.startswith("www.tiktok.com/"):
        candidate = "https://" + candidate

    try:
        parsed = urlsplit(candidate)
        host = (parsed.hostname or "").lower().rstrip(".")
        port = parsed.port
    except ValueError:
        return None

    if parsed.scheme != "https":
        return None

    if host not in TIKTOK_HOSTS:
        return None

    if parsed.username or parsed.password:
        return None

    if port not in (None, 443):
        return None

    if not parsed.path or parsed.path == "/":
        return None

    # Do not follow arbitrary URLs or permit non-TikTok hosts.
    return candidate


def url_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


# ---------- SQLite ----------

async def init_db() -> None:
    global db
    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row

    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")

    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            requests INTEGER NOT NULL DEFAULT 0,
            successes INTEGER NOT NULL DEFAULT 0,
            failures INTEGER NOT NULL DEFAULT 0
        );

        INSERT OR IGNORE INTO stats(id, requests, successes, failures)
        VALUES (1, 0, 0, 0);
        """
    )
    await db.commit()


async def touch_user(user_id: int) -> None:
    assert db is not None
    now = int(time.time())
    await db.execute(
        """
        INSERT INTO users(user_id, first_seen, last_seen)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen
        """,
        (user_id, now, now),
    )
    await db.commit()


async def increment_stat(name: str) -> None:
    assert db is not None
    allowed = {"requests", "successes", "failures"}
    if name not in allowed:
        raise ValueError("Invalid statistic")
    await db.execute(f"UPDATE stats SET {name} = {name} + 1 WHERE id=1")
    await db.commit()


async def read_stats() -> dict[str, int]:
    assert db is not None
    cursor = await db.execute(
        """
        SELECT requests, successes, failures,
               (SELECT COUNT(*) FROM users) AS users
        FROM stats WHERE id=1
        """
    )
    row = await cursor.fetchone()
    return dict(row) if row else {
        "requests": 0, "successes": 0, "failures": 0, "users": 0
    }


# ---------- Provider adapter ----------

class DownloadError(Exception):
    """Safe-to-display download error."""


class ProviderUnavailable(DownloadError):
    pass


class ProviderRejected(DownloadError):
    pass


class FileTooLarge(DownloadError):
    pass


class DownloadTimeout(DownloadError):
    pass


async def download_from_provider(url: str, output: Path) -> int:
    """
    Provider contract:
      POST configured HTTPS endpoint with JSON {"url": "<TikTok URL>"}.
      Optional Authorization: Bearer <DOWNLOAD_API_TOKEN>.
      Success: raw video bytes, HTTP 200, Content-Type video/mp4
               or video/webm.
      Failure: non-2xx status.

    The bot never requests a user-provided media URL directly and never
    follows provider redirects. Configure only a trusted provider.
    """
    if not DOWNLOAD_API_URL:
        raise ProviderUnavailable(
            "Downloader provider is not configured."
        )

    assert http_session is not None

    headers = {"Accept": "video/mp4, video/webm"}
    if DOWNLOAD_API_TOKEN:
        headers["Authorization"] = f"Bearer {DOWNLOAD_API_TOKEN}"

    timeout = aiohttp.ClientTimeout(
        total=DOWNLOAD_TIMEOUT,
        connect=min(20, DOWNLOAD_TIMEOUT),
        sock_read=DOWNLOAD_TIMEOUT,
    )

    for attempt in range(MAX_RETRIES):
        try:
            async with http_session.post(
                DOWNLOAD_API_URL,
                json={"url": url},
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
            ) as response:
                if response.status in (429, 500, 502, 503, 504):
                    if attempt + 1 < MAX_RETRIES:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise ProviderUnavailable(
                        "Provider is temporarily unavailable."
                    )

                if response.status != 200:
                    # Do not expose provider response bodies to users.
                    raise ProviderRejected(
                        "Provider could not process this video."
                    )

                content_type = response.headers.get(
                    "Content-Type", ""
                ).split(";")[0].strip().lower()

                if content_type not in {
                    "video/mp4",
                    "video/webm",
                    "application/octet-stream",
                }:
                    raise ProviderRejected(
                        "Provider returned an unsupported response."
                    )

                declared_size = response.content_length
                if (
                    declared_size is not None
                    and declared_size > MAX_FILE_SIZE
                ):
                    raise FileTooLarge(
                        f"Video exceeds the {MAX_FILE_SIZE_MB} MB limit."
                    )

                size = 0
                with output.open("wb") as file:
                    async for chunk in response.content.iter_chunked(
                        64 * 1024
                    ):
                        size += len(chunk)
                        if size > MAX_FILE_SIZE:
                            raise FileTooLarge(
                                f"Video exceeds the {MAX_FILE_SIZE_MB} MB limit."
                            )
                        file.write(chunk)

                # Reject empty/non-video-looking responses.
                if size < 1024:
                    raise ProviderRejected(
                        "The returned video is empty or invalid."
                    )

                with output.open("rb") as file:
                    signature = file.read(16)

                is_mp4 = (
                    len(signature) >= 8
                    and signature[4:8] == b"ftyp"
                )
                is_webm = signature.startswith(b"\x1aE\xdf\xa3")

                if not (is_mp4 or is_webm):
                    raise ProviderRejected(
                        "Provider did not return a valid MP4 or WebM file."
                    )

                return size

        except asyncio.CancelledError:
            raise
        except (FileTooLarge, ProviderRejected):
            raise
        except asyncio.TimeoutError as exc:
            if attempt + 1 < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
                continue
            raise DownloadTimeout(
                "The download timed out. Please try again later."
            ) from exc
        except (aiohttp.ClientError, OSError) as exc:
            if attempt + 1 < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
                continue
            raise ProviderUnavailable(
                "A network error occurred. Please try again later."
            ) from exc

    raise ProviderUnavailable("Provider is temporarily unavailable.")


# ---------- Commands ----------

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if message.from_user:
        await touch_user(message.from_user.id)
    await message.answer(WELCOME, reply_markup=main_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT, reply_markup=main_keyboard())


@router.message(Command("about"))
async def cmd_about(message: Message) -> None:
    await message.answer(ABOUT_TEXT)


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    stats = await read_stats()
    uptime = int(time.monotonic() - started_at)
    hours, remainder = divmod(uptime, 3600)
    minutes = remainder // 60

    await message.answer(
        "📊 **Bot Status**\n\n"
        f"• Provider configured: {'Yes' if DOWNLOAD_API_URL else 'No'}\n"
        f"• Active downloads: {len(active_urls)}\n"
        f"• Concurrent limit: {MAX_CONCURRENT_DOWNLOADS}\n"
        f"• File limit: {MAX_FILE_SIZE_MB} MB\n"
        f"• Requests: {stats['requests']}\n"
        f"• Successful: {stats['successes']}\n"
        f"• Failed: {stats['failures']}\n"
        f"• Users seen: {stats['users']}\n"
        f"• Uptime: {hours}h {minutes}m"
    )


@router.message(Command("adminstats"))
async def cmd_adminstats(message: Message) -> None:
    if not message.from_user or ADMIN_ID is None:
        return
    if message.from_user.id != ADMIN_ID:
        return

    stats = await read_stats()
    await message.answer(
        "🔐 **Admin Statistics**\n\n"
        f"Users: {stats['users']}\n"
        f"Requests: {stats['requests']}\n"
        f"Successful: {stats['successes']}\n"
        f"Failed: {stats['failures']}\n"
        f"Active jobs: {len(active_urls)}"
    )


@router.callback_query(F.data.in_({"help", "about", "status"}))
async def callbacks(callback: CallbackQuery) -> None:
    if callback.data == "help":
        text = HELP_TEXT
    elif callback.data == "about":
        text = ABOUT_TEXT
    else:
        stats = await read_stats()
        text = (
            "📊 **Bot Status**\n\n"
            f"Requests: {stats['requests']}\n"
            f"Successful: {stats['successes']}\n"
            f"Failed: {stats['failures']}\n"
            f"Users: {stats['users']}\n"
            f"Provider configured: {'Yes' if DOWNLOAD_API_URL else 'No'}"
        )

    await callback.answer()
    if callback.message:
        await callback.message.answer(text, reply_markup=main_keyboard())


# ---------- URL processing ----------

def user_error(exc: Exception) -> str:
    if isinstance(exc, FileTooLarge):
        return f"📦 {exc}"
    if isinstance(exc, DownloadTimeout):
        return "⏳ Download timed out. Please try again later."
    if isinstance(exc, ProviderUnavailable):
        return "🌐 Service unavailable. Please try again later."
    if isinstance(exc, ProviderRejected):
        return (
            "⚠️ This video is unsupported or unavailable.\n"
            "The video may be private, deleted, restricted, or "
            "unsupported by the provider."
        )
    if isinstance(exc, TelegramNetworkError):
        return "📡 Telegram network error. Please try again later."
    if isinstance(exc, TelegramAPIError):
        return (
            "📤 Telegram could not send this video. "
            "It may exceed Telegram's upload limits."
        )
    return "❌ Something went wrong. Please try again later."


@router.message(F.text)
async def handle_text(message: Message, bot: Bot) -> None:
    if not message.from_user or not message.text:
        return

    raw = message.text.strip()

    # Commands are handled by command handlers; don't treat them as URLs.
    if raw.startswith("/"):
        return

    url = validate_tiktok_url(raw)
    if not url:
        if "tiktok.com" in raw.lower():
            await message.answer(
                "❌ Invalid or unsupported TikTok link. "
                "Please send a full HTTPS video URL."
            )
        return

    await touch_user(message.from_user.id)
    await increment_stat("requests")

    key = url_key(url)
    async with active_lock:
        if key in active_urls:
            await message.answer(
                "⏳ This URL is already being processed. "
                "Please wait for the current request."
            )
            return
        active_urls.add(key)

    status: Message | None = None
    success = False

    try:
        async with semaphore:
            status = await message.answer("🔎 Checking link...")
            await status.edit_text("⬇️ Processing video...")

            with tempfile.TemporaryDirectory(
                prefix="ttdl_"
            ) as temp_dir:
                path = Path(temp_dir) / "video.mp4"

                await download_from_provider(url, path)

                await status.edit_text("📤 Uploading to Telegram...")
                await bot.send_chat_action(
                    chat_id=message.chat.id,
                    action=ChatAction.UPLOAD_VIDEO,
                )

                caption = "✅ Download completed!"
                if CAPTION_SOURCE_URL:
                    caption += f"\nSource: {url}"

                try:
                    with path.open("rb") as video:
                        await message.answer_video(
                            video=video,
                            caption=caption,
                            supports_streaming=True,
                            request_timeout=DOWNLOAD_TIMEOUT,
                        )
                except TelegramAPIError:
                    # Some providers return a file Telegram won't accept
                    # as a video; try a document upload once.
                    with path.open("rb") as document:
                        await message.answer_document(
                            document=document,
                            caption=caption,
                            request_timeout=DOWNLOAD_TIMEOUT,
                        )

                success = True

            with suppress(TelegramAPIError, TelegramNetworkError):
                await status.edit_text("✅ Download completed!")

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning(
            "Request failed (%s); user_id=%s",
            type(exc).__name__,
            message.from_user.id,
        )
        if status:
            with suppress(TelegramAPIError, TelegramNetworkError):
                await status.edit_text(user_error(exc))
        else:
            with suppress(TelegramAPIError, TelegramNetworkError):
                await message.answer(user_error(exc))
    finally:
        await increment_stat("successes" if success else "failures")
        async with active_lock:
            active_urls.discard(key)


# ---------- Startup ----------

async def main() -> None:
    global http_session

    await init_db()

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_DOWNLOADS,
        ttl_dns_cache=300,
    )
    http_session = aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": "Authorized-TikTok-Downloader-Bot/1.0"},
    )

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        log.info("Bot starting. Provider configured: %s",
                 bool(DOWNLOAD_API_URL))
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
        )
    finally:
        await bot.session.close()
        if http_session:
            await http_session.close()
        if db:
            await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
