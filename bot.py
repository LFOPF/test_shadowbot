import os
import asyncio
import logging
import re
import json
from typing import Optional, Set, List, Dict, Any
from urllib.parse import urlparse
from collections import deque
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    wait_fixed,
)
from tenacity import before_sleep_log, after_log
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import redis.asyncio as redis
from playwright.async_api import async_playwright, Playwright, BrowserContext, Page, Browser, Error as PlaywrightError
from bs4 import BeautifulSoup
import aiohttp

# ======================== НАСТРОЙКИ ========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
TELEGRAPH_ACCESS_TOKEN = os.getenv("TELEGRAPH_ACCESS_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")
ADMIN_ID = os.getenv("ADMIN_ID")

if not all([BOT_TOKEN, OPENROUTER_API_KEY, TELEGRAPH_ACCESS_TOKEN, REDIS_URL]):
    raise ValueError("Не заданы все обязательные переменные окружения")

TARGET_URL = "https://ranobes.net/chapters/1205249/"
CHECK_INTERVAL = 10800          # 3 часа
IDLE_TIMEOUT = 1200             # 20 минут бездействия → закрываем браузер
SITE_URL = "https://t.me/SHDSlaveBot"
SITE_NAME = "ShadowSlaveTranslator"
MAX_PAGES = 120
CHAPTERS_PER_PAGE = 25
TELEGRAPH_TITLE_MAX_LENGTH = 200
NOVEL_ID = "1205249"
GLOSSARY_FILE = "glossary.txt"  # файл с глоссарием

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RETRY_WEB = dict(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.2, min=3, max=75),
    retry=retry_if_exception_type((
        asyncio.TimeoutError, ConnectionError, OSError,
        aiohttp.ClientError, aiohttp.ClientResponseError, PlaywrightError
    )),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.INFO),
)

RETRY_API = dict(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=4, max=90),
    retry=retry_if_exception_type((aiohttp.ClientError, ConnectionError, asyncio.TimeoutError, Exception)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.INFO),
)

log_buffer = deque(maxlen=100)

class LogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        log_buffer.append(log_entry)

logging.getLogger().addHandler(LogHandler())
logging.getLogger().setLevel(logging.INFO)

# ======================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ========================
redis_client: Optional[redis.Redis] = None
bot: Optional[Bot] = None
playwright_instance: Optional[Playwright] = None
browser: Optional[Browser] = None
browser_context: Optional[BrowserContext] = None
shared_page: Optional[Page] = None

_last_activity_time = 0.0
_browser_keepalive_task: Optional[asyncio.Task] = None

# ======================== FSM СОСТОЯНИЯ ========================
class ChapterSelection(StatesGroup):
    waiting_for_chapter = State()

class AdminActions(StatesGroup):
    waiting_for_user_id = State()

# ======================== СИСТЕМНЫЙ ПРОМПТ ========================
SYSTEM_PROMPT = """Ты профессиональный переводчик ранобэ с английского на русский язык. 
Твоя задача — качественно переводить главы веб-новеллы Shadow Slave, сохраняя стиль, атмосферу и терминологию.

Правила перевода:
1. Используй естественный, живой русский язык, подходящий для фэнтези/литРПГ.
2. Сохраняй эмоциональную окраску и интонацию персонажей.
3. Имена собственные и ключевые термины переводи только согласно глоссарию (если он дан).
4. Диалоги оформляй кавычками «ёлочками», мысли — курсивом *вот так*.
5. Системные сообщения, уведомления, подсказки — выделяй [квадратными скобками] или <i>курсивом</i>.
6. Не добавляй от себя пояснения в скобках, если это не оговорено.
7. Сохраняй структуру: абзацы, переносы строк, выделения.
8. Не сокращай и не расширяй текст без необходимости."""

USER_PROMPT_TEMPLATE = """Переведи следующую главу на русский язык, строго следуя правилам выше:

{text}

Верни ТОЛЬКО переведённый текст без каких-либо комментариев, предисловий, послесловий и номеров глав."""

# ======================== ГЛОССАРИЙ ========================

async def load_glossary_to_redis():
    """Загружает глоссарий из файла в Redis один раз при старте"""
    try:
        with open(GLOSSARY_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()

        await redis_client.delete("glossary:terms")  # чистим старое

        count = 0
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = [part.strip() for part in line.split("=", 1)]
                await redis_client.hset("glossary:terms", key, value)
                count += 1
        
        logger.info(f"Глоссарий загружен из {GLOSSARY_FILE} — {count} записей")
    except FileNotFoundError:
        logger.warning(f"Файл глоссария {GLOSSARY_FILE} не найден — глоссарий пуст")
    except Exception as e:
        logger.error(f"Ошибка загрузки глоссария: {e}")


async def build_glossary_prompt(text: str) -> str:
    """Формирует блок глоссария только с терминами, которые есть в текущем тексте"""
    if not text:
        return ""

    try:
        terms = await redis_client.hgetall("glossary:terms")
        if not terms:
            return ""

        relevant = []
        text_lower = text.lower()

        for en_bytes, ru_bytes in terms.items():
            en = en_bytes.decode("utf-8")
            if en.lower() in text_lower:
                ru = ru_bytes.decode("utf-8")
                relevant.append(f"{en} = {ru}")

        if not relevant:
            return ""

        return (
            "\n\n=== СТРОГИЙ ГЛОССАРИЙ (ОБЯЗАТЕЛЬНО ИСПОЛЬЗУЙ ТОЛЬКО ЭТИ ПЕРЕВОДЫ) ===\n" +
            "\n".join(relevant) +
            "\n\nНикогда не придумывай свои варианты для этих терминов и фраз. "
            "Используй их в точности, даже если кажется, что перевод можно улучшить."
        )
    except Exception as e:
        logger.error(f"Ошибка при формировании глоссария: {e}")
        return ""

# ======================== БРАУЗЕР + IDLE-ТАЙМЕР ========================

async def launch_browser():
    global playwright_instance, browser, browser_context, shared_page

    if browser is not None:
        return

    logger.info("Запуск браузера Playwright...")
    try:
        playwright_instance = await async_playwright().start()

        browser = await playwright_instance.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--single-process',
                '--disable-extensions',
            ]
        )

        browser_context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport={'width': 1280, 'height': 720},
            ignore_https_errors=True,
        )

        shared_page = await browser_context.new_page()
        await shared_page.set_viewport_size({"width": 1280, "height": 720})
        await shared_page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        })
        await shared_page.route("**/*", lambda route: route.abort() if _should_block(route) else route.continue_())

        logger.info("Браузер запущен и страница создана")
    except Exception as e:
        logger.exception("Критическая ошибка при запуске браузера")
        raise


async def close_browser():
    global playwright_instance, browser, browser_context, shared_page, _browser_keepalive_task

    if browser is None:
        return

    logger.info("Закрываем браузер по таймеру бездействия")
    try:
        if browser_context:
            await browser_context.close()
        if browser:
            await browser.close()
        await asyncio.sleep(0.3)
    except PlaywrightError as e:
        err_str = str(e)
        if "Target page, context or browser has been closed" in err_str:
            logger.debug("Ожидаемое предупреждение Playwright при закрытии")
        else:
            logger.warning(f"Ошибка Playwright при закрытии: {e}")
    except Exception as e:
        logger.warning(f"Неожиданная ошибка при закрытии браузера: {e}")
    finally:
        if playwright_instance:
            try:
                await playwright_instance.stop()
            except Exception:
                pass
        playwright_instance = None
        browser = None
        browser_context = None
        shared_page = None
        if _browser_keepalive_task:
            _browser_keepalive_task.cancel()
            _browser_keepalive_task = None


async def keep_browser_alive():
    global _last_activity_time, _browser_keepalive_task

    while True:
        await asyncio.sleep(60)
        if browser is None:
            continue

        idle_time = asyncio.get_event_loop().time() - _last_activity_time
        if idle_time > IDLE_TIMEOUT:
            logger.info(f"Браузер бездействовал {idle_time:.0f} сек → закрываем")
            await close_browser()
            break


def touch_browser_activity():
    global _last_activity_time, _browser_keepalive_task

    _last_activity_time = asyncio.get_event_loop().time()

    if _browser_keepalive_task is None or _browser_keepalive_task.done():
        _browser_keepalive_task = asyncio.create_task(keep_browser_alive())


def _should_block(route) -> bool:
    req = route.request
    res_type = req.resource_type
    url = req.url.lower()

    if res_type in ("image", "stylesheet", "font", "media", "other"):
        return True

    ad_patterns = [
        "googleads", "doubleclick", "adservice", "adserver", "banner",
        "yandex.ru/ads", "rambler", "mgid", "cointraffic", "propellerads"
    ]
    if any(p in url for p in ad_patterns):
        return True

    return False


async def get_shared_page() -> Page:
    global shared_page

    touch_browser_activity()

    if browser is None:
        await launch_browser()

    if shared_page is not None and not shared_page.is_closed():
        return shared_page

    logger.info("Создаём новую общую страницу")
    shared_page = await browser_context.new_page()
    await shared_page.set_viewport_size({"width": 1280, "height": 720})
    await shared_page.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
    })
    await shared_page.route("**/*", lambda route: route.abort() if _should_block(route) else route.continue_())
    return shared_page

# ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================

def extract_chapter_id(text: str) -> Optional[str]:
    text = text.strip()
    patterns = [
        r'(?:Chapter|Ch\.?|Глава)\s*[:.\-–—]?\s*(\d+)',
        r'(\d{4,})',
    ]
    for pat in patterns:
        if m := re.search(pat, text, re.IGNORECASE):
            return m.group(1)
    return None


def clean_title(raw_title: str) -> str:
    return re.sub(
        r'\s*\d+\s*(?:minute|hour|day|week|month|year)s?\s+ago$',
        '',
        raw_title,
        flags=re.IGNORECASE
    ).strip()


def clean_title_for_telegraph(title: str) -> str:
    title = re.sub(r'\s*\([^)]*\)$', '', title).strip()
    if len(title) > TELEGRAPH_TITLE_MAX_LENGTH:
        title = title[:TELEGRAPH_TITLE_MAX_LENGTH - 3] + "..."
    return title


def text_to_html(text: str) -> str:
    paragraphs = text.split('\n\n')
    return ''.join(f"<p>{p.replace('\n', '<br>')}</p>" for p in paragraphs if p.strip())


def get_page_url(page_num: int) -> str:
    if page_num == 1:
        return TARGET_URL
    base = TARGET_URL.rstrip('/')
    return f"{base}/page/{page_num}/"


async def safe_edit_text(
    message: types.Message,
    text: str,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[InlineKeyboardMarkup | ReplyKeyboardMarkup] = None
) -> None:
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        logger.warning(f"TelegramBadRequest при edit: {e}")
    except Exception as e:
        logger.warning(f"Ошибка редактирования сообщения: {e}")


async def safe_delete(message: Optional[types.Message]) -> None:
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def get_cached_telegraph(chapter_id: str) -> Optional[str]:
    try:
        value = await redis_client.hget("telegraph_urls", chapter_id)
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_cached_telegraph error: {e}")
        return None


async def save_telegraph_url(chapter_id: str, url: str):
    try:
        await redis_client.hset("telegraph_urls", chapter_id, url)
        logger.info(f"Сохранён URL для главы {chapter_id}: {url}")
    except Exception as e:
        logger.error(f"save_telegraph_url error: {e}")


async def get_cached_title(chapter_id: str) -> Optional[str]:
    try:
        value = await redis_client.hget("chapter_titles", chapter_id)
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_cached_title error: {e}")
        return None


async def save_cached_title(chapter_id: str, title: str):
    try:
        await redis_client.hset("chapter_titles", chapter_id, title)
    except Exception as e:
        logger.error(f"save_cached_title error: {e}")


async def load_subscribers() -> Set[int]:
    try:
        data = await redis_client.get("subscribers")
        if data:
            return set(json.loads(data.decode()))
        return set()
    except Exception as e:
        logger.error(f"load_subscribers error: {e}")
        return set()


async def save_subscribers(subs: Set[int]):
    try:
        await redis_client.set("subscribers", json.dumps(list(subs)))
    except Exception as e:
        logger.error(f"save_subscribers error: {e}")


async def get_last_chapter() -> Optional[str]:
    try:
        value = await redis_client.get("last_chapter")
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_last_chapter error: {e}")
        return None


async def save_last_chapter(ch_id: str):
    try:
        await redis_client.set("last_chapter", ch_id)
    except Exception as e:
        logger.error(f"save_last_chapter error: {e}")


@retry(**RETRY_WEB)
async def fetch_html(url: str) -> Optional[str]:
    page = await get_shared_page()
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        if response and response.status >= 400:
            logger.warning(f"HTTP {response.status} на {url}")
            return None
        content = await page.content()
        return content
    except Exception as e:
        logger.error(f"Ошибка загрузки страницы {url}: {e}")
        return None


def parse_chapters(html: str) -> List[Dict[str, str]]:
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    chapters = []
    items = soup.select("ul.chapter-list li a")
    for item in items:
        title = clean_title(item.get_text(strip=True))
        href = item.get("href", "")
        if not href:
            continue
        chapter_id = extract_chapter_id(title)
        if not chapter_id:
            continue
        full_url = urlparse(href)._replace(scheme="https", netloc="ranobes.net").geturl()
        chapters.append({
            "id": chapter_id,
            "title": title,
            "url": full_url
        })
    return chapters


@retry(**RETRY_WEB)
async def get_chapter_content(chapter_url: str) -> Optional[str]:
    page = await get_shared_page()
    try:
        await page.goto(chapter_url, wait_until="domcontentloaded", timeout=60000)
        content = await page.query_selector("div#chapter-content")
        if not content:
            logger.warning(f"Не найден #chapter-content на {chapter_url}")
            return None
        text = await content.inner_text()
        return text.strip()
    except Exception as e:
        logger.error(f"Ошибка парсинга главы {chapter_url}: {e}")
        return None


@retry(**RETRY_API)
async def translate_text(text: str) -> str:
    if len(text) > 120000:
        logger.warning("Текст слишком длинный, обрезаем")
        text = text[:120000] + "\n... [обрезано]"

    glossary_block = await build_glossary_prompt(text)

    dynamic_system = SYSTEM_PROMPT + glossary_block

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json"
    }

    payload = {
        "model": "google/gemini-2.5-flash-lite",
        "messages": [
            {"role": "system", "content": dynamic_system},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(text=text)}
        ],
        "temperature": 0.90,
        "top_p": 0.92,
        "presence_penalty": 0.25,
        "frequency_penalty": 0.15,
        "max_tokens": 8000
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=130
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                return content.strip() if content else "[Ошибка: пустой ответ]"

            elif resp.status == 429:
                logger.warning("Rate limit от OpenRouter")
                raise aiohttp.ClientError("Rate limit")

            else:
                text_resp = await resp.text()
                logger.error(f"OpenRouter {resp.status}: {text_resp[:500]}")
                raise aiohttp.ClientError(f"HTTP {resp.status}")


async def create_telegraph_page(title: str, content: str) -> Optional[str]:
    if not TELEGRAPH_ACCESS_TOKEN:
        logger.error("TELEGRAPH_ACCESS_TOKEN не задан")
        return None

    clean_title = clean_title_for_telegraph(title)
    html_content = text_to_html(content)

    payload = {
        "access_token": TELEGRAPH_ACCESS_TOKEN,
        "title": clean_title,
        "content": json.dumps([{"tag": "p", "children": html_content}]),
        "author_name": "Shadow Slave Translator",
        "author_url": "https://t.me/SHDSlaveBot"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.telegra.ph/createPage",
            json=payload,
            timeout=25
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("ok"):
                    return data["result"]["url"]
            logger.error(f"Telegraph ошибка {resp.status}: {await resp.text()}")
            return None


async def find_chapter_by_number(chapter_num: int) -> Optional[Dict[str, str]]:
    for page in range(1, MAX_PAGES + 1):
        url = get_page_url(page)
        html = await fetch_html(url)
        if not html:
            continue
        chapters = parse_chapters(html)
        for ch in chapters:
            if int(ch['id']) == chapter_num:
                return ch
        await asyncio.sleep(1.2)
    return None


async def process_chapter_translation(chapter: Dict[str, str]) -> tuple[Optional[str], bool]:
    try:
        raw_text = await get_chapter_content(chapter["url"])
        if not raw_text or len(raw_text) < 300:
            logger.warning(f"Слишком короткий или пустой контент главы {chapter['id']}")
            return None, False

        translated = await translate_text(raw_text)
        if not translated or len(translated) < 400:
            logger.warning(f"Перевод главы {chapter['id']} получился пустым или слишком коротким")
            return None, False

        telegraph_url = await create_telegraph_page(chapter["title"], translated)
        if not telegraph_url:
            return None, False

        await save_telegraph_url(chapter["id"], telegraph_url)
        await save_cached_title(chapter["id"], chapter["title"])

        logger.info(f"Успешно переведена и загружена глава {chapter['id']} → {telegraph_url}")
        return telegraph_url, True

    except Exception as e:
        logger.exception(f"Ошибка обработки главы {chapter.get('id', '?')}: {e}")
        return None, False


# ======================== ПОДПИСКА / ЗАКЛАДКИ ========================

async def is_subscriber(user_id: int) -> bool:
    subs = await load_subscribers()
    return user_id in subs


async def add_subscriber(user_id: int):
    subs = await load_subscribers()
    subs.add(user_id)
    await save_subscribers(subs)


async def remove_subscriber(user_id: int) -> bool:
    subs = await load_subscribers()
    if user_id in subs:
        subs.remove(user_id)
        await save_subscribers(subs)
        return True
    return False


async def get_user_bookmark(user_id: int) -> Optional[str]:
    try:
        key = f"bookmark:{user_id}"
        value = await redis_client.get(key)
        return value.decode() if value else None
    except Exception:
        return None


async def save_user_bookmark(user_id: int, chapter_id: str):
    try:
        key = f"bookmark:{user_id}"
        await redis_client.set(key, chapter_id, ex=2592000)  # 30 дней
    except Exception as e:
        logger.error(f"Ошибка сохранения закладки: {e}")


async def notify_all_subscribers(text: str, parse_mode: str = None):
    subs = await load_subscribers()
    for uid in subs:
        try:
            await bot.send_message(uid, text, parse_mode=parse_mode, disable_web_page_preview=True)
            await asyncio.sleep(0.35)
        except TelegramForbiddenError:
            await remove_subscriber(uid)
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление {uid}: {e}")


# ======================== КЛАВИАТУРЫ ========================

def get_cancel_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


cancel_keyboard = get_cancel_keyboard()


async def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    is_sub = await is_subscriber(user_id)
    buttons = [
        [KeyboardButton(text="📌 Моя закладка")],
        [KeyboardButton(text="📖 Выбор главы")],
        [KeyboardButton(text="⬅️ Предыдущая глава"), KeyboardButton(text="➡️ Следующая глава")],
        [KeyboardButton(text="🤝 Поддержать")],
        [KeyboardButton(text="❓ Помощь")],
    ]
    if is_sub:
        buttons.append([KeyboardButton(text="❌ Отписаться")])
    else:
        buttons.append([KeyboardButton(text="✅ Подписаться")])

    if ADMIN_ID and str(user_id) == ADMIN_ID:
        buttons.append([KeyboardButton(text="📊 Статус")])

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


admin_status_buttons = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📊 Подписчики", callback_data="admin_subscribers")],
    [InlineKeyboardButton(text="📜 Логи", callback_data="admin_logs")],
    [InlineKeyboardButton(text="🧹 Очистить кэш", callback_data="admin_clear_cache")],
    [InlineKeyboardButton(text="🔄 Принуд. проверка", callback_data="admin_force_check")],
    [InlineKeyboardButton(text="👤 Управление пользователем", callback_data="admin_user_manage")],
    [InlineKeyboardButton(text="✖️ Закрыть", callback_data="admin_close")],
])


# ======================== ХЕНДЛЕРЫ ========================

async def cmd_start(message: types.Message):
    uid = message.from_user.id
    text = (
        "👋 Добро пожаловать в бота-переводчика Shadow Slave!\n\n"
        "Здесь ты можешь читать свежие главы с переводом.\n"
        "Используй кнопки меню внизу."
    )
    await message.answer(text, reply_markup=await get_main_menu(uid))


async def button_subscribe(message: types.Message):
    uid = message.from_user.id
    if await is_subscriber(uid):
        await message.answer("Вы уже подписаны на уведомления о новых главах.")
        return
    await add_subscriber(uid)
    await message.answer(
        "✅ Вы подписались на уведомления о новых главах Shadow Slave!",
        reply_markup=await get_main_menu(uid)
    )


async def button_unsubscribe(message: types.Message):
    uid = message.from_user.id
    if not await is_subscriber(uid):
        await message.answer("Вы и не были подписаны.")
        return
    await remove_subscriber(uid)
    await message.answer(
        "❌ Вы отписались от уведомлений.",
        reply_markup=await get_main_menu(uid)
    )


async def button_support(message: types.Message, state: FSMContext):
    await state.clear()
    text = (
        "Поддержать переводчика можно здесь:\n"
        "https://boosty.to/shadowslavetranslator\n\n"
        "Спасибо за любую помощь! ❤️"
    )
    await message.answer(text, reply_markup=await get_main_menu(message.from_user.id))


async def button_status(message: types.Message):
    uid = message.from_user.id
    if not ADMIN_ID or str(uid) != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return

    subs = await load_subscribers()
    last_ch = await get_last_chapter() or "—"
    text = (
        f"📊 Статус бота\n\n"
        f"Подписчиков: {len(subs)}\n"
        f"Последняя глава: {last_ch}\n"
        f"Браузер активен: {'да' if browser else 'нет'}"
    )
    await message.answer(text, reply_markup=admin_status_buttons)


async def admin_clear_cache(callback: types.CallbackQuery):
    try:
        await redis_client.delete("telegraph_urls")
        await redis_client.delete("chapter_titles")
        await redis_client.delete("last_chapter")
        await callback.message.edit_text("Кэш очищен.", reply_markup=admin_status_buttons)
        await callback.answer()
    except Exception as e:
        await callback.message.edit_text(f"Ошибка: {e}", reply_markup=admin_status_buttons)


async def admin_force_check(callback: types.CallbackQuery):
    await callback.message.edit_text("Запускаю принудительную проверку...")
    asyncio.create_task(force_monitor_run(callback.message))
    await callback.answer()


async def admin_show_subscribers(callback: types.CallbackQuery):
    subs = await load_subscribers()
    if not subs:
        text = "Подписчиков нет."
    else:
        text = "Подписчики:\n" + "\n".join(str(u) for u in sorted(subs))
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_show_logs(callback: types.CallbackQuery):
    if not log_buffer:
        text = "Логов пока нет."
    else:
        text = "Последние логи:\n\n" + "\n".join(log_buffer)
        if len(text) > 4000:
            text = text[-4000:] + "\n... (обрезано)"
    await callback.message.edit_text(f"<pre>{text}</pre>", parse_mode="HTML", reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_user_manage(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminActions.waiting_for_user_id)
    await callback.message.edit_text(
        "Введите ID пользователя для управления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_cancel")]
        ])
    )
    await callback.answer()


async def process_admin_user_id(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Нужно ввести числовой ID.")
        return

    user_id = int(message.text)
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Заблокировать", callback_data=f"admin_block:{user_id}")],
        [InlineKeyboardButton(text="Разблокировать", callback_data=f"admin_unblock:{user_id}")],
        [InlineKeyboardButton(text="Удалить из подписчиков", callback_data=f"admin_remove_sub:{user_id}")],
        [InlineKeyboardButton(text="Назад", callback_data="admin_back_to_main")],
    ])

    await message.answer(f"Действия с пользователем {user_id}:", reply_markup=markup)
    await state.clear()


async def admin_block(callback: types.CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await redis_client.sadd("blocked_users", str(user_id))
    await callback.message.edit_text(f"Пользователь {user_id} заблокирован.", reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_unblock(callback: types.CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await redis_client.srem("blocked_users", str(user_id))
    await callback.message.edit_text(f"Пользователь {user_id} разблокирован.", reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_remove_sub(callback: types.CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    removed = await remove_subscriber(user_id)
    text = f"Пользователь {user_id} удалён из подписчиков." if removed else f"Пользователь {user_id} не найден."
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_back_to_main(callback: types.CallbackQuery):
    await callback.message.edit_text("Админ-панель", reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.", reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_close(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()


async def button_choose_chapter(message: types.Message, state: FSMContext):
    if await is_user_blocked(message.from_user.id):
        await message.answer("Вы заблокированы.")
        return
    await state.clear()
    await state.set_state(ChapterSelection.waiting_for_chapter)
    last_chapter = await get_last_chapter() or "?"
    await message.answer(
        f"Введите номер главы (1 — {last_chapter}):",
        reply_markup=cancel_keyboard
    )


async def button_bookmark(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer("У вас ещё нет закладки.", reply_markup=await get_main_menu(uid))
        return
    await send_chapter_to_user(uid, int(bookmark), initial_message=message)


async def button_prev(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer("Нет закладки.", reply_markup=await get_main_menu(uid))
        return
    prev_num = int(bookmark) - 1
    if prev_num < 1:
        await message.answer("Это первая глава.", reply_markup=await get_main_menu(uid))
        return
    await send_chapter_to_user(uid, prev_num, initial_message=message)


async def button_next(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer("Нет закладки.", reply_markup=await get_main_menu(uid))
        return
    next_num = int(bookmark) + 1
    await send_chapter_to_user(uid, next_num, initial_message=message)


async def process_chapter_number(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        await state.clear()
        return

    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Ввод отменён.", reply_markup=await get_main_menu(uid))
        return

    if not message.text.isdigit():
        await message.answer("Введите число или нажмите Отмена.", reply_markup=cancel_keyboard)
        return

    chapter_num = int(message.text)
    status_msg = await message.answer(f"🔍 Обработка главы {chapter_num}...")
    await send_chapter_to_user(uid, chapter_num, status_msg=status_msg)
    await state.clear()


async def button_help(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    is_admin = ADMIN_ID is not None and str(uid) == ADMIN_ID
    help_text = (
        "🤖 Доступные команды:\n"
        "📌 Моя закладка — текущая глава\n"
        "📖 Выбор главы — ввести номер\n"
        "⬅️ / ➡️ — предыдущая / следующая\n"
        "✅ / ❌ — подписка / отписка\n"
        "🤝 Поддержать — Boosty\n"
        "❓ Помощь — это сообщение"
    )
    if is_admin:
        help_text += "\n📊 Статус — админ-панель"
    await message.answer(help_text, reply_markup=await get_main_menu(uid))


async def is_user_blocked(user_id: int) -> bool:
    try:
        return await redis_client.sismember("blocked_users", str(user_id))
    except Exception:
        return False


async def send_chapter_to_user(
    user_id: int,
    chapter_num: int,
    status_msg: Optional[types.Message] = None,
    initial_message: Optional[types.Message] = None,
) -> bool:
    touch_browser_activity()
    uid = user_id
    if await is_user_blocked(uid):
        await safe_delete(status_msg)
        if initial_message:
            await initial_message.answer("Вы заблокированы.")
        return False

    try:
        cached_url = await get_cached_telegraph(str(chapter_num))
        if cached_url:
            text = f"📖 <b>Глава {chapter_num}</b>\n\n🔗 {cached_url}"
            await safe_delete(status_msg)
            await (initial_message or status_msg).answer(
                text, parse_mode="HTML", reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, str(chapter_num))
            return True

        if status_msg:
            await safe_edit_text(status_msg, f"🔍 Поиск главы {chapter_num} на сайте...")

        chapter = await find_chapter_by_number(chapter_num)
        if not chapter:
            await safe_delete(status_msg)
            await (initial_message or status_msg).answer(
                f"❌ Глава {chapter_num} не найдена.",
                reply_markup=await get_main_menu(uid)
            )
            return False

        if status_msg:
            await safe_edit_text(status_msg, f"📥 Загружаю и перевожу главу {chapter_num}...")

        url, success = await process_chapter_translation(chapter)
        await safe_delete(status_msg)

        if success and url:
            text = f"📖 <b>{chapter['title']}</b>\n\n🔗 {url}"
            await (initial_message or status_msg).answer(
                text, parse_mode="HTML", reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, chapter['id'])
            return True

        await (initial_message or status_msg).answer(
            "❌ Не удалось создать перевод. Попробуйте позже.",
            reply_markup=await get_main_menu(uid)
        )
        return False

    except Exception as e:
        logger.exception(f"send_chapter_to_user error: {e}")
        await safe_delete(status_msg)
        await (initial_message or status_msg).answer(
            "❌ Ошибка при обработке главы.",
            reply_markup=await get_main_menu(uid)
        )
        return False
    finally:
        touch_browser_activity()


async def handle_other_text(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer("Ввод отменён.", reply_markup=await get_main_menu(message.from_user.id))
    else:
        await message.answer(
            "Пожалуйста, используйте кнопки меню.",
            reply_markup=await get_main_menu(message.from_user.id)
        )


async def force_monitor_run(msg: types.Message):
    logger.info("Принудительная проверка")
    try:
        await monitor(check_once=True)
        await msg.edit_text("✅ Проверка завершена.", reply_markup=admin_status_buttons)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}", reply_markup=admin_status_buttons)


async def monitor(check_once=False):
    logger.info("Мониторинг запущен")
    while True:
        touch_browser_activity()
        try:
            html = await fetch_html(TARGET_URL)
            chapters = parse_chapters(html)
            last_str = await get_last_chapter()
            last_int = int(last_str) if last_str and last_str.isdigit() else 0

            new_chapters = [ch for ch in reversed(chapters) if int(ch['id']) > last_int]

            if new_chapters:
                logger.info(f"Новых глав: {len(new_chapters)}")
                translated_urls = []
                failed = []

                for i, ch in enumerate(new_chapters):
                    try:
                        url, success = await process_chapter_translation(ch)
                        if success and url:
                            translated_urls.append((ch['id'], url))
                        else:
                            failed.append(ch['id'])
                    except Exception as e:
                        logger.exception(f"Ошибка главы {ch['id']}")
                        failed.append(ch['id'])
                    if i < len(new_chapters) - 1:
                        await asyncio.sleep(10)

                if ADMIN_ID:
                    admin_id = int(ADMIN_ID)
                    if translated_urls:
                        await bot.send_message(
                            admin_id,
                            "✅ Переведены новые главы:\n" + "\n".join(f"• {cid}: {url}" for cid, url in translated_urls),
                            parse_mode="HTML",
                            disable_web_page_preview=True
                        )
                    if failed:
                        await bot.send_message(admin_id, f"❌ Не удалось: {', '.join(map(str, failed))}")

                max_id = max(int(ch['id']) for ch in new_chapters)
                await save_last_chapter(str(max_id))

                for cid, url in translated_urls:
                    chapter_info = next((ch for ch in new_chapters if ch['id'] == cid), None)
                    notify_text = (
                        f"📢 <b>Новая глава!</b>\n\n📖 <b>{chapter_info['title']}</b>\n\n🔗 {url}"
                        if chapter_info else f"📢 <b>Новая глава {cid}!</b>\n\n🔗 {url}"
                    )
                    await notify_all_subscribers(notify_text, parse_mode="HTML")

            else:
                logger.info("Новых глав нет")

        except Exception as e:
            logger.exception(f"Критическая ошибка мониторинга: {e}")
            await asyncio.sleep(60)

        if check_once:
            break
        await asyncio.sleep(CHECK_INTERVAL)


async def on_startup():
    global _last_activity_time
    logger.info("Бот запущен. Инициализация...")
    logger.info(f"Текущая директория: {os.getcwd()}")
    logger.info(f"Список файлов в корне: {os.listdir('.')}")
    try:
        await launch_browser()
    except Exception as e:
        logger.exception("Ошибка запуска браузера при старте")

    await load_glossary_to_redis()

    _last_activity_time = asyncio.get_event_loop().time()
    asyncio.create_task(monitor())
    asyncio.create_task(keep_browser_alive())
    logger.info("Мониторинг, глоссарий и idle-таймер запущены")


async def on_shutdown():
    logger.info("Выключение...")
    await close_browser()
    if redis_client:
        await redis_client.aclose()


async def main():
    global redis_client, bot
    redis_client = await redis.from_url(REDIS_URL)
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=RedisStorage(redis_client))

    dp.message.register(process_chapter_number, ChapterSelection.waiting_for_chapter)
    dp.message.register(process_admin_user_id, AdminActions.waiting_for_user_id)

    dp.message.register(button_choose_chapter, lambda m: m.text == "📖 Выбор главы")
    dp.message.register(button_bookmark, lambda m: m.text == "📌 Моя закладка")
    dp.message.register(button_prev, lambda m: m.text == "⬅️ Предыдущая глава")
    dp.message.register(button_next, lambda m: m.text == "➡️ Следующая глава")
    dp.message.register(button_support, lambda m: m.text == "🤝 Поддержать")
    dp.message.register(button_status, lambda m: m.text == "📊 Статус")
    dp.message.register(button_help, lambda m: m.text == "❓ Помощь")
    dp.message.register(button_subscribe, lambda m: m.text == "✅ Подписаться")
    dp.message.register(button_unsubscribe, lambda m: m.text == "❌ Отписаться")
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(handle_other_text)

    dp.callback_query.register(admin_clear_cache, lambda c: c.data == "admin_clear_cache")
    dp.callback_query.register(admin_force_check, lambda c: c.data == "admin_force_check")
    dp.callback_query.register(admin_show_subscribers, lambda c: c.data == "admin_subscribers")
    dp.callback_query.register(admin_show_logs, lambda c: c.data == "admin_logs")
    dp.callback_query.register(admin_user_manage, lambda c: c.data == "admin_user_manage")
    dp.callback_query.register(admin_block, lambda c: c.data.startswith("admin_block:"))
    dp.callback_query.register(admin_unblock, lambda c: c.data.startswith("admin_unblock:"))
    dp.callback_query.register(admin_remove_sub, lambda c: c.data.startswith("admin_remove_sub:"))
    dp.callback_query.register(admin_back_to_main, lambda c: c.data == "admin_back_to_main")
    dp.callback_query.register(admin_cancel, lambda c: c.data == "admin_cancel")
    dp.callback_query.register(admin_close, lambda c: c.data == "admin_close")

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
