import os
import asyncio
import logging
import re
import json
import ast
import html
import time
import hashlib
from difflib import SequenceMatcher
from typing import Optional, Set, List, Dict, Any
from dataclasses import dataclass
from contextlib import asynccontextmanager
import uuid
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
IDLE_TIMEOUT = int(os.getenv("BROWSER_IDLE_TIMEOUT", "180"))  # 3 минуты по умолчанию для экономии RAM
SITE_URL = "https://t.me/SHDSlaveBot"
SITE_NAME = "ShadowSlaveTranslator"
MAX_PAGES = 120
CHAPTERS_PER_PAGE = 25
TELEGRAPH_TITLE_MAX_LENGTH = 200
NOVEL_ID = "1205249"
TRANSLATION_LOCK_TTL = 15 * 60
USER_CHAPTER_REQUEST_LOCK_TTL = int(os.getenv("USER_CHAPTER_REQUEST_LOCK_TTL", "600"))
USER_CHAPTER_CANCEL_TTL = int(os.getenv("USER_CHAPTER_CANCEL_TTL", "120"))
TRANSLATION_WAIT_TIMEOUT = 90
TRANSLATION_WAIT_STEP = 2
ERROR_TTL = 5 * 60
PLAYWRIGHT_CONCURRENCY = int(os.getenv("PLAYWRIGHT_CONCURRENCY", "2"))
GLOSSARY_CACHE_TTL = int(os.getenv("GLOSSARY_CACHE_TTL", "300"))
TRANSLATION_CACHE_VERSION = os.getenv("TRANSLATION_CACHE_VERSION", "v1")
MIN_CHAPTER_BODY_LENGTH = int(os.getenv("MIN_CHAPTER_BODY_LENGTH", "400"))
DUPLICATE_BODY_SIMILARITY_THRESHOLD = float(os.getenv("DUPLICATE_BODY_SIMILARITY_THRESHOLD", "0.97"))
CHAPTER_TITLE_PATTERN = re.compile(r"^\s*Chapter\s+(\d{1,6})(?:\s*:\s*.+)?\s*$", re.IGNORECASE)
CHAPTER_LINK_PATTERN = re.compile(rf"/[^/]*-{NOVEL_ID}/(\d+)\.html$", re.IGNORECASE)
WINDOW_DATA_ASSIGN_PATTERN = re.compile(r"window\.__DATA__\s*=", re.IGNORECASE)

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
    retry=retry_if_exception_type((aiohttp.ClientError, ConnectionError, asyncio.TimeoutError)),
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
_last_activity_time = 0.0
_browser_keepalive_task: Optional[asyncio.Task] = None
_browser_lock = asyncio.Lock()
_browser_is_closing = False
monitor_lock = asyncio.Lock()
http_session: Optional[aiohttp.ClientSession] = None
subscribers_key_ready = False
playwright_semaphore = asyncio.Semaphore(PLAYWRIGHT_CONCURRENCY)
_glossary_cache: Optional[Dict[str, str]] = None
_glossary_cache_expires_at = 0.0
_glossary_cache_lock = asyncio.Lock()
# glossary_dict: Dict[str, str] = {}  трайнем через хэш
PROMPTS_DIR = os.path.dirname(os.path.abspath(__file__))
SYSTEM_PROMPT_PATH = os.path.join(PROMPTS_DIR, "system_prompt.txt")
USER_PROMPT_PATH = os.path.join(PROMPTS_DIR, "user_prompt.txt")
TRANSLATION_MODEL = os.getenv("OPENROUTER_TRANSLATION_MODEL", "google/gemini-2.5-flash-lite")
TRANSLATION_INPUT_CHAR_LIMIT = 120000


def load_prompt_file(path: str, fallback: str = "") -> str:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                return content
            logger.warning("Prompt-файл пустой: %s", path)
    except FileNotFoundError:
        logger.error("Prompt-файл не найден: %s", path)
    except Exception:
        logger.exception("Не удалось загрузить prompt-файл: %s", path)
    return fallback


SYSTEM_PROMPT = load_prompt_file(SYSTEM_PROMPT_PATH)
USER_PROMPT_TEMPLATE = load_prompt_file(USER_PROMPT_PATH)

# ======================== FSM СОСТОЯНИЯ ========================
class ChapterSelection(StatesGroup):
    waiting_for_chapter = State()

class AdminActions(StatesGroup):
    waiting_for_user_id = State()


@dataclass
class ParsedChapterPage:
    title: str
    title_source: str
    body: str
    body_source: str
    chapter_number: Optional[int]
    valid_title: bool
    valid_body: bool
    reasons: List[str]


class TelegraphRetriableError(Exception):
    """Temporary Telegraph error that should trigger retry."""


class ChapterNonRetriableError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class ChapterRetriableError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def is_retriable_processing_error(exc: Exception) -> bool:
    return isinstance(exc, (
        aiohttp.ClientError,
        ConnectionError,
        asyncio.TimeoutError,
        TelegraphRetriableError,
        OSError,
    ))


async def run_retriable_step(
    *,
    step_name: str,
    func,
    attempts: int = 3,
    base_delay: float = 1.0,
):
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except Exception as exc:
            if not is_retriable_processing_error(exc) or attempt >= attempts:
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), 8.0)
            logger.warning(
                "Временная ошибка на шаге %s (попытка %s/%s): %s",
                step_name,
                attempt,
                attempts,
                exc,
            )
            await asyncio.sleep(delay)


def _is_prompt_file_ready(path: str) -> bool:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return bool(f.read().strip())
    except Exception:
        return False


async def run_startup_checks() -> None:
    required_prompt_files = [
        ("system_prompt", SYSTEM_PROMPT_PATH),
        ("user_prompt", USER_PROMPT_PATH),
    ]

    for label, path in required_prompt_files:
        if not _is_prompt_file_ready(path):
            raise RuntimeError(f"startup_check_failed: {label}_invalid path={path}")

    glossary_path = os.path.join(PROMPTS_DIR, "glossary.txt")
    try:
        with open(glossary_path, 'r', encoding='utf-8') as glossary_file:
            glossary_file.read(1)
    except Exception as exc:
        raise RuntimeError(f"startup_check_failed: glossary_unreadable path={glossary_path}") from exc

    await redis_client.ping()
    logger.info("startup_check_passed")


def _is_retriable_telegra_ph_status(status: int) -> bool:
    return status in {408, 425, 429, 500, 502, 503, 504}


@retry(**RETRY_WEB)
async def _search_chapter_in_page_window(chapter_number: int, left: int, right: int) -> Optional[Dict[str, str]]:
    while left <= right:
        mid = (left + right) // 2
        html = await fetch_html(get_page_url(mid))
        if not html:
            right = mid - 1
            continue

        chapters = parse_chapters(html)
        if not chapters:
            right = mid - 1
            continue

        first_id = int(chapters[0]['id'])
        last_id = int(chapters[-1]['id'])

        if chapter_number > first_id:
            right = mid - 1
        elif chapter_number < last_id:
            left = mid + 1
        else:
            for ch in chapters:
                if int(ch['id']) == chapter_number:
                    return ch
            return None

        await asyncio.sleep(0.5)

    return None


async def _find_chapter_with_extended_range(chapter_number: int) -> Optional[Dict[str, str]]:
    window_size = MAX_PAGES
    max_windows = 3

    for window_idx in range(max_windows):
        left = MAX_PAGES + 1 + window_idx * window_size
        right = left + window_size - 1

        found = await _search_chapter_in_page_window(chapter_number, left, right)
        if found:
            return found

        edge_html = await fetch_html(get_page_url(right))
        if not edge_html:
            break

        edge_chapters = parse_chapters(edge_html)
        if not edge_chapters:
            break

        oldest_on_edge_page = int(edge_chapters[-1]['id'])
        if chapter_number >= oldest_on_edge_page:
            break

    return None

# ======================== БРАУЗЕР + IDLE-ТАЙМЕР ========================
async def launch_browser():
    global playwright_instance, browser, browser_context

    async with _browser_lock:
        if browser is not None:
            touch_browser_activity()
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

        touch_browser_activity()
        logger.info("Браузер запущен")
    except Exception as e:
        logger.exception("Критическая ошибка при запуске браузера")
        raise


async def close_browser():
    global browser, browser_context, playwright_instance
    global _browser_keepalive_task, _browser_is_closing

    async with _browser_lock:
        if browser is None or _browser_is_closing:
            return

        _browser_is_closing = True
        logger.info("Закрываем браузер...")

        try:
            if _browser_keepalive_task:
                _browser_keepalive_task.cancel()
                try:
                    await _browser_keepalive_task
                except asyncio.CancelledError:
                    pass
                _browser_keepalive_task = None

            if browser_context:
                try:
                    await browser_context.close()
                except PlaywrightError as e:
                    if "Target" in str(e) and ("closed" in str(e) or "disposed" in str(e)):
                        logger.debug("Контекст уже закрыт/удалён")
                    else:
                        logger.warning(f"Ошибка закрытия context: {e}")

            if browser:
                try:
                    await browser.close()
                except PlaywrightError as e:
                    logger.debug(f"Browser.close: {e}")

            if playwright_instance:
                try:
                    await playwright_instance.stop()
                except Exception:
                    pass

            await asyncio.sleep(0.2)

        except Exception as e:
            logger.warning(f"Неожиданная ошибка при закрытии браузера: {e}")

        finally:
            browser = None
            browser_context = None
            playwright_instance = None
            _browser_is_closing = False


async def keep_browser_alive():
    global _last_activity_time

    try:
        while True:
            now = asyncio.get_event_loop().time()
            idle = now - _last_activity_time
            remaining = IDLE_TIMEOUT - idle

            if remaining <= 0:
                logger.info(f"Браузер бездействовал {idle:.0f} сек → закрываем")
                await close_browser()
                break

            await asyncio.sleep(min(remaining, 60.0))

    except asyncio.CancelledError:
        logger.debug("keep_browser_alive cancelled")
        raise
    except Exception as e:
        logger.exception("Ошибка в keep_browser_alive")


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


async def create_page() -> Page:
    touch_browser_activity()

    if browser is None or browser_context is None:
        await launch_browser()

    if browser_context is None:
        logger.error("browser_context is None после launch_browser — критично!")
        raise RuntimeError("Browser context lost")

    page = await browser_context.new_page()
    await page.set_viewport_size({"width": 1280, "height": 720})
    await page.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
    })
    await page.route("**/*", lambda route: route.abort() if _should_block(route) else route.continue_())
    return page


async def get_http_session() -> aiohttp.ClientSession:
    global http_session

    if http_session is None or http_session.closed:
        timeout = aiohttp.ClientTimeout(total=130, connect=20, sock_read=120)
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10, ttl_dns_cache=300)
        http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    return http_session


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


def extract_chapter_number_from_title(title: str) -> Optional[int]:
    if not title:
        return None
    match = CHAPTER_TITLE_PATTERN.match(title.strip())
    if not match:
        return None
    return int(match.group(1))


def is_valid_chapter_title(title: str) -> bool:
    return extract_chapter_number_from_title(title) is not None


def similarity_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def parse_chapter_page_html(html_content: str) -> ParsedChapterPage:
    soup = BeautifulSoup(html_content, 'html.parser')
    reasons: List[str] = []
    title = ""
    title_source = ""

    headline = soup.select_one('h1[itemprop="headline"]')
    if headline:
        title = headline.get_text(" ", strip=True)
        title_source = 'h1[itemprop="headline"]'
    else:
        json_ld = soup.find('script', attrs={'type': 'application/ld+json'})
        if json_ld and json_ld.string:
            try:
                payload = json.loads(json_ld.string)
                if isinstance(payload, dict):
                    structured_title = payload.get("headline") or payload.get("name")
                    if isinstance(structured_title, str):
                        title = structured_title.strip()
                        title_source = "structured_data"
            except json.JSONDecodeError:
                logger.debug("Невалидный JSON-LD на странице главы")
        if not title:
            for selector in (
                'meta[property="og:title"]',
                'meta[name="title"]',
                'meta[name="description"]',
                'meta[property="og:description"]',
            ):
                tag = soup.select_one(selector)
                if tag and tag.get("content"):
                    title = tag.get("content", "").strip()
                    title_source = selector
                    break

    title = clean_title(title) if title else ""
    valid_title = is_valid_chapter_title(title)
    if not valid_title:
        reasons.append("invalid_title")

    body_source = 'div.text#arrticle'
    body = ""
    article = soup.select_one('div.text#arrticle')
    if article:
        for selector in (
            'script', 'style', 'noscript', 'blockquote',
            '.comments', '.comment', '#comments',
            '.sidebar', '.rightside', '.recent-comments',
            'header', 'footer'
        ):
            for node in article.select(selector):
                node.decompose()
        parts = [p.get_text(" ", strip=True) for p in article.find_all('p')]
        if not parts:
            parts = [s.strip() for s in article.stripped_strings if s.strip()]
        body = "\n\n".join(parts).strip()

    valid_body = len(body) >= MIN_CHAPTER_BODY_LENGTH
    if not valid_body:
        reasons.append("invalid_body")

    return ParsedChapterPage(
        title=title,
        title_source=title_source or "missing",
        body=body,
        body_source=body_source if body else "missing",
        chapter_number=extract_chapter_number_from_title(title),
        valid_title=valid_title,
        valid_body=valid_body,
        reasons=reasons,
    )

async def load_glossary_to_redis(force: bool = False):
    """
    Загружает glossary.txt → Redis хэш glossary:terms
    Если force=False — загружает только если хэш пустой
    """
    redis_key = "glossary:terms"
    
    # Проверяем, есть ли уже данные
    existing_count = await redis_client.hlen(redis_key)
    
    if existing_count > 0 and not force:
        logger.info(f"Глоссарий в Redis уже существует ({existing_count} терминов), пропускаем загрузку")
        return
    
    try:
        terms = {}
        with open('glossary.txt', 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    logger.warning(f"glossary.txt:{line_num} — строка без '='")
                    continue
                
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                if key and value:
                    terms[key] = value
        
        if not terms:
            logger.warning("Глоссарий пустой или невалидный")
            return
        
        # Сортируем
        sorted_terms = dict(sorted(terms.items(), key=lambda x: len(x[0]), reverse=True))
        
        await redis_client.hset(redis_key, mapping=sorted_terms)
        
        logger.info(f"Глоссарий успешно загружен в Redis → {len(sorted_terms)} терминов")
        
    except FileNotFoundError:
        logger.error("Файл glossary.txt не найден")
    except Exception as e:
        logger.exception("Ошибка при загрузке глоссария в Redis")


async def get_glossary_terms(force_refresh: bool = False) -> Dict[str, str]:
    global _glossary_cache, _glossary_cache_expires_at

    now = time.time()
    if not force_refresh and _glossary_cache is not None and now < _glossary_cache_expires_at:
        return _glossary_cache

    async with _glossary_cache_lock:
        now = time.time()
        if not force_refresh and _glossary_cache is not None and now < _glossary_cache_expires_at:
            return _glossary_cache

        all_terms = await redis_client.hgetall("glossary:terms")
        if not all_terms:
            logger.warning("Глоссарий в Redis пустой")
            _glossary_cache = {}
            _glossary_cache_expires_at = now + GLOSSARY_CACHE_TTL
            return _glossary_cache

        _glossary_cache = {
            eng.decode('utf-8'): rus.decode('utf-8')
            for eng, rus in all_terms.items()
        }
        _glossary_cache_expires_at = now + GLOSSARY_CACHE_TTL
        return _glossary_cache


async def get_relevant_glossary(text: str) -> str:
    all_terms = await get_glossary_terms()
    if not all_terms:
        return ""

    relevant = []
    text_lower = text.lower()

    for eng, rus in all_terms.items():
        pattern = re.compile(r"(?i)\b" + re.escape(eng) + r"\b")
        if pattern.search(text) or eng.lower() in text_lower:
            relevant.append(f"{eng} → {rus}")

    if not relevant:
        return ""

    return (
        "=== ГЛОССАРИЙ ===\n"
        "Используй ТОЛЬКО следующие переводы имён, терминов и названий (строго!):\n\n"
        + "\n".join(relevant)
        + "\n\nНе придумывай другие варианты перевода для этих слов."
    )


def sanitize_model_output(text: str) -> str:
    if not text:
        return ""

    cleaned = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    cleaned = re.sub(r"^```[\w-]*\n?", "", cleaned)
    cleaned = re.sub(r"\n?```$", "", cleaned)

    lines = []
    junk_line_patterns = [
        r"^\s*(перевод|edited translation|final translation|translation|translated text)\s*:?\s*$",
        r"^\s*(глава|chapter)\s+\d+\s*$",
        r"^\s*(готовый перевод|отредактированный перевод|художественный перевод)\s*:?\s*$",
        r"^\s*(shadow slave translator|openrouter|telegram bot)\s*$",
        r"^\s*(с уважением|thanks|thank you).*$",
    ]

    for raw_line in cleaned.split("\n"):
        line = raw_line.strip()
        if not line:
            lines.append("")
            continue
        if any(re.match(pattern, line, flags=re.IGNORECASE) for pattern in junk_line_patterns):
            continue
        lines.append(line)

    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    return cleaned.strip()


async def request_translation_completion(
    *,
    session: aiohttp.ClientSession,
    headers: Dict[str, str],
    messages: List[Dict[str, str]],
    stage_name: str,
    temperature: float,
    top_p: float,
    max_tokens: int = 8192,
    presence_penalty: float = 0.0,
    frequency_penalty: float = 0.0,
) -> str:
    payload = {
        "model": TRANSLATION_MODEL,
        "messages": messages,
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "presence_penalty": presence_penalty,
        "frequency_penalty": frequency_penalty,
    }

    async with session.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=payload,
    ) as resp:
        if resp.status == 200:
            data = await resp.json()
            try:
                content = data["choices"][0]["message"]["content"]
            except (KeyError, IndexError, TypeError) as exc:
                raise ValueError(f"{stage_name}: некорректный ответ модели") from exc

            cleaned = sanitize_model_output(content or "")
            if not cleaned:
                raise ValueError(f"{stage_name}: пустой ответ модели")
            return cleaned

        if resp.status == 429:
            logger.warning("Rate limit от OpenRouter на этапе %s", stage_name)
            raise aiohttp.ClientError(f"Rate limit during {stage_name}")

        text_resp = await resp.text()
        logger.error("OpenRouter %s %s: %s", stage_name, resp.status, text_resp[:500])
        raise aiohttp.ClientError(f"HTTP {resp.status} during {stage_name}")

def text_to_html(text: str) -> str:
    paragraphs = text.split('\n\n')
    return ''.join(
        "<p>" + html.escape(p).replace('\n', '<br>') + "</p>"
        for p in paragraphs if p.strip()
    )


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

async def get_chapter_original_text(chapter_id: str) -> Optional[str]:
    try:
        value = await redis_client.get(f"chapter:original:{chapter_id}")
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_chapter_original_text error for {chapter_id}: {e}")
        return None


async def save_chapter_original_text(chapter_id: str, text: str) -> None:
    try:
        await redis_client.set(f"chapter:original:{chapter_id}", text)
    except Exception as e:
        logger.error(f"save_chapter_original_text error for {chapter_id}: {e}")


def build_translation_signature(original_text: str) -> str:
    payload = json.dumps({
        "cache_version": TRANSLATION_CACHE_VERSION,
        "system_prompt": SYSTEM_PROMPT,
        "user_prompt": USER_PROMPT_TEMPLATE,
        "text_sha256": hashlib.sha256(original_text.encode("utf-8")).hexdigest(),
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def invalidate_outdated_chapter_cache(chapter_id: str, cache: dict[str, str], translation_signature: str) -> dict[str, str]:
    cached_signature = cache.get("translation_signature")
    if not cached_signature or cached_signature == translation_signature:
        return cache

    logger.info(
        "Сигнатура перевода для главы %s изменилась, сбрасываем устаревшие перевод и Telegraph",
        chapter_id,
    )
    await redis_client.delete(f"chapter:translated:{chapter_id}")
    await redis_client.hdel("telegraph_urls", chapter_id)
    await save_chapter_cache(chapter_id, {
        "translation_signature": translation_signature,
        "telegraph_url": "",
        "translated_title": "",
        "status": "stale",
        "error": "",
    })
    refreshed_cache = await get_chapter_cache(chapter_id)
    refreshed_cache["translation_signature"] = translation_signature
    return refreshed_cache


async def get_chapter_translated_text(chapter_id: str) -> Optional[str]:
    try:
        value = await redis_client.get(f"chapter:translated:{chapter_id}")
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_chapter_translated_text error for {chapter_id}: {e}")
        return None


async def save_chapter_translated_text(chapter_id: str, text: str) -> None:
    try:
        await redis_client.set(f"chapter:translated:{chapter_id}", text)
    except Exception as e:
        logger.error(f"save_chapter_translated_text error for {chapter_id}: {e}")
        
async def get_chapter_cache(chapter_id: str) -> dict[str, str]:
    try:
        data = await redis_client.hgetall(f"chapter:{chapter_id}")
        if not data:
            return {}
        return {k.decode(): v.decode() for k, v in data.items()}
    except Exception as e:
        logger.error(f"get_chapter_cache error for {chapter_id}: {e}")
        return {}


async def save_chapter_cache(chapter_id: str, mapping: dict[str, str]) -> None:
    try:
        payload = {k: str(v) for k, v in mapping.items() if v is not None}
        if not payload:
            return
        await redis_client.hset(f"chapter:{chapter_id}", mapping=payload)
    except Exception as e:
        logger.error(f"save_chapter_cache error for {chapter_id}: {e}")


async def set_chapter_status(chapter_id: str, status: str, error: str = "") -> None:
    await save_chapter_cache(chapter_id, {
        "status": status,
        "error": error,
        "updated_at": str(int(time.time())),
    })


@dataclass
class RedisLockHandle:
    key: str
    token: str


def _translation_lock_key(chapter_id: str) -> str:
    return f"translation:lock:{chapter_id}"


def _user_chapter_lock_key(user_id: int) -> str:
    return f"user:chapter_request_lock:{user_id}"


def _user_chapter_cancel_key(user_id: int) -> str:
    return f"user:chapter_request_cancel:{user_id}"


async def acquire_redis_lock(lock_key: str, ttl: int) -> Optional[RedisLockHandle]:
    token = uuid.uuid4().hex
    try:
        acquired = await redis_client.set(lock_key, token, ex=ttl, nx=True)
        if acquired:
            return RedisLockHandle(key=lock_key, token=token)
        return None
    except Exception as e:
        logger.error("acquire_redis_lock error for %s: %s", lock_key, e)
        return None


async def release_redis_lock(lock: Optional[RedisLockHandle]) -> None:
    if not lock:
        return
    try:
        await redis_client.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            1,
            lock.key,
            lock.token,
        )
    except Exception as e:
        logger.error("release_redis_lock error for %s: %s", lock.key, e)


async def lock_exists(lock_key: str) -> bool:
    try:
        return bool(await redis_client.exists(lock_key))
    except Exception as e:
        logger.error("lock_exists error for %s: %s", lock_key, e)
        return False


async def acquire_translation_lock_handle(chapter_id: str) -> Optional[RedisLockHandle]:
    return await acquire_redis_lock(_translation_lock_key(chapter_id), TRANSLATION_LOCK_TTL)


async def acquire_translation_lock(chapter_id: str) -> bool:
    lock = await acquire_translation_lock_handle(chapter_id)
    if lock:
        await release_redis_lock(lock)
        return True
    return False


async def release_translation_lock(chapter_id: str) -> None:
    try:
        await redis_client.delete(_translation_lock_key(chapter_id))
    except Exception as e:
        logger.error(f"release_translation_lock error for {chapter_id}: {e}")


async def release_translation_lock_handle(lock: Optional[RedisLockHandle]) -> None:
    await release_redis_lock(lock)


async def is_translation_in_progress(chapter_id: str) -> bool:
    return await lock_exists(_translation_lock_key(chapter_id))


async def acquire_user_chapter_lock_handle(user_id: int) -> Optional[RedisLockHandle]:
    return await acquire_redis_lock(_user_chapter_lock_key(user_id), USER_CHAPTER_REQUEST_LOCK_TTL)


async def acquire_user_chapter_lock(user_id: int) -> bool:
    lock = await acquire_user_chapter_lock_handle(user_id)
    if lock:
        await release_redis_lock(lock)
        return True
    return False


async def release_user_chapter_lock(user_id: int, token: Optional[str] = None) -> None:
    if token:
        await release_redis_lock(RedisLockHandle(key=_user_chapter_lock_key(user_id), token=token))
        return
    try:
        await redis_client.delete(_user_chapter_lock_key(user_id))
    except Exception as e:
        logger.error("release_user_chapter_lock error for %s: %s", user_id, e)


async def is_user_chapter_request_in_progress(user_id: int) -> bool:
    return await lock_exists(_user_chapter_lock_key(user_id))


async def mark_user_chapter_request_cancelled(user_id: int) -> None:
    try:
        await redis_client.set(_user_chapter_cancel_key(user_id), "1", ex=USER_CHAPTER_CANCEL_TTL)
    except Exception as e:
        logger.error("mark_user_chapter_request_cancelled error for %s: %s", user_id, e)


async def clear_user_chapter_request_cancelled(user_id: int) -> None:
    try:
        await redis_client.delete(_user_chapter_cancel_key(user_id))
    except Exception as e:
        logger.error("clear_user_chapter_request_cancelled error for %s: %s", user_id, e)


async def is_user_chapter_request_cancelled(user_id: int) -> bool:
    return await lock_exists(_user_chapter_cancel_key(user_id))


class UserChapterRequestCancelled(Exception):
    pass


async def raise_if_user_request_cancelled(user_id: int) -> None:
    if await is_user_chapter_request_cancelled(user_id):
        raise UserChapterRequestCancelled(f"User {user_id} cancelled chapter request")


@asynccontextmanager
async def user_chapter_request_lock(user_id: int):
    lock = await acquire_user_chapter_lock_handle(user_id)
    try:
        yield lock
    finally:
        if lock:
            await release_user_chapter_lock(user_id, lock.token)
            await clear_user_chapter_request_cancelled(user_id)


async def wait_for_ready_translation(chapter_id: str, timeout: int = TRANSLATION_WAIT_TIMEOUT) -> Optional[str]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        url = await get_cached_telegraph(chapter_id)
        if url:
            return url

        cache = await get_chapter_cache(chapter_id)

        if cache.get("telegraph_url"):
            return cache["telegraph_url"]

        if cache.get("status") in {"failed", "retryable_error"}:
            return None

        await asyncio.sleep(TRANSLATION_WAIT_STEP)

    url = await get_cached_telegraph(chapter_id)
    if url:
        return url

    cache = await get_chapter_cache(chapter_id)
    return cache.get("telegraph_url")


async def save_translation_error(chapter_id: str, error: str) -> None:
    try:
        await redis_client.set(f"translation:error:{chapter_id}", error, ex=ERROR_TTL)
    except Exception as e:
        logger.error(f"save_translation_error error for {chapter_id}: {e}")


async def get_translation_error(chapter_id: str) -> Optional[str]:
    try:
        value = await redis_client.get(f"translation:error:{chapter_id}")
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_translation_error error for {chapter_id}: {e}")
        return None


async def save_chapter_meta(ch: Dict[str, str]) -> None:
    try:
        await redis_client.hset(f"chapter_meta:{ch['id']}", mapping={
            "id": ch["id"],
            "title": ch["title"],
            "link": ch["link"],
        })
    except Exception as e:
        logger.error(f"save_chapter_meta error for {ch['id']}: {e}")


async def get_chapter_meta(chapter_id: str) -> Optional[Dict[str, str]]:
    try:
        data = await redis_client.hgetall(f"chapter_meta:{chapter_id}")
        if not data:
            return None
        decoded = {k.decode(): v.decode() for k, v in data.items()}
        if not decoded.get("id") or not decoded.get("title") or not decoded.get("link"):
            return None
        return {
            "id": decoded["id"],
            "title": decoded["title"],
            "link": decoded["link"],
        }
    except Exception as e:
        logger.error(f"get_chapter_meta error for {chapter_id}: {e}")
        return None


async def ensure_subscribers_key() -> None:
    global subscribers_key_ready
    if subscribers_key_ready:
        return
    try:
        key_type = await redis_client.type("subscribers")
        if isinstance(key_type, bytes):
            key_type = key_type.decode()

        if key_type in ("none", "set"):
            subscribers_key_ready = True
            return

        migrated_ids: Set[int] = set()

        if key_type == "string":
            raw = await redis_client.get("subscribers")
            if raw:
                raw_str = raw.decode() if isinstance(raw, bytes) else str(raw)
                try:
                    parsed = json.loads(raw_str)
                    if isinstance(parsed, list):
                        for item in parsed:
                            try:
                                migrated_ids.add(int(item))
                            except (TypeError, ValueError):
                                continue
                    elif isinstance(parsed, dict):
                        for item in parsed.keys():
                            try:
                                migrated_ids.add(int(item))
                            except (TypeError, ValueError):
                                continue
                except json.JSONDecodeError:
                    for part in re.split(r"[,\s]+", raw_str.strip()):
                        if not part:
                            continue
                        try:
                            migrated_ids.add(int(part))
                        except ValueError:
                            continue
        elif key_type == "list":
            values = await redis_client.lrange("subscribers", 0, -1)
            for value in values:
                try:
                    migrated_ids.add(int(value.decode() if isinstance(value, bytes) else value))
                except (TypeError, ValueError):
                    continue

        pipe = redis_client.pipeline()
        pipe.delete("subscribers")
        if migrated_ids:
            pipe.sadd("subscribers", *[str(uid) for uid in migrated_ids])
        await pipe.execute()
        subscribers_key_ready = True
        logger.info(f"Миграция ключа subscribers завершена, перенесено {len(migrated_ids)} подписчиков")
    except Exception as e:
        logger.error(f"ensure_subscribers_key error: {e}")


async def load_subscribers() -> Set[int]:
    try:
        await ensure_subscribers_key()
        values = await redis_client.smembers("subscribers")
        return {int(v.decode() if isinstance(v, bytes) else v) for v in values}
    except Exception as e:
        logger.error(f"load_subscribers error: {e}")
        return set()


async def save_subscribers(subs: Set[int]):
    try:
        await ensure_subscribers_key()
        pipe = redis_client.pipeline()
        pipe.delete("subscribers")
        if subs:
            pipe.sadd("subscribers", *[str(uid) for uid in subs])
        await pipe.execute()
    except Exception as e:
        logger.error(f"save_subscribers error: {e}")


async def add_subscriber(user_id: int) -> None:
    try:
        await ensure_subscribers_key()
        await redis_client.sadd("subscribers", str(user_id))
    except Exception as e:
        logger.error(f"add_subscriber error: {e}")


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
async def get_first_chapter() -> Optional[int]:
    try:
        cached = await redis_client.get("first_chapter")
        if cached:
            return int(cached.decode())
    except Exception as e:
        logger.error(f"get_first_chapter cached error: {e}")

    html = await fetch_html(TARGET_URL)
    if not html:
        return None
    chapters = parse_chapters(html)
    if not chapters:
        return None
    first = int(chapters[0]['id'])
    try:
        await redis_client.set("first_chapter", str(first), ex=3600)
    except Exception as e:
        logger.error(f"save_first_chapter error: {e}")
    return first


blocked_users_key = "blocked_users"


async def is_user_blocked(user_id: int) -> bool:
    try:
        return await redis_client.sismember(blocked_users_key, str(user_id))
    except Exception as e:
        logger.error(f"is_user_blocked error: {e}")
        return False


async def block_user(user_id: int):
    try:
        await redis_client.sadd(blocked_users_key, str(user_id))
        await remove_subscriber(user_id)
        logger.info(f"Пользователь {user_id} заблокирован и удалён из подписчиков")
    except Exception as e:
        logger.error(f"block_user error: {e}")


async def unblock_user(user_id: int):
    try:
        await redis_client.srem(blocked_users_key, str(user_id))
        logger.info(f"Пользователь {user_id} разблокирован")
    except Exception as e:
        logger.error(f"unblock_user error: {e}")


async def remove_subscriber(user_id: int) -> bool:
    try:
        await ensure_subscribers_key()
        removed = await redis_client.srem("subscribers", str(user_id))
        if removed:
            logger.info(f"Пользователь {user_id} удалён из подписчиков")
            return True
        return False
    except Exception as e:
        logger.error(f"remove_subscriber error: {e}")
        return False


async def get_user_bookmark(user_id: int) -> Optional[str]:
    try:
        value = await redis_client.hget("user_bookmarks", str(user_id))
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_user_bookmark error: {e}")
        return None


async def save_user_bookmark(user_id: int, chapter_id: str):
    try:
        await redis_client.hset("user_bookmarks", str(user_id), chapter_id)
    except Exception as e:
        logger.error(f"save_user_bookmark error: {e}")


@retry(**RETRY_WEB)
async def fetch_html(url: str) -> str:
    async with playwright_semaphore:
        page = await create_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            try:
                await page.wait_for_selector('a:has-text("Chapter")', timeout=40000)
            except PlaywrightError:
                logger.warning(f"Селектор 'a:has-text(\"Chapter\")' не найден на {url}, продолжаем")
            await page.wait_for_timeout(1500)
            return await page.content()
        except Exception as e:
            logger.error(f"Ошибка в fetch_html на {url}: {e}")
            raise
        finally:
            if not page.is_closed():
                await page.close()


@retry(**RETRY_WEB)
async def fetch_chapter_page_data(url: str) -> ParsedChapterPage:
    async with playwright_semaphore:
        page = await create_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_selector('div.text#arrticle', timeout=60000)
            parsed = parse_chapter_page_html(await page.content())
            return parsed
        except Exception as e:
            logger.error(f"Ошибка в fetch_chapter_page_data на {url}: {e}")
            raise
        finally:
            if not page.is_closed():
                await page.close()


@retry(**RETRY_WEB)
async def fetch_chapter_text(url: str) -> str:
    parsed = await fetch_chapter_page_data(url)
    if not parsed.valid_body:
        raise ValueError(f"Body parsing failed for {url}")
    return parsed.body


def parse_chapters(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')
    chapters: List[Dict[str, str]] = []
    seen_ids: set[str] = set()
    diagnostics: List[str] = []
    window_data_found = False

    data_raw = extract_window_data_object(html)
    if data_raw:
        window_data_found = True
        try:
            payload = parse_window_data_payload(data_raw)
            if not isinstance(payload, dict):
                raise ValueError("window.__DATA__ is not an object")
            payload_chapters = payload.get("chapters", []) if isinstance(payload, dict) else []
            diagnostics.append(f"window.__DATA__.chapters={len(payload_chapters)}")
            for item in payload_chapters:
                if not isinstance(item, dict):
                    continue
                raw_title = (item.get("title") or "").strip()
                chapter_number = extract_chapter_number_from_title(raw_title)
                link = (item.get("link") or "").strip()
                source_id = str(item.get("id", "")).strip()
                if chapter_number is None or not link:
                    continue
                cid = str(chapter_number)
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
                chapters.append({
                    'id': cid,
                    'source_id': source_id,
                    'raw_title': raw_title,
                    'title': clean_title(raw_title),
                    'link': link if link.startswith('http') else f"https://ranobes.net{link}"
                })
        except ValueError as e:
            diagnostics.append(f"window.__DATA__ decode failed: {e}")
        except Exception as e:
            diagnostics.append(f"window.__DATA__ parse error: {e}")
    else:
        diagnostics.append("window.__DATA__ not found")

    if not chapters:
        fallback_selectors = (
            f'.last-chapters a[href*="-{NOVEL_ID}/"][href$=".html"]',
            f'.chapters a[href*="-{NOVEL_ID}/"][href$=".html"]',
            f'.chapter-list a[href*="-{NOVEL_ID}/"][href$=".html"]',
            'a[rel="chapter"]',
            'a.chapter-item',
            'a.chapter-item.txt-dec',
        )
        for selector in fallback_selectors:
            matched = soup.select(selector)
            diagnostics.append(f"fallback {selector}={len(matched)}")
            for a in matched:
                raw_title = a.get_text(" ", strip=True)
                chapter_number = extract_chapter_number_from_title(raw_title)
                if chapter_number is None:
                    title_node = a.select_one('.title')
                    if title_node:
                        raw_title = title_node.get_text(" ", strip=True)
                        chapter_number = extract_chapter_number_from_title(raw_title)
                href = (a.get('href') or "").strip()
                if chapter_number is None or not href:
                    continue
                link = href if href.startswith('http') else f"https://ranobes.net{href}"
                if NOVEL_ID not in link:
                    continue
                cid = str(chapter_number)
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
                parsed_url = urlparse(link)
                source_match = re.search(r"/(\d+)\.html$", parsed_url.path)
                chapters.append({
                    'id': cid,
                    'source_id': source_match.group(1) if source_match else "",
                    'raw_title': raw_title,
                    'title': clean_title(raw_title),
                    'link': link
                })

    if not chapters:
        legacy_count = 0
        for a in soup.find_all('a', href=True):
            href = (a.get('href') or "").strip()
            if not href:
                continue
            link = href if href.startswith('http') else f"https://ranobes.net{href}"
            parsed_url = urlparse(link)
            chapter_match = CHAPTER_LINK_PATTERN.search(parsed_url.path)
            if not chapter_match:
                continue
            legacy_count += 1
            raw_title = a.get_text(" ", strip=True)
            chapter_number = extract_chapter_number_from_title(raw_title)
            if chapter_number is None:
                continue
            cid = str(chapter_number)
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            chapters.append({
                'id': cid,
                'source_id': chapter_match.group(1),
                'raw_title': raw_title,
                'title': clean_title(raw_title),
                'link': link
            })
        diagnostics.append(f"legacy /chapters links={legacy_count}")

    chapters.sort(key=lambda x: int(x['id']), reverse=True)

    if chapters:
        logger.info(f"Найдено глав: {len(chapters)} | Самая новая: {chapters[0]['id']} — {chapters[0]['raw_title']!r}")
    else:
        logger.error(
            "Не найдено ни одной главы. diagnostics=%s | window_data_found=%s",
            "; ".join(diagnostics),
            window_data_found,
        )
    return chapters


def extract_window_data_object(page_html: str) -> Optional[str]:
    assign_match = WINDOW_DATA_ASSIGN_PATTERN.search(page_html)
    if not assign_match:
        return None

    idx = assign_match.end()
    while idx < len(page_html) and page_html[idx].isspace():
        idx += 1
    if idx >= len(page_html) or page_html[idx] != "{":
        return None

    start = idx
    depth = 0
    in_string: Optional[str] = None
    escaped = False

    while idx < len(page_html):
        ch = page_html[idx]

        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_string:
                in_string = None
        else:
            if ch in ('"', "'"):
                in_string = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return page_html[start:idx + 1]
        idx += 1
    return None


def parse_window_data_payload(raw_payload: str) -> Dict[str, Any]:
    try:
        return json.loads(raw_payload)
    except json.JSONDecodeError as strict_error:
        relaxed_payload = re.sub(r",\s*([}\]])", r"\1", raw_payload)
        relaxed_payload = re.sub(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:", r'\1"\2":', relaxed_payload)
        relaxed_payload = re.sub(r"\bundefined\b", "null", relaxed_payload)
        try:
            return json.loads(relaxed_payload)
        except json.JSONDecodeError:
            python_literal = relaxed_payload
            python_literal = re.sub(r"\btrue\b", "True", python_literal, flags=re.IGNORECASE)
            python_literal = re.sub(r"\bfalse\b", "False", python_literal, flags=re.IGNORECASE)
            python_literal = re.sub(r"\bnull\b", "None", python_literal, flags=re.IGNORECASE)
            try:
                parsed = ast.literal_eval(python_literal)
            except Exception as relaxed_error:
                raise ValueError(f"strict={strict_error.msg}; relaxed={relaxed_error}") from relaxed_error
            if not isinstance(parsed, dict):
                raise ValueError("window.__DATA__ relaxed parse did not return dict")
            return parsed


@retry(**RETRY_WEB)
async def find_chapter_by_number(chapter_number: int) -> Optional[Dict[str, str]]:
    first_chapter = await get_first_chapter()
    if not first_chapter:
        return await find_chapter_by_number_binary(chapter_number)

    page_estimate = 1 + (first_chapter - chapter_number) // CHAPTERS_PER_PAGE
    if page_estimate < 1:
        page_estimate = 1
    if page_estimate > MAX_PAGES:
        page_estimate = MAX_PAGES

    url = get_page_url(page_estimate)
    html = await fetch_html(url)
    if html:
        chapters = parse_chapters(html)
        for ch in chapters:
            if int(ch['id']) == chapter_number:
                logger.info(f"Глава {chapter_number} найдена на странице {page_estimate}")
                return ch

    return await find_chapter_by_number_binary(chapter_number)


@retry(**RETRY_WEB)
async def find_chapter_by_number_binary(chapter_number: int) -> Optional[Dict[str, str]]:
    found = await _search_chapter_in_page_window(chapter_number, 1, MAX_PAGES)
    if found:
        return found

    max_page_html = await fetch_html(get_page_url(MAX_PAGES))
    if not max_page_html:
        return None

    max_page_chapters = parse_chapters(max_page_html)
    if not max_page_chapters:
        return None

    oldest_in_limited_range = int(max_page_chapters[-1]['id'])
    if chapter_number >= oldest_in_limited_range:
        return None

    logger.warning(
        "page_limit_reached chapter=%s max_pages=%s oldest_in_limit=%s",
        chapter_number,
        MAX_PAGES,
        oldest_in_limited_range,
    )
    return await _find_chapter_with_extended_range(chapter_number)



@retry(**RETRY_API)
async def translate_text(text: str) -> str:
    if not SYSTEM_PROMPT or not USER_PROMPT_TEMPLATE:
        raise RuntimeError("Не удалось загрузить system_prompt.txt или user_prompt.txt")

    def _split_text_into_chunks(source_text: str, max_chunk_size: int) -> list[str]:
        if not source_text:
            return []

        paragraphs = [p for p in source_text.split("\n\n") if p and p.strip()]
        chunks: list[str] = []
        current_parts: list[str] = []
        current_len = 0

        def flush_current() -> None:
            nonlocal current_parts, current_len
            if not current_parts:
                return
            chunk_text = "\n\n".join(current_parts).strip()
            if chunk_text:
                chunks.append(chunk_text)
            current_parts = []
            current_len = 0

        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue

            paragraph_len = len(paragraph)
            if paragraph_len > max_chunk_size:
                flush_current()
                words = re.split(r"(\s+)", paragraph)
                overlong_parts: list[str] = []
                overlong_len = 0
                for word in words:
                    if not word:
                        continue
                    word_len = len(word)
                    if overlong_len + word_len > max_chunk_size and overlong_parts:
                        long_chunk = "".join(overlong_parts).strip()
                        if long_chunk:
                            chunks.append(long_chunk)
                        overlong_parts = [word]
                        overlong_len = word_len
                    else:
                        overlong_parts.append(word)
                        overlong_len += word_len
                if overlong_parts:
                    long_chunk = "".join(overlong_parts).strip()
                    if long_chunk:
                        chunks.append(long_chunk)
                continue

            add_len = paragraph_len if not current_parts else paragraph_len + 2
            if current_len + add_len > max_chunk_size and current_parts:
                flush_current()
                add_len = paragraph_len

            current_parts.append(paragraph)
            current_len += add_len

        flush_current()
        return chunks

    chunks = _split_text_into_chunks(text, TRANSLATION_INPUT_CHAR_LIMIT)
    if not chunks:
        logger.warning("Пустой текст перевода после разбиения на чанки")
        return ""

    logger.info("Разбивка текста на чанки завершена: chunks=%s", len(chunks))
    for i, chunk in enumerate(chunks, start=1):
        logger.info("Размер чанка %s: %s символов", i, len(chunk))

    glossary_section = await get_relevant_glossary(text)

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json"
    }

    session = await get_http_session()
    system_prompt = SYSTEM_PROMPT
    if glossary_section:
        system_prompt = f"{glossary_section}\n\n{SYSTEM_PROMPT}"

    async def _translate_chunk(chunk_text: str, chunk_index: int) -> str:
        first_pass_user_prompt = USER_PROMPT_TEMPLATE.format(
            text=chunk_text, stage="translation", draft="", source_text=chunk_text
        )
        logger.info("Старт первого прохода перевода чанка %s: %s символов", chunk_index, len(chunk_text))
        first_pass = await request_translation_completion(
            session=session,
            headers=headers,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": first_pass_user_prompt},
            ],
            stage_name=f"first_pass_translation_chunk_{chunk_index}",
            temperature=0.8,
            top_p=0.95,
            presence_penalty=0.15,
            frequency_penalty=0.08,
        )

        second_pass_user_prompt = (
            "Ниже исходный английский фрагмент и его русский черновик.\n\n"
            "Сделай ТОЛЬКО редактуру русского текста.\n"
            "Ты — редактор художественного перевода.\n"
            "Если оно звучит как перевод — перепиши полностью.\n"
            "Особое внимание:\n"
            "• длинные предложения → разбивать\n"
            "• калька → уничтожать\n"
            "• слабые формулировки → усиливать\n"
            "Добавь:\n"
            "• ритм\n"
            "• паузы\n"
            "• эмоциональные удары\n"
            "Можно:\n"
            "• сильно переписывать\n"
            "• менять структуру\n"
            "• сокращать\n"
            "Нельзя:\n"
            "• менять смысл\n"
            "• добавлять новый сюжет\n"
            "Цель:\n"
            "текст должен звучать как книга, а не перевод.\n"
            "Не добавляй комментарии, заголовки, markdown, пояснения или альтернативные версии.\n\n"
            f"Исходник:\n{chunk_text}\n\n"
            f"Черновой перевод:\n{first_pass}"
        )

        try:
            logger.info("Старт второго прохода редактуры чанка %s: %s символов", chunk_index, len(first_pass))
            second_pass = await request_translation_completion(
                session=session,
                headers=headers,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            f"{system_prompt}\n\n"
                            "Ты выступаешь как финальный литературный редактор русского текста. "
                            "Не пересказывай и не объясняй правки — верни только готовую отредактированную версию."
                        ),
                    },
                    {"role": "user", "content": second_pass_user_prompt},
                ],
                stage_name=f"second_pass_editing_chunk_{chunk_index}",
                temperature=0.75,
                top_p=0.9,
                presence_penalty=0.05,
                frequency_penalty=0.03,
            )
            return second_pass
        except Exception:
            logger.exception("Второй проход перевода чанка %s не удался, возвращаем результат первого прохода", chunk_index)
            if first_pass:
                return first_pass
            raise

    translated_chunks: list[str] = []
    seen_chunk_hashes: Set[str] = set()
    for idx, chunk in enumerate(chunks, start=1):
        if not chunk.strip():
            logger.warning("Пропускаем пустой чанк %s", idx)
            continue

        chunk_hash = hashlib.sha256(chunk.encode("utf-8")).hexdigest()
        if chunk_hash in seen_chunk_hashes:
            logger.warning("Пропускаем дублирующийся чанк %s", idx)
            continue
        seen_chunk_hashes.add(chunk_hash)

        try:
            translated_chunk = await _translate_chunk(chunk, idx)
        except Exception:
            logger.exception("Какой чанк упал: %s", idx)
            raise

        if translated_chunk and translated_chunk.strip():
            translated_chunks.append(translated_chunk.strip())
        else:
            logger.warning("Пустой перевод чанка %s после обработки", idx)

    if not translated_chunks:
        logger.warning("После пакетного перевода не получено ни одного чанка")
        return ""

    return "\n\n".join(translated_chunks)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5) + wait_exponential(min=2, max=60),
    retry=retry_if_exception_type((
        aiohttp.ClientError,
        ConnectionError,
        asyncio.TimeoutError,
        TelegraphRetriableError,
    )),
)
async def create_telegraph_page(
    title: str,
    content_html: str,
    author: str = "Shadow Slave Bot"
) -> Optional[str]:
    clean_title_text = clean_title_for_telegraph(title)

    soup = BeautifulSoup(content_html, 'html.parser')
    nodes = []
    for elem in soup.children:
        if elem.name == 'p':
            children = []
            for sub in elem.children:
                if sub.name == 'a':
                    children.append({
                        "tag": "a",
                        "attrs": {"href": sub.get('href')},
                        "children": [sub.get_text()]
                    })
                else:
                    text = sub.string if sub.string else ''
                    if text:
                        children.append(text)
            nodes.append({"tag": "p", "children": children})

    if not nodes:
        nodes = [{"tag": "p", "children": [content_html]}]

    payload = {
        "access_token": TELEGRAPH_ACCESS_TOKEN,
        "title": clean_title_text,
        "author_name": author,
        "content": nodes,
    }

    for attempt in range(2):
        session = await get_http_session()
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        try:
            async with session.post("https://api.telegra.ph/createPage", json=payload, timeout=timeout) as resp:
                if _is_retriable_telegra_ph_status(resp.status):
                    logger.warning("Telegraph retriable_error status=%s", resp.status)
                    raise TelegraphRetriableError(f"status={resp.status}")

                data = await resp.json()
                if data.get("ok"):
                    url = data["result"]["url"]
                    logger.info(f"Создана страница Telegraph: {url}")
                    return url

                error = data.get('error')
                if error == 'TITLE_TOO_LONG' and attempt == 0:
                    clean_title_text = clean_title_text[:150] + "..."
                    payload["title"] = clean_title_text
                    logger.info("Telegraph non_retriable_error type=TITLE_TOO_LONG action=trim_title")
                    continue

                if error in {'PATH_NUM_NOT_FOUND', 'CONTENT_FORMAT_INVALID', 'ACCESS_TOKEN_INVALID'}:
                    logger.error("Telegraph non_retriable_error type=%s", error)
                    return None

                logger.warning("Telegraph retriable_error type=%s", error)
                raise TelegraphRetriableError(f"error={error}")
        except (aiohttp.ClientError, ConnectionError, asyncio.TimeoutError) as exc:
            logger.warning("Telegraph retriable_error type=network error=%s", exc)
            raise
        except TelegraphRetriableError:
            raise
        except Exception as exc:
            logger.error("Telegraph non_retriable_error type=unexpected error=%s", exc)
            return None

    return None


@retry(**RETRY_API)
async def translate_title(title: str) -> str:
    """Переводит только заголовок главы коротким и дешёвым запросом."""
    source_title = (title or "").strip()
    if not source_title:
        return "Без названия"

    # Если заголовок уже содержит кириллицу, считаем его переведённым.
    if re.search(r"[А-Яа-яЁё]", source_title):
        return clean_title_for_telegraph(source_title)

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json"
    }

    payload = {
        "model": "google/gemini-2.5-flash-lite",
        "messages": [
            {
                "role": "system",
                "content": (
                    "Переведи ТОЛЬКО название главы новеллы на русский язык. "
                    "Сохрани номер главы, стиль и смысл. "
                    "Верни только итоговое название без пояснений, кавычек и префиксов."
                ),
            },
            {"role": "user", "content": source_title}
        ],
        "temperature": 0.4,
        "top_p": 0.9,
        "max_tokens": 120,
    }

    session = await get_http_session()
    async with session.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=payload,
    ) as resp:
        if resp.status == 200:
            data = await resp.json()
            content = data["choices"][0]["message"]["content"]
            translated = content.strip() if content else source_title
            return clean_title_for_telegraph(translated)
        elif resp.status == 429:
            logger.warning("Rate limit от OpenRouter при переводе заголовка")
            raise aiohttp.ClientError("Rate limit")
        else:
            text_resp = await resp.text()
            logger.error(f"OpenRouter title {resp.status}: {text_resp[:500]}")
            return clean_title_for_telegraph(source_title)


async def process_chapter_translation(ch: Dict[str, str]) -> tuple[Optional[str], bool]:
    cid = ch["id"]
    title = ch["title"]
    source_url = ch["link"]

    await save_chapter_meta(ch)

    ready_url = await get_cached_telegraph(cid)
    if not ready_url:
        ready_url = (await get_chapter_cache(cid)).get("telegraph_url")
    if ready_url:
        logger.info("Глава %s уже есть в готовом кэше, повторный перевод не нужен", cid)
        return ready_url, True

    chapter_lock = await acquire_translation_lock_handle(cid)
    if not chapter_lock:
        logger.info("Глава %s уже обрабатывается другим процессом, ждём готовый результат", cid)
        ready_url = await wait_for_ready_translation(cid)
        if ready_url:
            return ready_url, True
        logger.warning("Ожидание перевода главы %s завершилось без результата", cid)
        return None, False

    try:
        await set_chapter_status(cid, "processing")
        cache = await get_chapter_cache(cid)
        await save_chapter_cache(cid, {
            "source_url": source_url,
            "source_title": title,
        })

        url = cache.get("telegraph_url") or await get_cached_telegraph(cid)
        if url:
            await save_chapter_cache(cid, {"telegraph_url": url, "status": "ready", "error": ""})
            return url, True

        cached_source_url = cache.get("source_url")
        original_text = await get_chapter_original_text(cid)
        if cached_source_url and cached_source_url != source_url:
            logger.warning(
                "source_url сменился для главы %s: %s -> %s. Сбрасываем старый текст.",
                cid,
                cached_source_url,
                source_url,
            )
            original_text = None
            await redis_client.delete(f"chapter:original:{cid}")

        if not original_text:
            try:
                parsed_page = await run_retriable_step(
                    step_name=f"fetch_chapter_page_data:{cid}",
                    func=lambda: fetch_chapter_page_data(source_url),
                    attempts=3,
                )
            except Exception as e:
                if is_retriable_processing_error(e):
                    raise ChapterRetriableError("fetch_temporary_error") from e
                raise

            if not parsed_page.valid_title:
                logger.warning(
                    "Глава %s отклонена: invalid_title title_source=%s reasons=%s",
                    cid,
                    parsed_page.title_source,
                    ",".join(parsed_page.reasons),
                )
                raise ChapterNonRetriableError("invalid_title")
            if not parsed_page.valid_body:
                logger.warning(
                    "Глава %s отклонена: invalid_body body_source=%s reasons=%s",
                    cid,
                    parsed_page.body_source,
                    ",".join(parsed_page.reasons),
                )
                raise ChapterNonRetriableError("invalid_body")
            if parsed_page.chapter_number != int(cid):
                logger.warning(
                    "Глава %s отклонена: номер в заголовке %s не совпадает",
                    cid,
                    parsed_page.chapter_number,
                )
                raise ChapterNonRetriableError("chapter_mismatch")
            original_text = parsed_page.body
            await save_chapter_original_text(cid, original_text)

        translation_signature = build_translation_signature(original_text)
        cache = await invalidate_outdated_chapter_cache(cid, cache, translation_signature)
        await save_chapter_cache(cid, {"translation_signature": translation_signature})

        url = cache.get("telegraph_url") or await get_cached_telegraph(cid)
        if url:
            logger.info(f"Глава {cid} найдена в актуальном telegraph-кэше")
            await save_chapter_cache(cid, {"telegraph_url": url, "status": "ready", "error": ""})
            return url, True

        translated_text = await get_chapter_translated_text(cid)
        if not translated_text:
            try:
                translated_text = await run_retriable_step(
                    step_name=f"translate_text:{cid}",
                    func=lambda: translate_text(original_text),
                    attempts=2,
                )
            except Exception as e:
                if is_retriable_processing_error(e):
                    raise ChapterRetriableError("translate_temporary_error") from e
                raise ChapterNonRetriableError("translate_logic_error") from e
            if not translated_text or not translated_text.strip():
                raise ChapterNonRetriableError("empty_translation")
            await save_chapter_translated_text(cid, translated_text)

        translated_title = cache.get("translated_title") or await get_cached_title(cid)
        if translated_title and re.search(r"[А-Яа-яЁё]", translated_title):
            translated_title_clean = clean_title_for_telegraph(translated_title)
        else:
            try:
                translated_title_clean = await run_retriable_step(
                    step_name=f"translate_title:{cid}",
                    func=lambda: translate_title(title),
                    attempts=2,
                )
            except Exception as e:
                if is_retriable_processing_error(e):
                    raise ChapterRetriableError("title_temporary_error") from e
                raise ChapterNonRetriableError("invalid_title") from e
            if not translated_title_clean:
                translated_title_clean = clean_title_for_telegraph(title)
        if not translated_title_clean or not translated_title_clean.strip():
            raise ChapterNonRetriableError("invalid_title")

        await save_cached_title(cid, translated_title_clean)
        await save_chapter_cache(cid, {"translated_title": translated_title_clean})

        url = (await get_chapter_cache(cid)).get("telegraph_url") or await get_cached_telegraph(cid)
        if url:
            await set_chapter_status(cid, "ready")
            return url, True

        html = text_to_html(translated_text)
        try:
            new_url = await run_retriable_step(
                step_name=f"create_telegraph_page:{cid}",
                func=lambda: create_telegraph_page(title=translated_title_clean, content_html=html),
                attempts=3,
            )
        except Exception as e:
            if is_retriable_processing_error(e):
                raise ChapterRetriableError("telegraph_temporary_error") from e
            raise

        if new_url:
            await save_telegraph_url(cid, new_url)
            await save_chapter_cache(cid, {"telegraph_url": new_url, "status": "ready", "error": ""})
            await set_chapter_status(cid, "ready")
            return new_url, True

        logger.error(f"Не удалось создать Telegraph для главы {cid}")
        raise ChapterRetriableError("telegraph_failed")

    except ChapterNonRetriableError as e:
        logger.warning("process_chapter_translation non-retriable for %s: %s", cid, e.code)
        await set_chapter_status(cid, "failed", e.code)
        await save_translation_error(cid, e.code)
        return None, False
    except ChapterRetriableError as e:
        logger.warning("process_chapter_translation retriable for %s: %s", cid, e.code)
        await set_chapter_status(cid, "retryable_error", e.code)
        await save_translation_error(cid, e.code)
        return None, False
    except Exception as e:
        logger.exception(f"process_chapter_translation error for {cid}: {e}")
        if is_retriable_processing_error(e):
            await set_chapter_status(cid, "retryable_error", "temporary_error")
            await save_translation_error(cid, "temporary_error")
        else:
            await set_chapter_status(cid, "failed", "unexpected_error")
            await save_translation_error(cid, "unexpected_error")
        return None, False

    finally:
        await release_translation_lock_handle(chapter_lock)


async def notify_all_subscribers(text: str, parse_mode: str = "HTML", reply_markup=None):
    subs = await load_subscribers()
    if not subs:
        logger.info("Нет подписчиков")
        return

    semaphore = asyncio.Semaphore(10)

    async def send_with_limit(uid):
        if await is_user_blocked(uid):
            return
        async with semaphore:
            try:
                await bot.send_message(
                    chat_id=uid,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup
                )
            except (TelegramForbiddenError, TelegramBadRequest):
                logger.info(f"Пользователь {uid} недоступен")
            except Exception as e:
                logger.error(f"Ошибка отправки {uid}: {e}")

    tasks = [send_with_limit(uid) for uid in subs]
    await asyncio.gather(*tasks, return_exceptions=True)


async def is_user_subscribed(user_id: int) -> bool:
    subs = await load_subscribers()
    return user_id in subs


async def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    is_admin = ADMIN_ID is not None and str(user_id) == ADMIN_ID

    buttons = [
        [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="📖 Выбор главы")],
        [KeyboardButton(text="⬅️ Предыдущая глава"), KeyboardButton(text="➡️ Следующая глава")],
        [KeyboardButton(text="🤝 Поддержать")],
    ]
    row3 = [KeyboardButton(text="❓ Помощь")]
    if is_admin:
        row3.insert(0, KeyboardButton(text="📊 Статус"))
    buttons.append(row3)

    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


async def get_profile_menu(user_id: int) -> ReplyKeyboardMarkup:
    is_subscribed = await is_user_subscribed(user_id)
    subscribe_button = "❌ Отписаться от рассылки" if is_subscribed else "✅ Подписаться на рассылку"

    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📌 Моя закладка")],
            [KeyboardButton(text=subscribe_button)],
            [KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


async def ensure_subscription(message: types.Message) -> bool:
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return False
    if await is_user_subscribed(uid):
        return True

    await message.answer(
        "🔒 Этот раздел доступен только подписчикам бота.\n\n"
        "Нажмите «✅ Подписаться на рассылку» в профиле, чтобы открыть доступ к главам и навигации.",
        reply_markup=await get_main_menu(uid)
    )
    return False


cancel_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
    input_field_placeholder="Введите номер главы или нажмите Отмена"
)

admin_status_buttons = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Очистить кэш", callback_data="admin_clear_cache")],
        [InlineKeyboardButton(text="🚀 Принудительная проверка", callback_data="admin_force_check")],
        [InlineKeyboardButton(text="📋 Список подписчиков", callback_data="admin_subscribers")],
        [InlineKeyboardButton(text="📜 Последние логи", callback_data="admin_logs")],
        [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_user_manage")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close")]
    ]
)

admin_user_manage_buttons = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Заблокировать пользователя", callback_data="admin_block")],
        [InlineKeyboardButton(text="✅ Разблокировать пользователя", callback_data="admin_unblock")],
        [InlineKeyboardButton(text="🗑 Удалить подписку", callback_data="admin_remove_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_to_main")]
    ]
)


async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы. Обратитесь к администратору.")
        return
    if await is_user_subscribed(uid):
        await message.answer("С возвращением! Используйте кнопки меню для навигации.", reply_markup=await get_main_menu(uid))
        return
    await message.answer(
        "👋 Добро пожаловать!\n\n"
        "Чтобы пользоваться главами, навигацией и получать новые главы, сначала подпишитесь через кнопку «👤 Мой профиль».",
        reply_markup=await get_main_menu(uid)
    )


async def button_support(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    await message.answer(
        "❤️ Спасибо, что пользуетесь ботом!\n\n"
        "Если вы хотите поддержать проект — Boosty:\n"
        "👉 https://boosty.to/1h8u",
        reply_markup=await get_main_menu(uid)
    )


async def button_profile(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    await message.answer("👤 Профиль", reply_markup=await get_profile_menu(uid))


async def button_profile_subscribe(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    if await is_user_subscribed(uid):
        await message.answer("Вы уже подписаны на рассылку.", reply_markup=await get_profile_menu(uid))
    else:
        await add_subscriber(uid)
        await message.answer("✅ Вы подписались на рассылку и получили доступ к функциям бота.", reply_markup=await get_profile_menu(uid))


async def button_profile_unsubscribe(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    if await is_user_subscribed(uid):
        await remove_subscriber(uid)
        await message.answer("❌ Вы отписались от рассылки.", reply_markup=await get_profile_menu(uid))
    else:
        await message.answer("Вы не были подписаны на рассылку.", reply_markup=await get_profile_menu(uid))


async def button_back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    await message.answer("↩️ Возвращаю в главное меню.", reply_markup=await get_main_menu(uid))


async def button_status(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if ADMIN_ID is None or str(uid) != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    await message.answer("Выберите действие:", reply_markup=admin_status_buttons)


async def admin_clear_cache(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    try:
        preserved_keys = {"last_chapter", "subscribers", "glossary:terms"}
        deleted_keys = 0

        async for key in redis_client.scan_iter(match="*"):
            key_name = key.decode("utf-8") if isinstance(key, bytes) else key
            if key_name in preserved_keys:
                continue
            deleted_keys += await redis_client.delete(key)

        await callback.answer("Данные Redis очищены")
        await safe_edit_text(
            callback.message,
            (
                "✅ Все данные Redis удалены, кроме "
                "<code>last_chapter</code>, <code>subscribers</code> "
                "и <code>glossary:terms</code>.\n"
                f"Удалено ключей: <b>{deleted_keys}</b>."
            ),
            parse_mode="HTML",
            reply_markup=admin_status_buttons
        )
    except Exception:
        logger.exception("Ошибка очистки Redis")
        await callback.answer("Ошибка", show_alert=True)


async def admin_force_check(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.answer("Запуск проверки...")
    msg = await callback.message.edit_text("🔄 Принудительная проверка запущена...", reply_markup=admin_status_buttons)
    asyncio.create_task(force_monitor_run(msg))


async def admin_show_subscribers(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    subs = await load_subscribers()
    text = f"Всего подписчиков: {len(subs)}" if subs else "Нет подписчиков."
    await safe_edit_text(callback.message, text, reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_show_logs(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    text = "Последние 10 логов:\n" + "\n".join(list(log_buffer)[-10:]) if log_buffer else "Логи отсутствуют."
    await safe_edit_text(callback.message, text, reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_user_manage(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await safe_edit_text(callback.message, "Выберите действие:", reply_markup=admin_user_manage_buttons)
    await callback.answer()


async def admin_back_to_main(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await safe_edit_text(callback.message, "Выберите действие:", reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_close(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.message.delete()
    await callback.answer()


async def admin_action_start(callback: types.CallbackQuery, state: FSMContext, action: str):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await state.set_state(AdminActions.waiting_for_user_id)
    await state.update_data(action_type=action)
    msg = await callback.message.edit_text(
        "Введите ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]])
    )
    await state.update_data(request_msg_id=msg.message_id)
    await callback.answer()


async def admin_block(callback: types.CallbackQuery, state: FSMContext):
    await admin_action_start(callback, state, "block")


async def admin_unblock(callback: types.CallbackQuery, state: FSMContext):
    await admin_action_start(callback, state, "unblock")


async def admin_remove_sub(callback: types.CallbackQuery, state: FSMContext):
    await admin_action_start(callback, state, "remove")


async def process_admin_user_id(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Введите число или Отмена.")
        return
    user_id = int(message.text)
    data = await state.get_data()
    action = data.get("action_type")
    request_msg_id = data.get("request_msg_id")

    if action == "block":
        await block_user(user_id)
        response = f"Пользователь {user_id} заблокирован."
    elif action == "unblock":
        await unblock_user(user_id)
        response = f"Пользователь {user_id} разблокирован."
    elif action == "remove":
        removed = await remove_subscriber(user_id)
        response = f"Пользователь {user_id} удалён из подписчиков." if removed else f"Пользователь {user_id} не найден."
    else:
        response = "Неизвестное действие."

    await state.clear()
    if request_msg_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=request_msg_id)
        except Exception:
            pass
    await message.answer(response, reply_markup=admin_status_buttons)


async def admin_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_text(callback.message, "Действие отменено.", reply_markup=admin_status_buttons)
    await callback.answer()


async def button_choose_chapter(message: types.Message, state: FSMContext):
    if not await ensure_subscription(message):
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
    if not await ensure_subscription(message):
        return
    uid = message.from_user.id
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer("У вас ещё нет закладки. Пожалуйста, воспользуйтесь выбором главы.", reply_markup=await get_main_menu(uid))
        return
    await send_chapter_to_user(uid, int(bookmark), initial_message=message)


async def button_prev(message: types.Message, state: FSMContext):
    await state.clear()
    if not await ensure_subscription(message):
        return
    uid = message.from_user.id
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer("Нет закладки. Пожалуйста, воспользуйтесь выбором главы.", reply_markup=await get_main_menu(uid))
        return
    prev_num = int(bookmark) - 1
    if prev_num < 1:
        await message.answer("Это первая глава.", reply_markup=await get_main_menu(uid))
        return
    status_msg = await message.answer(f"🔍 Обработка главы {prev_num}...")
    await send_chapter_to_user(uid, prev_num, status_msg=status_msg)


async def button_next(message: types.Message, state: FSMContext):
    await state.clear()
    if not await ensure_subscription(message):
        return
    uid = message.from_user.id
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer("Нет закладки. Пожалуйста, воспользуйтесь выбором главы.", reply_markup=await get_main_menu(uid))
        return
    next_num = int(bookmark) + 1
    status_msg = await message.answer(f"🔍 Обработка главы {next_num}...")
    await send_chapter_to_user(uid, next_num, status_msg=status_msg)


async def process_chapter_number(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        await state.clear()
        return
    if not await is_user_subscribed(uid):
        await message.answer(
            "🔒 Выбор главы доступен только подписчикам бота.",
            reply_markup=await get_main_menu(uid)
        )
        await state.clear()
        return

    if message.text == "❌ Отмена":
        if await is_user_chapter_request_in_progress(uid):
            await mark_user_chapter_request_cancelled(uid)
            await message.answer(
                "🛑 Текущий запрос главы отменяется. Если перевод уже запущен, он может завершиться в фоне, но новый запрос можно будет отправить после завершения отмены.",
                reply_markup=await get_main_menu(uid)
            )
        else:
            await message.answer("Ввод отменён.", reply_markup=await get_main_menu(uid))
        await state.clear()
        return

    if not message.text.isdigit():
        await message.answer("Введите число или нажмите Отмена.", reply_markup=cancel_keyboard)
        return

    lock = await acquire_user_chapter_lock_handle(uid)
    if not lock:
        await message.answer(
            "⏳ У вас уже обрабатывается запрос главы. Дождитесь завершения текущего запроса или отмените его.",
            reply_markup=cancel_keyboard
        )
        return

    chapter_num = int(message.text)
    status_msg = None
    try:
        await clear_user_chapter_request_cancelled(uid)
        status_msg = await message.answer(f"🔍 Обработка главы {chapter_num}...")
        await raise_if_user_request_cancelled(uid)
        await send_chapter_to_user(uid, chapter_num, status_msg=status_msg)
    except UserChapterRequestCancelled:
        logger.info("Запрос главы пользователя %s отменён до отправки результата", uid)
        await safe_delete(status_msg)
        await message.answer("🛑 Запрос главы отменён.", reply_markup=await get_main_menu(uid))
    finally:
        await release_user_chapter_lock(uid, lock.token)
        await clear_user_chapter_request_cancelled(uid)
        await state.clear()


async def button_help(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    is_admin = ADMIN_ID is not None and str(uid) == ADMIN_ID
    help_text = (
        "🤖 Доступные команды:\n"
        "👤 Мой профиль — меню профиля\n"
        "📌 Моя закладка — открыть сохранённую главу\n"
        "📖 Выбор главы — ввести номер главы\n"
        "⬅️ / ➡️ — предыдущая / следующая глава\n"
        "✅ / ❌ — подписка или отписка от рассылки в профиле\n"
        "🤝 Поддержать — Boosty\n"
        "❓ Помощь — это сообщение"
    )
    if is_admin:
        help_text += "\n📊 Статус — админ-панель"
    await message.answer(help_text, reply_markup=await get_main_menu(uid))


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
        await raise_if_user_request_cancelled(uid)
        cached_url = await get_cached_telegraph(str(chapter_num))
        if not cached_url:
            chapter_cache = await get_chapter_cache(str(chapter_num))
            cached_url = chapter_cache.get("telegraph_url")
            if cached_url:
                await save_telegraph_url(str(chapter_num), cached_url)

        if cached_url:
            text = f"📖 <b>Глава {chapter_num}</b>\n\n🔗 {cached_url}"
            await safe_delete(status_msg)
            await (initial_message or status_msg).answer(
                text, parse_mode="HTML", reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, str(chapter_num))
            return True

        await raise_if_user_request_cancelled(uid)
        chapter = await get_chapter_meta(str(chapter_num))
        if chapter:
            logger.info(f"Глава {chapter_num} найдена в chapter_meta кэше")
            if status_msg:
                await safe_edit_text(status_msg, f"📦 Глава {chapter_num} найдена в кэше, подготавливаю перевод...")
        else:
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
            await save_chapter_meta(chapter)

        in_progress = await is_translation_in_progress(chapter["id"])
        if status_msg:
            if in_progress:
                await safe_edit_text(status_msg, f"⏳ Глава {chapter_num} уже переводится, ожидаю готовый результат...")
            else:
                await safe_edit_text(status_msg, f"📥 Загружаю и перевожу главу {chapter_num}...")

        await raise_if_user_request_cancelled(uid)
        url, success = await process_chapter_translation(chapter)
        await raise_if_user_request_cancelled(uid)
        await safe_delete(status_msg)

        if success and url:
            text = f"📖 <b>{chapter['title']}</b>\n\n🔗 {url}"
            await (initial_message or status_msg).answer(
                text, parse_mode="HTML", reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, chapter['id'])
            return True

        recent_error = await get_translation_error(chapter["id"])
        if recent_error:
            error_map = {
                "fetch_failed": "❌ Не удалось загрузить текст главы. Попробуйте позже.",
                "telegraph_failed": "❌ Перевод получен, но не удалось создать страницу Telegraph. Попробуйте позже.",
                "unexpected_error": "❌ Во время обработки главы произошла ошибка. Попробуйте позже.",
            }
            await (initial_message or status_msg).answer(
                error_map.get(recent_error, "❌ Не удалось создать перевод. Попробуйте позже."),
                reply_markup=await get_main_menu(uid)
            )
            return False

        await (initial_message or status_msg).answer(
            "❌ Не удалось создать перевод. Попробуйте позже.",
            reply_markup=await get_main_menu(uid)
        )
        return False
    except UserChapterRequestCancelled:
        logger.info("send_chapter_to_user cancelled for user %s", uid)
        await safe_delete(status_msg)
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
        await safe_edit_text(msg, "✅ Проверка завершена.", reply_markup=admin_status_buttons)
    except Exception as e:
        await safe_edit_text(msg, f"❌ Ошибка: {e}", reply_markup=admin_status_buttons)


async def validate_monitor_candidate(ch: Dict[str, str], previous_latest_id: int) -> tuple[bool, str]:
    chapter_id = int(ch["id"])
    if chapter_id <= previous_latest_id:
        return False, "not_new"

    parsed = await fetch_chapter_page_data(ch["link"])
    similarity = 0.0
    previous_text = await get_chapter_original_text(str(previous_latest_id)) if previous_latest_id > 0 else None
    if previous_text and parsed.body:
        similarity = similarity_ratio(parsed.body, previous_text)

    logger.info(
        "monitor_validation chapter=%s url=%s title_source=%s body_source=%s parsed_number=%s prev=%s similarity=%.4f",
        ch["id"], ch["link"], parsed.title_source, parsed.body_source,
        parsed.chapter_number, previous_latest_id, similarity,
    )

    if not parsed.valid_title:
        return False, "invalid_title"
    if not parsed.valid_body:
        return False, "invalid_body"
    if parsed.chapter_number is None:
        return False, "missing_chapter_number"
    if parsed.chapter_number != chapter_id:
        return False, "number_mismatch"
    if similarity >= DUPLICATE_BODY_SIMILARITY_THRESHOLD:
        return False, "duplicate_body"
    return True, "ok"


async def monitor(check_once=False):
    logger.info("Мониторинг запущен")
    while True:
        try:
            async with monitor_lock:
                html = await fetch_html(TARGET_URL)
                chapters = parse_chapters(html)
                for ch in chapters:
                    await save_chapter_meta(ch)
                last_str = await get_last_chapter()
                last_int = int(last_str) if last_str and last_str.isdigit() else 0

                new_chapters = [ch for ch in reversed(chapters) if int(ch['id']) > last_int]

                if new_chapters:
                    logger.info(f"Новых глав: {len(new_chapters)}")
                    translated_urls = []
                    failed = []
                    last_successful_id = last_int
                    stop_on_first_failure = False

                    for i, ch in enumerate(new_chapters):
                        if stop_on_first_failure:
                            failed.append(ch['id'])
                            continue
                        try:
                            is_valid, reason = await validate_monitor_candidate(ch, last_successful_id)
                            if not is_valid:
                                logger.warning(
                                    "monitor_decision=quarantined chapter=%s reason=%s url=%s",
                                    ch["id"], reason, ch["link"],
                                )
                                failed.append(ch['id'])
                                stop_on_first_failure = True
                                continue

                            url, success = await process_chapter_translation(ch)
                            if success and url:
                                logger.info("monitor_decision=published chapter=%s url=%s", ch["id"], ch["link"])
                                translated_urls.append((ch['id'], url))
                                last_successful_id = int(ch['id'])
                            else:
                                logger.warning("monitor_decision=skipped chapter=%s reason=translation_failed", ch["id"])
                                failed.append(ch['id'])
                                stop_on_first_failure = True
                        except Exception:
                            logger.exception(f"Ошибка главы {ch['id']}")
                            failed.append(ch['id'])
                            stop_on_first_failure = True
                        if i < len(new_chapters) - 1 and not stop_on_first_failure:
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

                    if last_successful_id > last_int:
                            await save_last_chapter(str(last_successful_id))

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
    logger.info("Бот запущен. Загружаем глоссарий и запускаем мониторинг...")

    await run_startup_checks()
    await load_glossary_to_redis(force=False)
    terms = await get_glossary_terms(force_refresh=True)
    logger.info(f"Глоссарий в Redis: {len(terms)} терминов")

    asyncio.create_task(monitor())
    logger.info("Мониторинг запущен; браузер будет стартовать лениво по запросу")


async def on_shutdown():
    global http_session
    logger.info("Выключение...")
    await close_browser()
    if http_session and not http_session.closed:
        await http_session.close()
        http_session = None
    if redis_client:
        await redis_client.aclose()


async def main():
    global redis_client, bot
    redis_client = await redis.from_url(REDIS_URL)
    await ensure_subscribers_key()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=RedisStorage(redis_client))

    dp.message.register(process_chapter_number, ChapterSelection.waiting_for_chapter)
    dp.message.register(process_admin_user_id, AdminActions.waiting_for_user_id)

    dp.message.register(button_profile, lambda m: m.text == "👤 Мой профиль")
    dp.message.register(button_choose_chapter, lambda m: m.text == "📖 Выбор главы")
    dp.message.register(button_bookmark, lambda m: m.text == "📌 Моя закладка")
    dp.message.register(button_prev, lambda m: m.text == "⬅️ Предыдущая глава")
    dp.message.register(button_next, lambda m: m.text == "➡️ Следующая глава")
    dp.message.register(button_support, lambda m: m.text == "🤝 Поддержать")
    dp.message.register(button_status, lambda m: m.text == "📊 Статус")
    dp.message.register(button_help, lambda m: m.text == "❓ Помощь")
    dp.message.register(button_profile_subscribe, lambda m: m.text == "✅ Подписаться на рассылку")
    dp.message.register(button_profile_unsubscribe, lambda m: m.text == "❌ Отписаться от рассылки")
    dp.message.register(button_back_to_main, lambda m: m.text == "⬅️ Назад")
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(handle_other_text)

    dp.callback_query.register(admin_clear_cache, lambda c: c.data == "admin_clear_cache")
    dp.callback_query.register(admin_force_check, lambda c: c.data == "admin_force_check")
    dp.callback_query.register(admin_show_subscribers, lambda c: c.data == "admin_subscribers")
    dp.callback_query.register(admin_show_logs, lambda c: c.data == "admin_logs")
    dp.callback_query.register(admin_user_manage, lambda c: c.data == "admin_user_manage")
    dp.callback_query.register(admin_back_to_main, lambda c: c.data == "admin_back_to_main")
    dp.callback_query.register(admin_close, lambda c: c.data == "admin_close")
    dp.callback_query.register(admin_block, lambda c: c.data == "admin_block")
    dp.callback_query.register(admin_unblock, lambda c: c.data == "admin_unblock")
    dp.callback_query.register(admin_remove_sub, lambda c: c.data == "admin_remove_sub")
    dp.callback_query.register(admin_cancel, lambda c: c.data == "admin_cancel")

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
