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
IDLE_TIMEOUT = 1200             # 20 минут
SITE_URL = "https://t.me/SHDSlaveBot"
SITE_NAME = "ShadowSlaveTranslator"
MAX_PAGES = 120
CHAPTERS_PER_PAGE = 25
TELEGRAPH_TITLE_MAX_LENGTH = 200
NOVEL_ID = "1205249"

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
_browser_lock = asyncio.Lock()
_browser_is_closing = False
# glossary_dict: Dict[str, str] = {}  трайнем через хэш
SYSTEM_PROMPT = (
    "Ты — профессиональный литературный переводчик веб-новелл, специализирующийся на Shadow Slave. "
    "Ты переводишь на уровне лучших русскоязычных команд (RanobeLib, Rulate, BookWire). "
    "Твоя задача — сделать текст живым, атмосферным, кинематографичным и естественным, как будто это оригинальная русская книга.\n\n"
    "Обязательно соблюдай:\n"
    "• Живой литературный русский: варьируй длину предложений, используй динамичный ритм, создавай напряжение.\n"
    "• Сохраняй чёрный юмор, иронию и внутренние мысли Санни.\n"
    "• Идеальные склонения, род, падежи и местоимения.\n"
    "• Точные имена: Sunny = Санни, Nephis = Нефис (никогда Неф!), Azarax = Азаракс, Killer = Убийца, Shadow Legion = Теневой Легион.\n\n"
    "СТРОГИЙ ЗАПРЕТ (нарушение = ошибка):\n"
    "• НИКОГДА не добавляй ватермарки, обфускации, символы, названия сайтов или ботов (особенно ŔãŊօΒЁS, ranobes, Shadow Slave Bot, даты и т.п.).\n"
    "• Выводи ТОЛЬКО чистый текст перевода главы. Никаких заголовков, дат, подписей, примечаний и лишних строк."
    "• Очень важно соблюдать склонения, местоимения и падежи"
    "• НЕ ДОЛЖНО БЫТЬ ТАКОГО И ПОДОБНОГО ЭТОМУ: он держал чашку кофе - не дешевЫЙ синтетическИЙ пойлО"
)

USER_PROMPT_TEMPLATE = (
    "Переведи следующий текст главы на русский язык максимально качественно и литературно. "
    "Сделай перевод живым, атмосферным и естественным. Сохрани характер Санни, его внутренний сарказм и мысли. "
    "Используй динамичные предложения и правильный ритм русского языка.\n\n"
    "Текст для перевода:\n\n{text}"
)

# ======================== FSM СОСТОЯНИЯ ========================
class ChapterSelection(StatesGroup):
    waiting_for_chapter = State()

class AdminActions(StatesGroup):
    waiting_for_user_id = State()

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
    global browser, browser_context, shared_page, playwright_instance
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
            shared_page = None
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


async def get_shared_page() -> Page:
    global shared_page

    touch_browser_activity()

    if browser is None:
        await launch_browser()

    if shared_page is not None and not shared_page.is_closed():
        return shared_page

    if browser_context is None:
        logger.error("browser_context is None после launch_browser — критично!")
        raise RuntimeError("Browser context lost")

    logger.info("Создаём новую общую страницу")
    shared_page = await browser_context.new_page()
    await shared_page.set_viewport_size({"width": 1280, "height": 720})
    await shared_page.set_extra_http_headers({
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
    })
    await shared_page.route("**/*", lambda route: route.abort() if _should_block(route) else route.continue_())
    return shared_page


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
        
        await redis_client.hmset(redis_key, sorted_terms)
        
        logger.info(f"Глоссарий успешно загружен в Redis → {len(sorted_terms)} терминов")
        
    except FileNotFoundError:
        logger.error("Файл glossary.txt не найден")
    except Exception as e:
        logger.exception("Ошибка при загрузке глоссария в Redis")


async def get_relevant_glossary(text: str) -> str:
    """Возвращает строку с релевантными терминами из Redis"""
    redis_key = "glossary:terms"
    
    all_terms = await redis_client.hgetall(redis_key)
    
    if not all_terms:
        logger.warning("Глоссарий в Redis пустой")
        return ""
    
    relevant = []
    text_lower = text.lower()
    
    for eng_bytes, rus_bytes in all_terms.items():
        eng = eng_bytes.decode('utf-8')
        rus = rus_bytes.decode('utf-8')
        
        # Точный поиск с границами слова
        pattern = re.compile(r'(?i)\b' + re.escape(eng) + r'\b')
        if pattern.search(text) or eng.lower() in text_lower:
            relevant.append(f"{eng} → {rus}")
    
    if not relevant:
        return ""
    
    return (
        "=== ГЛОССАРИЙ ===\n"
        "Используй ТОЛЬКО следующие переводы имён, терминов и названий (строго!):\n\n" +
        "\n".join(relevant) +
        "\n\nНе придумывай другие варианты перевода для этих слов."
    )


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
    subs = await load_subscribers()
    if user_id in subs:
        subs.remove(user_id)
        await save_subscribers(subs)
        logger.info(f"Пользователь {user_id} удалён из подписчиков")
        return True
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
    page = await get_shared_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=90000)
        try:
            await page.wait_for_selector('a:has-text("Chapter")', timeout=40000)
        except PlaywrightError:
            logger.warning(f"Селектор 'a:has-text(\"Chapter\")' не найден на {url}, продолжаем")
        await page.wait_for_timeout(2000)
        return await page.content()
    except Exception as e:
        logger.error(f"Ошибка в fetch_html на {url}: {e}")
        if not page.is_closed():
            await page.close()
        raise


@retry(**RETRY_WEB)
async def fetch_chapter_text(url: str) -> str:
    page = await get_shared_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_selector('div.text#arrticle', timeout=60000)

        paragraphs = await page.evaluate('''() => {
            const c = document.querySelector('div.text#arrticle');
            if (!c) return [];
            return Array.from(c.querySelectorAll('p'))
                .map(p => p.innerText.trim())
                .filter(t => t.length > 0);
        }''')

        if paragraphs:
            return '\n\n'.join(paragraphs)

        content = await page.text_content('div.text#arrticle')
        return content.strip() if content else "[Текст не найден]"
    except Exception as e:
        logger.error(f"Ошибка в fetch_chapter_text на {url}: {e}")
        if not page.is_closed():
            await page.close()
        raise


def parse_chapters(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')
    chapters = []
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href']
        cid = extract_chapter_id(text)
        if cid and cid.isdigit():
            link = 'https://ranobes.net' + href if not href.startswith('http') else href
            if NOVEL_ID not in link:
                continue
            chapters.append({
                'id': cid,
                'raw_title': text,
                'title': clean_title(text),
                'link': link
            })
    chapters.sort(key=lambda x: int(x['id']), reverse=True)

    if chapters:
        logger.info(f"Найдено глав: {len(chapters)} | Самая новая: {chapters[0]['id']} — {chapters[0]['raw_title']!r}")
    else:
        logger.error("Не найдено ни одной главы — структура сайта изменилась?")
    return chapters


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
    left, right = 1, MAX_PAGES
    while left <= right:
        mid = (left + right) // 2
        url = get_page_url(mid)
        html = await fetch_html(url)
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


SYSTEM_PROMPT = (
    "Ты — профессиональный литературный переводчик веб-новелл, специализирующийся на Shadow Slave. "
    "Ты переводишь на уровне лучших русскоязычных команд (RanobeLib, Rulate, BookWire). "
    "Твоя задача — сделать текст живым, атмосферным, кинематографичным и естественным, как будто это оригинальная русская книга.\n\n"
    "Обязательно соблюдай:\n"
    "• Живой литературный русский: варьируй длину предложений, используй динамичный ритм, создавай напряжение.\n"
    "• Сохраняй чёрный юмор, иронию и внутренние мысли Санни.\n"
    "• Идеальные склонения, род, падежи и местоимения.\n"
    "• Точные имена: Sunny = Санни, Nephis = Нефис (никогда Неф!), Azarax = Азаракс, Killer = Убийца, Shadow Legion = Теневой Легион.\n\n"
    "СТРОГИЙ ЗАПРЕТ (нарушение = ошибка):\n"
    "• НИКОГДА не добавляй ватермарки, обфускации, символы, названия сайтов или ботов (особенно ŔãŊօΒЁS, ranobes, Shadow Slave Bot, даты и т.п.).\n"
    "• Выводи ТОЛЬКО чистый текст перевода главы. Никаких заголовков, дат, подписей, примечаний и лишних строк."
    "• Очень важно соблюдать склонения, местоимения и падежи"
    "• НЕ ДОЛЖНО БЫТЬ ТАКОГО И ПОДОБНОГО ЭТОМУ: он держал чашку кофе - не дешевЫЙ синтетическИЙ пойлО"
)

USER_PROMPT_TEMPLATE = (
    "Переведи следующий текст главы на русский язык максимально качественно и литературно. "
    "Сделай перевод живым, атмосферным и естественным. Сохрани характер Санни, его внутренний сарказм и мысли. "
    "Используй динамичные предложения и правильный ритм русского языка.\n\n"
    "Текст для перевода:\n\n{text}"
)


@retry(**RETRY_API)
async def translate_text(text: str) -> str:
    if len(text) > 120000:
        logger.warning("Текст слишком длинный, обрезаем")
        text = text[:120000] + "\n... [обрезано]"

    glossary_section = get_relevant_glossary(text)

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json"
    }
    
    system_prompt = SYSTEM_PROMPT
    if glossary_section:
        system_prompt = glossary_section + "\n\n" + SYSTEM_PROMPT

    payload = {
        "model": "google/gemini-2.5-flash-lite",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(text=text)}
        ],
        "temperature": 0.90,
        "top_p": 0.92,
        "max_tokens": 8192,
        "presence_penalty": 0.15,
        "frequency_penalty": 0.08
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


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5) + wait_exponential(min=2, max=60),
    retry=retry_if_exception_type((aiohttp.ClientError, ConnectionError, Exception)),
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
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post("https://api.telegra.ph/createPage", json=payload, timeout=30) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        url = data["result"]["url"]
                        logger.info(f"Создана страница Telegraph: {url}")
                        return url
                    else:
                        error = data.get('error')
                        logger.error(f"Telegraph error: {data}")
                        if error == 'TITLE_TOO_LONG' and attempt == 0:
                            clean_title_text = clean_title_text[:150] + "..."
                            payload["title"] = clean_title_text
                            continue
                        return None
        except Exception as e:
            logger.exception(f"Ошибка при создании страницы Telegraph: {e}")
            return None
    return None


@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.ERROR),
)
async def process_chapter_translation(ch: Dict[str, str]) -> tuple[Optional[str], bool]:
    cid = ch['id']
    title = ch['title']

    url = await get_cached_telegraph(cid)
    if url:
        logger.info(f"Глава {cid} найдена в кэше")
        return url, True

    try:
        text = await fetch_chapter_text(ch['link'])
    except Exception as e:
        logger.exception(f"Ошибка загрузки главы {cid}")
        return None, False

    translated = await translate_text(text)
    translated_title = await translate_text(title)
    translated_title_clean = clean_title_for_telegraph(translated_title)

    await save_cached_title(cid, translated_title_clean)

    html = text_to_html(translated)
    new_url = await create_telegraph_page(
        title=translated_title_clean,
        content_html=html
    )

    if new_url:
        await save_telegraph_url(cid, new_url)
        return new_url, True
    logger.error(f"Не удалось создать Telegraph для главы {cid}")
    return None, False


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


async def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    subs = await load_subscribers()
    is_subscribed = user_id in subs
    is_admin = ADMIN_ID is not None and str(user_id) == ADMIN_ID

    buttons = [
        [KeyboardButton(text="📌 Моя закладка"), KeyboardButton(text="📖 Выбор главы")],
        [KeyboardButton(text="⬅️ Предыдущая глава"), KeyboardButton(text="➡️ Следующая глава")],
        [KeyboardButton(text="🤝 Поддержать")],
    ]
    row3 = [KeyboardButton(text="❓ Помощь")]
    if is_admin:
        row3.insert(0, KeyboardButton(text="📊 Статус"))
    buttons.append(row3)

    if is_subscribed:
        buttons.append([KeyboardButton(text="❌ Отписаться")])
    else:
        buttons.append([KeyboardButton(text="✅ Подписаться")])

    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


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
    subs = await load_subscribers()
    if uid not in subs:
        subs.add(uid)
        await save_subscribers(subs)
        await message.answer(
            "✅ Подписка оформлена!\n\nИспользуйте кнопки меню для навигации.",
            reply_markup=await get_main_menu(uid)
        )
    else:
        await message.answer("Вы уже подписаны!", reply_markup=await get_main_menu(uid))


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


async def button_subscribe(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    subs = await load_subscribers()
    if uid in subs:
        await message.answer("Вы уже подписаны.", reply_markup=await get_main_menu(uid))
    else:
        subs.add(uid)
        await save_subscribers(subs)
        await message.answer("✅ Вы подписались!", reply_markup=await get_main_menu(uid))


async def button_unsubscribe(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    subs = await load_subscribers()
    if uid in subs:
        subs.remove(uid)
        await save_subscribers(subs)
        await message.answer("❌ Вы отписались.", reply_markup=await get_main_menu(uid))
    else:
        await message.answer("Вы не были подписаны.", reply_markup=await get_main_menu(uid))


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
        await redis_client.delete("first_chapter")
        await callback.answer("Кэш очищен")
        await callback.message.edit_text(
            "✅ Кэш первой главы очищен.",
            reply_markup=admin_status_buttons
        )
    except Exception as e:
        logger.exception("Ошибка очистки кэша")
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
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_show_logs(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    text = "Последние 10 логов:\n" + "\n".join(list(log_buffer)[-10:]) if log_buffer else "Логи отсутствуют."
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
    await callback.answer()


async def admin_user_manage(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.message.edit_text("Выберите действие:", reply_markup=admin_user_manage_buttons)
    await callback.answer()


async def admin_back_to_main(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.message.edit_text("Выберите действие:", reply_markup=admin_status_buttons)
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
    await callback.message.edit_text("Действие отменено.", reply_markup=admin_status_buttons)
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
    logger.info("Бот запущен. Запускаем браузер, глоссарий и мониторинг...")
   
    try:
        await load_glossary_to_redis(force=False)
        count = await redis_client.hlen("glossary:terms")
        logger.info(f"Глоссарий в Redis: {count} терминов")
    except Exception as e:
        logger.error(f"Не удалось загрузить/проверить глоссарий: {e}")
    
    try:
        await launch_browser()
    except Exception as e:
        logger.exception("Ошибка запуска браузера при старте")
        
    _last_activity_time = asyncio.get_event_loop().time()
    asyncio.create_task(monitor())
    asyncio.create_task(keep_browser_alive())
    
    logger.info("Мониторинг и idle-таймер запущены")


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

