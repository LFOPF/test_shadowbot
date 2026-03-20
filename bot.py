import os
import asyncio
import logging
import re
import json
import time
from typing import Optional, Set, List, Dict, Any
from urllib.parse import urlparse
from collections import deque

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
from playwright.async_api import async_playwright, Playwright, BrowserContext
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

# ==================== КОНФИГ ДЛЯ МНОГИХ РОМАНОВ ====================
NOVELS = {
    "1205249": {
        "name": "Shadow Slave",
        "base_url": "https://ranobes.net/chapters/1205249/",
        "max_pages": 120,
        "chapters_per_page": 25,
        "novel_id_in_url": "1205249",
    }
}

CURRENT_NOVEL_ID = "1205249"
TARGET_URL = NOVELS[CURRENT_NOVEL_ID]["base_url"]
CHECK_INTERVAL = 3600
SITE_URL = "https://t.me/SHDSlaveBot"
SITE_NAME = "ShadowSlaveTranslator"
MAX_PAGES = NOVELS[CURRENT_NOVEL_ID]["max_pages"]
CHAPTERS_PER_PAGE = NOVELS[CURRENT_NOVEL_ID]["chapters_per_page"]
TELEGRAPH_TITLE_MAX_LENGTH = 200

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

log_buffer = deque(maxlen=100)

class LogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        log_buffer.append(log_entry)

logging.getLogger().addHandler(LogHandler())
logging.getLogger().setLevel(logging.INFO)

redis_client: Optional[redis.Redis] = None
bot: Optional[Bot] = None
playwright_instance: Optional[Playwright] = None
browser_context: Optional[BrowserContext] = None
PLAYWRIGHT_SEMAPHORE: Optional[asyncio.Semaphore] = None

PREMIUM_KEY = "premium_users"

# ======================== FSM ========================
class ChapterSelection(StatesGroup):
    waiting_for_chapter = State()

class AdminActions(StatesGroup):
    waiting_for_user_id = State()
    action_type = State()

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
        title = title[:TELEGRAPH_TITLE_MAX_LENGTH-3] + "..."
    return title

def text_to_html(text: str) -> str:
    paragraphs = text.split('\n\n')
    return ''.join(f"<p>{p.replace('\n', '<br>')}</p>" for p in paragraphs if p.strip())

def get_page_url(page_num: int = 1) -> str:
    if page_num == 1:
        return TARGET_URL
    base = TARGET_URL.rstrip('/')
    return f"{base}/page/{page_num}/"

# ======================== PREMIUM + RATE LIMIT ========================
async def is_premium(user_id: int) -> bool:
    try:
        value = await redis_client.hget(PREMIUM_KEY, str(user_id))
        if not value:
            return False
        return int(value.decode()) > time.time()
    except Exception as e:
        logger.error(f"is_premium error: {e}")
        return False

async def add_premium(user_id: int, days: int = 30):
    try:
        expire = time.time() + days * 86400
        await redis_client.hset(PREMIUM_KEY, str(user_id), str(int(expire)))
        logger.info(f"Премиум добавлен пользователю {user_id} на {days} дней")
    except Exception as e:
        logger.error(f"add_premium error: {e}")

async def check_rate_limit(user_id: int) -> bool:
    if await is_premium(user_id):
        return True
    key = f"rate_limit:{user_id}:hour"
    count = await redis_client.incr(key)
    if count == 1:
        await redis_client.expire(key, 3600)
    if count > 3:
        return False
    return True

# ======================== НОВАЯ ФУНКЦИЯ: ОСТАВШЕЕСЯ ВРЕМЯ ПРЕМИУМА ========================
async def get_premium_remaining(user_id: int) -> str:
    try:
        value = await redis_client.hget(PREMIUM_KEY, str(user_id))
        if not value:
            return "У вас нет активной премиум-подписки.\n\n💎 Нажми «Стать премиум», чтобы оформить."

        expire = float(value.decode())
        now = time.time()

        if expire <= now:
            # await redis_client.hdel(PREMIUM_KEY, str(user_id))  # можно раскомментировать для автоочистки
            return "Премиум истёк.\n\n💎 Оформи новую подписку через кнопку «Стать премиум»."

        remaining = expire - now
        days = int(remaining // 86400)
        hours = int((remaining % 86400) // 3600)
        minutes = int((remaining % 3600) // 60)

        if days >= 1:
            return f"Твой премиум действителен еще: {days}д и {hours}ч"
        elif hours >= 1:
            return f"Твой премиум действителен еще: {hours}ч и {minutes}мин"
        else:
            return f"Твой премиум действителен еще: {minutes}мин"

    except Exception as e:
        logger.error(f"get_premium_remaining error: {e}")
        return "Ошибка проверки статуса премиум."

# ======================== REDIS ========================
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
        return set(json.loads(data.decode())) if data else set()
    except Exception:
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
    except Exception:
        return None

async def save_last_chapter(ch_id: str):
    try:
        await redis_client.set("last_chapter", ch_id)
    except Exception as e:
        logger.error(f"save_last_chapter error: {e}")

async def get_latest_chapter_id() -> Optional[str]:
    cached = await get_last_chapter()
    if cached:
        return cached
    html = await fetch_html(get_page_url(1))
    if not html:
        return None
    chapters = parse_chapters(html)
    if not chapters:
        return None
    latest = chapters[0]['id']
    await save_last_chapter(latest)
    return latest

# ======================== БЛОКИРОВКИ ========================
blocked_users_key = "blocked_users"

async def is_user_blocked(user_id: int) -> bool:
    try:
        return await redis_client.sismember(blocked_users_key, str(user_id))
    except Exception:
        return False

async def block_user(user_id: int):
    await redis_client.sadd(blocked_users_key, str(user_id))
    await remove_subscriber(user_id)

async def unblock_user(user_id: int):
    await redis_client.srem(blocked_users_key, str(user_id))

async def remove_subscriber(user_id: int) -> bool:
    subs = await load_subscribers()
    if user_id in subs:
        subs.remove(user_id)
        await save_subscribers(subs)
        return True
    return False

# ======================== ЗАКЛАДКИ ========================
async def get_user_bookmark(user_id: int) -> Optional[str]:
    try:
        value = await redis_client.hget("user_bookmarks", str(user_id))
        return value.decode() if value else None
    except Exception:
        return None

async def save_user_bookmark(user_id: int, chapter_id: str):
    await redis_client.hset("user_bookmarks", str(user_id), chapter_id)

# ======================== PLAYWRIGHT ========================
async def fetch_html(url: str, retries: int = 2) -> str:
    async with PLAYWRIGHT_SEMAPHORE:
        for attempt in range(retries):
            page = await browser_context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_selector('a:has-text("Chapter")', timeout=30000)
                await page.wait_for_timeout(2000)
                return await page.content()
            except Exception as e:
                logger.warning(f"fetch_html error on {url} (попытка {attempt+1}): {e}")
                if attempt == retries - 1:
                    return ""
                await asyncio.sleep(2 ** attempt)
            finally:
                await page.close()
    return ""

async def fetch_chapter_text(url: str) -> str:
    async with PLAYWRIGHT_SEMAPHORE:
        page = await browser_context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector('div.text#arrticle', timeout=30000)
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
            logger.warning(f"fetch_chapter_text {url}: {e}")
            return f"[Ошибка загрузки: {e}]"
        finally:
            await page.close()

def parse_chapters(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')
    chapters = []
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href']
        cid = extract_chapter_id(text)
        if cid and cid.isdigit():
            link = 'https://ranobes.net' + href if not href.startswith('http') else href
            if NOVELS[CURRENT_NOVEL_ID]["novel_id_in_url"] not in link:
                continue
            chapters.append({
                'id': cid,
                'raw_title': text,
                'title': clean_title(text),
                'link': link
            })
    chapters.sort(key=lambda x: int(x['id']), reverse=True)
    return chapters

async def find_chapter_by_number(chapter_number: int) -> Optional[Dict[str, str]]:
    latest = await get_latest_chapter_id()
    if not latest:
        return await find_chapter_by_number_binary(chapter_number)

    page_estimate = max(1, 1 + (int(latest) - chapter_number) // CHAPTERS_PER_PAGE)
    if page_estimate > MAX_PAGES:
        page_estimate = MAX_PAGES

    url = get_page_url(page_estimate)
    html = await fetch_html(url)
    if html:
        chapters = parse_chapters(html)
        for ch in chapters:
            if int(ch['id']) == chapter_number:
                return ch
    return await find_chapter_by_number_binary(chapter_number)

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

# ======================== ПЕРЕВОД ========================
SYSTEM_PROMPT = (
    "Ты профессиональный переводчик художественной литературы. "
    "Переводи точно и сохраняй стиль. "
    "НЕ ДОБАВЛЯЙ никаких пояснений, примечаний, комментариев в скобках. "
    "Передавай ТОЛЬКО переведённый текст, без лишних слов."
)

USER_PROMPT_TEMPLATE = (
    "Переведи следующий текст на русский язык. "
    "Сохрани абзацы и форматирование. "
    "Не добавляй ничего от себя, только перевод. "
    "Текст:\n\n{text}"
)

async def translate_text(text: str, retries: int = 3) -> str:
    if len(text) > 120000:
        text = text[:120000] + "\n... [обрезано]"

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json"
    }
    payload = {
        "model": "stepfun/step-3.5-flash:free",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(text=text)}
        ],
        "temperature": 0.3,
        "max_tokens": 8000
    }

    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=120
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data["choices"][0]["message"]["content"].strip()
                    elif resp.status == 429:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        return f"[Ошибка перевода {resp.status}]"
        except Exception:
            if attempt == retries - 1:
                return "[Не удалось перевести]"
            await asyncio.sleep(2 ** attempt)
    return "[Не удалось перевести]"

# ======================== TELEGRAPH ========================
async def create_telegraph_page(title: str, content_html: str) -> Optional[str]:
    clean_title_text = clean_title_for_telegraph(title)
    payload = {
        "access_token": TELEGRAPH_ACCESS_TOKEN,
        "title": clean_title_text,
        "author_name": "Shadow Slave Bot",
        "content": [{"tag": "p", "children": [content_html]}],
    }

    for attempt in range(2):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post("https://api.telegra.ph/createPage", json=payload, timeout=30) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return data["result"]["url"]
                    if data.get('error') == 'TITLE_TOO_LONG' and attempt == 0:
                        payload["title"] = clean_title_text[:150] + "..."
                        continue
        except Exception as e:
            logger.exception(f"Telegraph error: {e}")
    return None

# ======================== ОБРАБОТКА ПЕРЕВОДА ГЛАВЫ ========================
async def process_chapter_translation(ch: Dict[str, str]) -> tuple[Optional[str], bool]:
    cid = ch['id']
    if url := await get_cached_telegraph(cid):
        return url, True

    text = await fetch_chapter_text(ch['link'])
    translated = await translate_text(text)
    translated_title = await translate_text(ch['title'])
    translated_title_clean = clean_title_for_telegraph(translated_title)

    await save_cached_title(cid, translated_title_clean)

    html = text_to_html(translated)
    new_url = await create_telegraph_page(translated_title_clean, html)

    if new_url:
        await save_telegraph_url(cid, new_url)
        return new_url, True
    return None, False

# ======================== РАССЫЛКА ========================
async def notify_all_subscribers(text: str, parse_mode: str = "HTML"):
    subs = await load_subscribers()
    semaphore = asyncio.Semaphore(10)

    async def send(uid):
        if await is_user_blocked(uid):
            return
        async with semaphore:
            try:
                await bot.send_message(uid, text, parse_mode=parse_mode, disable_web_page_preview=True)
            except (TelegramForbiddenError, TelegramBadRequest):
                await remove_subscriber(uid)
            except Exception as e:
                logger.error(f"Ошибка отправки {uid}: {e}")

    await asyncio.gather(*[send(uid) for uid in subs], return_exceptions=True)

# ======================== КЛАВИАТУРЫ ========================
async def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    subs = await load_subscribers()
    is_subscribed = user_id in subs
    is_admin = ADMIN_ID is not None and str(user_id) == ADMIN_ID

    buttons = [
        [KeyboardButton(text="📌 Моя закладка"), KeyboardButton(text="📖 Выбор главы")],
        [KeyboardButton(text="⬅️ Предыдущая"), KeyboardButton(text="➡️ Следующая")],
        [KeyboardButton(text="💎 Премиум")],
    ]

    if is_admin:
        buttons.append([KeyboardButton(text="📊 Статус")])

    buttons.append([KeyboardButton(text="❌ Отписаться" if is_subscribed else "✅ Подписаться")])
    buttons.append([KeyboardButton(text="❓ Помощь")])

    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие…"
    )

cancel_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True
)

premium_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Стать премиум")],
        [KeyboardButton(text="Мой премиум")],
        [KeyboardButton(text="← Назад в главное меню")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Премиум-раздел"
)

    
admin_status_buttons = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Очистить кэш", callback_data="admin_clear_cache")],
        [InlineKeyboardButton(text="🚀 Принудительная проверка", callback_data="admin_force_check")],
        [InlineKeyboardButton(text="💎 Добавить премиум", callback_data="admin_add_premium")],
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

# ======================== ХЕНДЛЕРЫ ========================
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    subs = await load_subscribers()
    if uid not in subs:
        subs.add(uid)
        await save_subscribers(subs)
    await message.answer("✅ Добро пожаловать!", reply_markup=await get_main_menu(uid))

async def button_premium_section(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return

    await message.answer(
        "💎 Премиум-раздел",
        reply_markup=premium_menu
    )

async def button_my_premium(message: types.Message):
    status_text = await get_premium_remaining(message.from_user.id)
    await message.answer(
        status_text,
        reply_markup=premium_menu  # остаёмся в премиум-меню
    )

async def button_back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Вернулись в главное меню",
        reply_markup=await get_main_menu(message.from_user.id)
    )

async def button_subscribe(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы и не можете подписаться.")
        return
    subs = await load_subscribers()
    if uid in subs:
        await message.answer(
            "Вы уже подписаны на уведомления.",
            reply_markup=await get_main_menu(uid)
        )
    else:
        subs.add(uid)
        await save_subscribers(subs)
        await message.answer(
            "✅ Вы подписались на уведомления о новых главах!",
            reply_markup=await get_main_menu(uid)
        )

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
        await message.answer(
            "❌ Вы отписались от уведомлений.",
            reply_markup=await get_main_menu(uid)
        )
    else:
        await message.answer(
            "Вы не были подписаны.",
            reply_markup=await get_main_menu(uid)
        )

async def button_status(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if ADMIN_ID is None or str(uid) != ADMIN_ID:
        await message.answer("У вас нет доступа к этой функции.")
        return
    await message.answer(
        "Выберите действие:",
        reply_markup=admin_status_buttons
    )

async def button_become_premium(message: types.Message):
    await message.answer(
        "💎 <b>Премиум-подписка</b>\n\n"
        "💎 Безлимит глав\n"
        "💎 Приоритетный перевод\n"
        "💎 Доступ к будущим романам\n\n"
        "👉 Оплатить: https://boosty.to/1h8u\n\n"
        "После оплаты напишите администратору (@admin_username) — активируем за 1–5 минут.",
        parse_mode="HTML",
        reply_markup=premium_menu  # остаёмся в премиум-меню
    )

async def button_choose_chapter(message: types.Message, state: FSMContext):
    if await is_user_blocked(message.from_user.id):
        await message.answer("Вы заблокированы.")
        return
    await state.clear()
    await state.set_state(ChapterSelection.waiting_for_chapter)
    await message.answer(
        "Введите номер главы для перевода:",
        reply_markup=cancel_keyboard
    )

async def process_chapter_number(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        await state.clear()
        return

    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer(
            "Ввод отменён.",
            reply_markup=await get_main_menu(uid)
        )
        return

    if not message.text.isdigit():
        await message.answer(
            "Пожалуйста, введите число.",
            reply_markup=cancel_keyboard
        )
        return

    if not await check_rate_limit(uid):
        await message.answer(
            "❌ Лимит 3 главы в час для бесплатных пользователей.\n💎 Купи премиум!",
            reply_markup=await get_main_menu(uid)
        )
        await state.clear()
        return

    chapter_num = int(message.text)
    status_msg = await message.answer(f"🔍 Ищу перевод главы {chapter_num}...")

    cached_url = await get_cached_telegraph(str(chapter_num))
    if cached_url:
        await status_msg.delete()
        await message.answer(
            f"📖 <b>Глава {chapter_num}</b>\n\n🔗 {cached_url}",
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        await save_user_bookmark(uid, str(chapter_num))
        await state.clear()
        return

    chapter = await find_chapter_by_number(chapter_num)
    if not chapter:
        await status_msg.delete()
        await message.answer(
            f"Глава с номером {chapter_num} не найдена.",
            reply_markup=await get_main_menu(uid)
        )
        await state.clear()
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {chapter_num}...")
    try:
        url, success = await process_chapter_translation(chapter)
        await status_msg.delete()
        if success and url:
            await message.answer(
                f"📖 <b>{chapter['title']}</b>\n\n🔗 {url}",
                parse_mode="HTML",
                reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, chapter['id'])
        else:
            await message.answer(
                "❌ Не удалось создать перевод. Попробуйте позже.",
                reply_markup=await get_main_menu(uid)
            )
    except Exception as e:
        logger.exception(f"process_chapter_number error: {e}")
        await message.answer(
            "❌ Ошибка при обработке главы.",
            reply_markup=await get_main_menu(uid)
        )
    finally:
        await state.clear()

async def button_bookmark(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    if not await check_rate_limit(uid):
        await message.answer("❌ Лимит 3 главы в час для бесплатных.\n💎 Купи премиум!", reply_markup=await get_main_menu(uid))
        return

    await state.clear()
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer(
            "У вас ещё нет закладки. Выберите главу через кнопку «📖 Выбор главы».",
            reply_markup=await get_main_menu(uid)
        )
        return

    cached_url = await get_cached_telegraph(bookmark)
    if cached_url:
        await message.answer(
            f"📖 <b>Глава {bookmark}</b>\n\n🔗 {cached_url}",
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        await save_user_bookmark(uid, bookmark)
        return

    status_msg = await message.answer(f"🔍 Ищу перевод главы {bookmark}...")
    chapter = await find_chapter_by_number(int(bookmark))
    if not chapter:
        await status_msg.delete()
        await message.answer(
            "Закладка указывает на несуществующую главу.",
            reply_markup=await get_main_menu(uid)
        )
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {bookmark}...")
    try:
        url, success = await process_chapter_translation(chapter)
        await status_msg.delete()
        if success and url:
            await message.answer(
                f"📖 <b>{chapter['title']}</b>\n\n🔗 {url}",
                parse_mode="HTML",
                reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, chapter['id'])
        else:
            await message.answer(
                "❌ Не удалось создать перевод.",
                reply_markup=await get_main_menu(uid)
            )
    except Exception as e:
        logger.exception(f"button_bookmark error: {e}")
        await message.answer(
            "❌ Ошибка при загрузке главы.",
            reply_markup=await get_main_menu(uid)
        )

async def button_prev(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    if not await check_rate_limit(uid):
        await message.answer("❌ Лимит 3 главы в час для бесплатных.\n💎 Купи премиум!", reply_markup=await get_main_menu(uid))
        return
    await state.clear()
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer(
            "У вас нет закладки. Сначала выберите главу через кнопку «📖 Выбор главы».",
            reply_markup=await get_main_menu(uid)
        )
        return

    current = int(bookmark)
    prev_num = current - 1
    if prev_num < 1:
        await message.answer(
            "Это первая глава. Предыдущей не существует.",
            reply_markup=await get_main_menu(uid)
        )
        return

    cached_url = await get_cached_telegraph(str(prev_num))
    if cached_url:
        await message.answer(
            f"📖 <b>Глава {prev_num}</b>\n\n🔗 {cached_url}",
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        await save_user_bookmark(uid, str(prev_num))
        return

    status_msg = await message.answer(f"🔍 Ищу перевод главы {prev_num}...")
    chapter = await find_chapter_by_number(prev_num)
    if not chapter:
        await status_msg.delete()
        await message.answer(
            f"Глава {prev_num} не найдена.",
            reply_markup=await get_main_menu(uid)
        )
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {prev_num}...")
    try:
        url, success = await process_chapter_translation(chapter)
        await status_msg.delete()
        if success and url:
            await message.answer(
                f"📖 <b>{chapter['title']}</b>\n\n🔗 {url}",
                parse_mode="HTML",
                reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, chapter['id'])
        else:
            await message.answer(
                "❌ Не удалось создать перевод.",
                reply_markup=await get_main_menu(uid)
            )
    except Exception as e:
        logger.exception(f"button_prev error: {e}")
        await message.answer(
            "❌ Ошибка при загрузке главы.",
            reply_markup=await get_main_menu(uid)
        )

async def button_next(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if await is_user_blocked(uid):
        await message.answer("Вы заблокированы.")
        return
    if not await check_rate_limit(uid):
        await message.answer("❌ Лимит 3 главы в час для бесплатных.\n💎 Купи премиум!", reply_markup=await get_main_menu(uid))
        return
    await state.clear()
    bookmark = await get_user_bookmark(uid)
    if not bookmark:
        await message.answer(
            "У вас нет закладки. Сначала выберите главу через кнопку «📖 Выбор главы».",
            reply_markup=await get_main_menu(uid)
        )
        return

    current = int(bookmark)
    next_num = current + 1

    cached_url = await get_cached_telegraph(str(next_num))
    if cached_url:
        await message.answer(
            f"📖 <b>Глава {next_num}</b>\n\n🔗 {cached_url}",
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        await save_user_bookmark(uid, str(next_num))
        return

    status_msg = await message.answer(f"🔍 Ищу перевод главы {next_num}...")
    chapter = await find_chapter_by_number(next_num)
    if not chapter:
        await status_msg.delete()
        await message.answer(
            f"Глава {next_num} не найдена.",
            reply_markup=await get_main_menu(uid)
        )
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {next_num}...")
    try:
        url, success = await process_chapter_translation(chapter)
        await status_msg.delete()
        if success and url:
            await message.answer(
                f"📖 <b>{chapter['title']}</b>\n\n🔗 {url}",
                parse_mode="HTML",
                reply_markup=await get_main_menu(uid)
            )
            await save_user_bookmark(uid, chapter['id'])
        else:
            await message.answer(
                "❌ Не удалось создать перевод.",
                reply_markup=await get_main_menu(uid)
            )
    except Exception as e:
        logger.exception(f"button_next error: {e}")
        await message.answer(
            "❌ Ошибка при загрузке главы.",
            reply_markup=await get_main_menu(uid)
        )

async def button_help(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    is_admin = ADMIN_ID is not None and str(uid) == ADMIN_ID
    help_text = (
        "🤖 Доступные действия через кнопки:\n"
        "📌 Моя закладка – показать перевод текущей сохранённой главы\n"
        "📖 Выбор главы – ввести номер главы и получить перевод\n"
        "⬅️ Предыдущая глава – перевод предыдущей главы (относительно закладки)\n"
        "➡️ Следующая глава – перевод следующей главы (относительно закладки)\n"
    )
    if is_admin:
        help_text += "📊 Статус – дополнительные административные функции\n"
    help_text += "✅ Подписаться / ❌ Отписаться – управление уведомлениями\n"
    help_text += "❓ Помощь – это сообщение"

    await message.answer(help_text, reply_markup=await get_main_menu(uid))

async def handle_other_text(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer(
            "Ввод отменён. Используйте кнопки меню.",
            reply_markup=await get_main_menu(message.from_user.id)
        )
    else:
        uid = message.from_user.id
        await message.answer(
            "Пожалуйста, используйте кнопки меню для взаимодействия с ботом.",
            reply_markup=await get_main_menu(uid)
        )

# ======================== АДМИН ХЕНДЛЕРЫ ========================
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
        await callback.answer("Ошибка при очистке", show_alert=True)

async def admin_force_check(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.answer("Запускаю принудительную проверку...")
    msg = await callback.message.edit_text("🔄 Запущена принудительная проверка...", reply_markup=admin_status_buttons)
    asyncio.create_task(force_monitor_run(msg))

async def admin_show_subscribers(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    subs = await load_subscribers()
    text = f"Всего подписчиков: {len(subs)}\nПервые 20: {list(subs)[:20]}"
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
    await callback.answer()

async def admin_show_logs(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    logs = list(log_buffer)[-10:]
    text = "Последние 10 записей лога:\n" + "\n".join(logs)
    await callback.message.edit_text(text[:4096], reply_markup=admin_status_buttons)
    await callback.answer()

async def admin_user_manage(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.message.edit_text(
        "Выберите действие с пользователем:",
        reply_markup=admin_user_manage_buttons
    )
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
        "Введите ID пользователя (число):",
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

async def admin_add_premium(callback: types.CallbackQuery, state: FSMContext):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await state.set_state(AdminActions.waiting_for_user_id)
    await state.update_data(action_type="premium")
    msg = await callback.message.edit_text(
        "Введите ID пользователя и количество дней через пробел\nПример: 123456789 30",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]])
    )
    await state.update_data(request_msg_id=msg.message_id)
    await callback.answer()

async def process_admin_user_id(message: types.Message, state: FSMContext):
    if not message.text.strip().replace(' ', '').isdigit() and ' ' not in message.text:
        await message.answer("Некорректный формат.")
        return
    data = await state.get_data()
    action = data.get("action_type")
    request_msg_id = data.get("request_msg_id")
    response = ""

    if action == "premium":
        try:
            uid_str, days_str = message.text.split()
            uid = int(uid_str)
            days = int(days_str)
            await add_premium(uid, days)
            response = f"✅ Премиум добавлен пользователю {uid} на {days} дней"
        except Exception:
            response = "Неверный формат. Пример: 123456789 30"
    elif action == "block":
        await block_user(int(message.text))
        response = f"Пользователь {message.text} заблокирован"
    elif action == "unblock":
        await unblock_user(int(message.text))
        response = f"Пользователь {message.text} разблокирован"
    elif action == "remove":
        removed = await remove_subscriber(int(message.text))
        response = f"Пользователь {message.text} {'удалён' if removed else 'не найден'} из подписчиков"
    else:
        response = "Неизвестное действие."

    await state.clear()
    if request_msg_id:
        try:
            await message.bot.delete_message(message.chat.id, request_msg_id)
        except Exception:
            pass
    await message.answer(response, reply_markup=admin_status_buttons)

async def admin_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.", reply_markup=admin_status_buttons)
    await callback.answer()

# ======================== ПРИНУДИТЕЛЬНЫЙ МОНИТОРИНГ ========================
async def force_monitor_run(msg: types.Message):
    logger.info("Принудительный запуск мониторинга")
    try:
        await monitor(check_once=True)
        await msg.edit_text("✅ Принудительная проверка завершена.", reply_markup=admin_status_buttons)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}", reply_markup=admin_status_buttons)

# ======================== МОНИТОРИНГ ========================
async def monitor(check_once=False):
    logger.info("Мониторинг запущен")
    while True:
        try:
            html = await fetch_html(TARGET_URL)
            chapters = parse_chapters(html)
            last_str = await get_last_chapter()
            last_int = int(last_str) if last_str and last_str.isdigit() else 0

            new_chapters = [ch for ch in reversed(chapters) if int(ch['id']) > last_int]

            if new_chapters:
                translated_urls = []
                for ch in new_chapters:
                    url, success = await process_chapter_translation(ch)
                    if success and url:
                        translated_urls.append((ch['id'], url))
                    await asyncio.sleep(8)

                if translated_urls:
                    for cid, url in translated_urls:
                        await notify_all_subscribers(
                            f"📢 <b>Новая глава!</b>\n\n📖 Глава {cid}\n🔗 {url}",
                            parse_mode="HTML"
                        )
                max_id = max(int(ch['id']) for ch in new_chapters)
                await save_last_chapter(str(max_id))

        except Exception as e:
            logger.exception(f"Критическая ошибка мониторинга: {e}")
            await asyncio.sleep(60)

        if check_once:
            break
        await asyncio.sleep(CHECK_INTERVAL)

# ======================== LIFECYCLE ========================
async def on_startup():
    global playwright_instance, browser_context, PLAYWRIGHT_SEMAPHORE
    logger.info("Инициализация Playwright...")
    playwright_instance = await async_playwright().start()
    browser = await playwright_instance.chromium.launch(headless=True)
    browser_context = await browser.new_context(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    )
    PLAYWRIGHT_SEMAPHORE = asyncio.Semaphore(4)
    logger.info("Playwright готов (semaphore = 4)")
    asyncio.create_task(monitor())

async def on_shutdown():
    logger.info("Остановка бота...")
    if browser_context:
        await browser_context.close()
    if playwright_instance:
        await playwright_instance.stop()
    if redis_client:
        await redis_client.aclose()

# ======================== ЗАПУСК ========================
async def main():
    global redis_client, bot, PLAYWRIGHT_SEMAPHORE
    redis_client = await redis.from_url(REDIS_URL)
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=RedisStorage(redis_client))

    # Регистрация всех хендлеров
    dp.message.register(button_premium_section, lambda m: m.text == "💎 Премиум")
    dp.message.register(process_chapter_number, ChapterSelection.waiting_for_chapter)
    dp.message.register(process_admin_user_id, AdminActions.waiting_for_user_id)
    dp.message.register(button_my_premium, lambda m: m.text == "💎 Мой премиум")
    dp.message.register(button_become_premium, lambda m: m.text == "💎 Стать премиум")
    dp.message.register(button_choose_chapter, lambda m: m.text == "📖 Выбор главы")
    dp.message.register(button_bookmark, lambda m: m.text == "📌 Моя закладка")
    dp.message.register(button_prev, lambda m: m.text == "⬅️ Предыдущая глава")
    dp.message.register(button_next, lambda m: m.text == "➡️ Следующая глава")
    dp.message.register(button_status, lambda m: m.text == "📊 Статус")
    dp.message.register(button_help, lambda m: m.text == "❓ Помощь")
    dp.message.register(button_subscribe, lambda m: m.text == "✅ Подписаться")
    dp.message.register(button_unsubscribe, lambda m: m.text == "❌ Отписаться")
    dp.message.register(handle_other_text)
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(button_back_to_main, lambda m: m.text == "← Назад в главное меню")

    # Callback'ы
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
    dp.callback_query.register(admin_add_premium, lambda c: c.data == "admin_add_premium")
    dp.callback_query.register(admin_cancel, lambda c: c.data == "admin_cancel")

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
