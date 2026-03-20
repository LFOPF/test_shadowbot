import os
import asyncio
import logging
import re
import json
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
ADMIN_ID = os.getenv("ADMIN_ID")  # Telegram ID администратора

if not all([BOT_TOKEN, OPENROUTER_API_KEY, TELEGRAPH_ACCESS_TOKEN, REDIS_URL]):
    raise ValueError("Не заданы все обязательные переменные окружения")

TARGET_URL = "https://ranobes.net/chapters/1205249/"
CHECK_INTERVAL = 3600
SITE_URL = "https://t.me/SHDSlaveBot"
SITE_NAME = "ShadowSlaveTranslator"
MAX_PAGES = 120
CHAPTERS_PER_PAGE = 25
TELEGRAPH_TITLE_MAX_LENGTH = 200

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Простой буфер для логов (последние 100 строк)
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

# ======================== FSM СОСТОЯНИЯ ========================
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

def get_page_url(page_num: int) -> str:
    if page_num == 1:
        return TARGET_URL
    base = TARGET_URL.rstrip('/')
    return f"{base}/page/{page_num}/"

def get_telegraph_path(url: str) -> str:
    return urlparse(url).path.split('/')[-1]


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


# ======================== БЛОКИРОВКИ ========================
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
        # Также удаляем из подписчиков
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


# ======================== ЗАКЛАДКИ ========================
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


# ======================== PLAYWRIGHT ========================
async def fetch_html(url: str, retries: int = 2) -> str:
    if not playwright_instance or not browser_context:
        raise RuntimeError("Playwright не инициализирован")

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
    if not playwright_instance or not browser_context:
        raise RuntimeError("Playwright не инициализирован")

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
            # Формируем полную ссылку
            link = 'https://ranobes.net' + href if not href.startswith('http') else href
            # Фильтр: ссылка должна вести на страницу глав нашего произведения
            if not link.startswith('https://ranobes.net/chapters/1205249/'):
                logger.debug(f"Пропущена ссылка на чужое произведение: {link}")
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

async def find_chapter_by_number(chapter_number: int) -> Optional[Dict[str, str]]:
    first_chapter = await get_first_chapter()
    if not first_chapter:
        logger.warning("Не удалось получить первую главу, переключаюсь на бинарный поиск")
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

    logger.info(f"Глава не на странице {page_estimate}, запускаю бинарный поиск")
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
        logger.warning("Текст слишком длинный, обрезаем")
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
                        logger.warning(f"Rate limit (попытка {attempt+1})")
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error(f"Перевод {resp.status}: {await resp.text()}")
                        return f"[Ошибка перевода {resp.status}]"
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут перевода (попытка {attempt+1})")
            if attempt == retries - 1:
                return "[Таймаут]"
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.exception(f"Исключение перевода: {e}")
    return "[Не удалось перевести]"


# ======================== TELEGRAPH ========================
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
        else:
            pass

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
                            logger.info(f"Повторная попытка с укороченным заголовком: {clean_title_text}")
                            continue
                        else:
                            return None
        except Exception as e:
            logger.exception(f"Ошибка при создании страницы Telegraph: {e}")
            return None
    return None


# ======================== ОБРАБОТКА ПЕРЕВОДА ГЛАВЫ ========================
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
    else:
        logger.error(f"Не удалось создать Telegraph для главы {cid}")
        return None, False


# ======================== РАССЫЛКА ========================
async def notify_all_subscribers(text: str, parse_mode: str = "HTML", reply_markup=None):
    subs = await load_subscribers()
    if not subs:
        logger.info("Нет подписчиков")
        return

    semaphore = asyncio.Semaphore(10)

    async def send_with_limit(uid):
        if await is_user_blocked(uid):
            logger.info(f"Пользователь {uid} заблокирован, пропускаем")
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
                logger.info(f"Пользователь {uid} недоступен, будет удалён при следующей очистке")
            except Exception as e:
                logger.error(f"Ошибка отправки {uid}: {e}")

    tasks = [send_with_limit(uid) for uid in subs]
    await asyncio.gather(*tasks, return_exceptions=True)


# ======================== КЛАВИАТУРЫ ========================
async def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    subs = await load_subscribers()
    is_subscribed = user_id in subs
    is_admin = ADMIN_ID is not None and str(user_id) == ADMIN_ID

    buttons = [
        [KeyboardButton(text="📌 Моя закладка"), KeyboardButton(text="📖 Выбор главы")],
        [KeyboardButton(text="⬅️ Предыдущая глава"), KeyboardButton(text="➡️ Следующая глава")],
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


# ======================== ХЕНДЛЕРЫ ОСНОВНЫХ ДЕЙСТВИЙ ========================
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
            "✅ Подписка оформлена!\n\n"
            "Используйте кнопки меню для навигации.",
            reply_markup=await get_main_menu(uid)
        )
    else:
        await message.answer(
            "Вы уже подписаны!",
            reply_markup=await get_main_menu(uid)
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


# ======================== ХЕНДЛЕРЫ АДМИНИСТРАТОРА ========================
async def admin_clear_cache(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    try:
        await redis_client.delete("first_chapter")
        await callback.answer("Кэш очищен", show_alert=False)
        await callback.message.edit_text(
            "✅ Кэш первой главы очищен. При следующем поиске будет загружена актуальная информация.",
            reply_markup=admin_status_buttons
        )
    except Exception as e:
        logger.exception("Ошибка очистки кэша")
        await callback.answer("Ошибка при очистке", show_alert=True)

async def admin_force_check(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await callback.answer("Запускаю принудительную проверку...", show_alert=False)
    await callback.message.edit_text("🔄 Запущена принудительная проверка новых глав...", reply_markup=admin_status_buttons)
    asyncio.create_task(force_monitor_run())

async def admin_show_subscribers(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    subs = await load_subscribers()
    if not subs:
        text = "Нет подписчиков."
    else:
        subs_list = list(subs)
        subs_preview = subs_list[:20]
        text = f"Всего подписчиков: {len(subs)}\n"
        text += "Первые 20:\n" + "\n".join(str(uid) for uid in subs_preview)
        if len(subs_list) > 20:
            text += "\n..."
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
    await callback.answer()

async def admin_show_logs(callback: types.CallbackQuery):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    if not log_buffer:
        text = "Логи отсутствуют."
    else:
        logs = list(log_buffer)[-10:]
        text = "Последние 10 записей лога:\n" + "\n".join(logs)
        if len(text) > 4096:
            text = text[:4093] + "..."
    await callback.message.edit_text(text, reply_markup=admin_status_buttons)
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


# ======================== ХЕНДЛЕРЫ ДЛЯ ВВОДА ID ПОЛЬЗОВАТЕЛЯ ========================
async def admin_action_start(callback: types.CallbackQuery, state: FSMContext, action: str):
    if ADMIN_ID is None or str(callback.from_user.id) != ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    await state.set_state(AdminActions.waiting_for_user_id)
    await state.update_data(action_type=action)
    await callback.message.edit_text(
        "Введите ID пользователя (число):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]])
    )
    await callback.answer()

async def admin_block(callback: types.CallbackQuery, state: FSMContext):
    await admin_action_start(callback, state, "block")

async def admin_unblock(callback: types.CallbackQuery, state: FSMContext):
    await admin_action_start(callback, state, "unblock")

async def admin_remove_sub(callback: types.CallbackQuery, state: FSMContext):
    await admin_action_start(callback, state, "remove")

async def process_admin_user_id(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Некорректный ID. Введите число или нажмите Отмена.")
        return
    user_id = int(message.text)
    data = await state.get_data()
    action = data.get("action_type")
    response = ""
    if action == "block":
        await block_user(user_id)
        response = f"Пользователь {user_id} заблокирован и удалён из подписчиков."
    elif action == "unblock":
        await unblock_user(user_id)
        response = f"Пользователь {user_id} разблокирован."
    elif action == "remove":
        removed = await remove_subscriber(user_id)
        if removed:
            response = f"Пользователь {user_id} удалён из подписчиков."
        else:
            response = f"Пользователь {user_id} не найден в подписчиках."
    else:
        response = "Неизвестное действие."
    await state.clear()
    # Отправляем одно сообщение с результатом и меню (без дубляжа)
    await message.answer(response, reply_markup=admin_status_buttons)

async def admin_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.", reply_markup=admin_status_buttons)
    await callback.answer()


# ======================== ХЕНДЛЕРЫ ВЫБОРА ГЛАВЫ ========================
async def button_choose_chapter(message: types.Message, state: FSMContext):
    if await is_user_blocked(message.from_user.id):
        await message.answer("Вы заблокированы.")
        return
    await state.clear()
    await state.set_state(ChapterSelection.waiting_for_chapter)
    last_chapter = await get_last_chapter() or "?"
    await message.answer(
        f"Введите номер главы для перевода (от 1 до {last_chapter}):",
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
            "Пожалуйста, введите число. Если хотите отменить ввод, нажмите кнопку «❌ Отмена».",
            reply_markup=cancel_keyboard
        )
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
            f"Глава с номером {chapter_num} не найдена. Проверьте номер.",
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
        logger.exception(f"process_chapter_number translation error: {e}")
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
            "Закладка указывает на несуществующую главу. Установите новую закладку.",
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


# ======================== ПРИНУДИТЕЛЬНЫЙ ЗАПУСК МОНИТОРИНГА ========================
async def force_monitor_run():
    logger.info("Принудительный запуск мониторинга")
    await monitor(check_once=True)


# ======================== МОНИТОРИНГ ========================
async def monitor(check_once=False):
    logger.info("Мониторинг запущен — автоматический перевод и рассылка включены")
    while True:
        logger.info("Начало проверки")
        try:
            html = await fetch_html(TARGET_URL)
            chapters = parse_chapters(html)
            last_str = await get_last_chapter()
            last_int = int(last_str) if last_str and last_str.isdigit() else 0

            new_chapters = [ch for ch in reversed(chapters) if int(ch['id']) > last_int]

            if new_chapters:
                logger.info(f"Новых глав: {len(new_chapters)}")
                translated_urls = []
                failed_chapters = []

                for i, ch in enumerate(new_chapters):
                    cid = ch['id']
                    logger.info(f"Автоматическая обработка главы {cid}")
                    try:
                        url, success = await process_chapter_translation(ch)
                        if success and url:
                            translated_urls.append((cid, url))
                        else:
                            failed_chapters.append(cid)
                    except Exception as e:
                        logger.exception(f"Ошибка обработки новой главы {cid}")
                        failed_chapters.append(cid)

                    if i < len(new_chapters) - 1:
                        await asyncio.sleep(10)

                if ADMIN_ID:
                    admin_id = int(ADMIN_ID)
                    if translated_urls:
                        msg_lines = ["✅ Переведены новые главы:"]
                        for cid, url in translated_urls:
                            msg_lines.append(f"• Глава {cid}: {url}")
                        await bot.send_message(
                            admin_id,
                            "\n".join(msg_lines),
                            parse_mode="HTML",
                            disable_web_page_preview=True
                        )
                    if failed_chapters:
                        await bot.send_message(
                            admin_id,
                            f"❌ Не удалось перевести главы: {', '.join(map(str, failed_chapters))}",
                            parse_mode="HTML"
                        )

                max_id = max(int(ch['id']) for ch in new_chapters)
                await save_last_chapter(str(max_id))
                logger.info(f"last_chapter обновлён до {max_id}")

                for cid, url in translated_urls:
                    chapter_info = next((ch for ch in new_chapters if ch['id'] == cid), None)
                    if chapter_info:
                        await notify_all_subscribers(
                            f"📢 <b>Новая глава!</b>\n\n📖 <b>{chapter_info['title']}</b>\n\n🔗 {url}",
                            parse_mode="HTML"
                        )
                    else:
                        await notify_all_subscribers(
                            f"📢 <b>Новая глава {cid}!</b>\n\n🔗 {url}",
                            parse_mode="HTML"
                        )

            else:
                logger.info("Новых глав нет")

        except Exception as e:
            logger.exception(f"Критическая ошибка мониторинга: {e}")
            await asyncio.sleep(60)

        if check_once:
            break
        logger.info(f"Проверка завершена. Ожидание {CHECK_INTERVAL} сек")
        await asyncio.sleep(CHECK_INTERVAL)


# ======================== LIFECYCLE ========================
async def on_startup():
    logger.info("Бот запущен. Инициализация Playwright...")
    global playwright_instance, browser_context
    try:
        playwright_instance = await async_playwright().start()
        browser = await playwright_instance.chromium.launch(headless=True)
        browser_context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        logger.info("Playwright готов")
    except Exception as e:
        logger.exception(f"Ошибка инициализации Playwright: {e}")
        raise

    asyncio.create_task(monitor())
    logger.info("Мониторинг запущен")

async def on_shutdown():
    logger.info("Бот остановлен. Закрытие ресурсов...")
    global playwright_instance, browser_context
    if browser_context:
        await browser_context.close()
    if playwright_instance:
        await playwright_instance.stop()
    if redis_client:
        await redis_client.aclose()


# ======================== ЗАПУСК ========================
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
    dp.message.register(button_status, lambda m: m.text == "📊 Статус")
    dp.message.register(button_help, lambda m: m.text == "❓ Помощь")
    dp.message.register(button_subscribe, lambda m: m.text == "✅ Подписаться")
    dp.message.register(button_unsubscribe, lambda m: m.text == "❌ Отписаться")
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

    dp.message.register(cmd_start, Command("start"))

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
