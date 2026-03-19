import os
import asyncio
import logging
import re
import json
from typing import Optional, Set, List, Dict, Any
from urllib.parse import urlparse

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

if not all([BOT_TOKEN, OPENROUTER_API_KEY, TELEGRAPH_ACCESS_TOKEN, REDIS_URL]):
    raise ValueError("Не заданы все обязательные переменные окружения")

TARGET_URL = "https://ranobes.net/chapters/1205249/"
CHECK_INTERVAL = 3600  # 1 час
SITE_URL = "https://t.me/SHDSlaveBot"
SITE_NAME = "ShadowSlaveTranslator"
MAX_PAGES = 120  # Максимальное количество страниц для поиска (с сайта)
CHAPTERS_PER_PAGE = 25  # Количество глав на одной странице

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ======================== ГЛОБАЛЬНЫЕ ОБЪЕКТЫ ========================
redis_client: Optional[redis.Redis] = None
bot: Optional[Bot] = None
playwright_instance: Optional[Playwright] = None
browser_context: Optional[BrowserContext] = None

# ======================== FSM СОСТОЯНИЯ ========================
class ChapterSelection(StatesGroup):
    waiting_for_choice = State()          # ожидание выбора типа (оригинал/перевод)
    waiting_for_number = State()           # ожидание номера главы

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
    # Удаляем в конце строки временные метки типа "X minutes/hours/days/weeks/months/years ago"
    return re.sub(
        r'\s*\d+\s*(?:minute|hour|day|week|month|year)s?\s+ago$',
        '',
        raw_title,
        flags=re.IGNORECASE
    ).strip()


def text_to_html(text: str) -> str:
    paragraphs = text.split('\n\n')
    return ''.join(f"<p>{p.replace('\n', '<br>')}</p>" for p in paragraphs if p.strip())


def get_page_url(page_num: int) -> str:
    """Возвращает URL страницы с главами по номеру страницы."""
    if page_num == 1:
        return TARGET_URL
    base = TARGET_URL.rstrip('/')
    return f"{base}/page/{page_num}/"


def get_telegraph_path(url: str) -> str:
    """Извлекает путь из URL Telegraph (последняя часть)."""
    return urlparse(url).path.split('/')[-1]


# ======================== REDIS (общие данные) с декодированием ========================
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
    """Возвращает переведённое название главы из хеша chapter_titles."""
    try:
        value = await redis_client.hget("chapter_titles", chapter_id)
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_cached_title error: {e}")
        return None


async def save_cached_title(chapter_id: str, title: str):
    """Сохраняет переведённое название главы в хеш chapter_titles."""
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
    """Возвращает номер самой новой главы (из первой страницы или Redis)."""
    try:
        cached = await redis_client.get("first_chapter")
        if cached:
            return int(cached.decode())
    except Exception as e:
        logger.error(f"get_first_chapter cached error: {e}")

    # Если нет в кэше, загружаем первую страницу
    html = await fetch_html(TARGET_URL)
    if not html:
        return None
    chapters = parse_chapters(html)
    if not chapters:
        return None
    first = int(chapters[0]['id'])
    try:
        await redis_client.set("first_chapter", str(first), ex=3600)  # кэш на час
    except Exception as e:
        logger.error(f"save_first_chapter error: {e}")
    return first


# ======================== КЭШ СТРАНИЦ (теперь в хеше) ========================
async def get_cached_page(page_num: int) -> Optional[str]:
    try:
        value = await redis_client.hget("page_cache", str(page_num))
        return value.decode() if value else None
    except Exception as e:
        logger.error(f"get_cached_page error: {e}")
        return None


async def save_page_cache(page_num: int, html: str):
    try:
        await redis_client.hset("page_cache", str(page_num), html)
    except Exception as e:
        logger.error(f"save_page_cache error: {e}")


# ======================== REDIS (закладки пользователей) ========================
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


# ======================== PLAYWRIGHT (глобальный браузер) ========================
async def fetch_html(url: str, retries: int = 2) -> str:
    """Загружает HTML страницы через Playwright с повторными попытками."""
    if not playwright_instance or not browser_context:
        raise RuntimeError("Playwright не инициализирован")

    for attempt in range(retries):
        page = await browser_context.new_page()
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_selector('a:has-text("Chapter")', timeout=20000)
            return await page.content()
        except Exception as e:
            logger.warning(f"fetch_html error on {url} (попытка {attempt+1}): {e}")
            if attempt == retries - 1:
                return ""
            await asyncio.sleep(2 ** attempt)
        finally:
            await page.close()
    return ""


async def fetch_html_cached(url: str, page_num: int) -> str:
    """Загружает страницу с кэшированием."""
    cached = await get_cached_page(page_num)
    if cached:
        return cached
    html = await fetch_html(url)
    if html:
        await save_page_cache(page_num, html)
    return html


async def fetch_chapter_text(url: str) -> str:
    """Загружает текст конкретной главы через Playwright."""
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
            link = 'https://ranobes.net' + href if not href.startswith('http') else href
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
    """
    Ускоренный поиск главы с математическим расчётом страницы и кэшированием.
    """
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
    html = await fetch_html_cached(url, page_estimate)
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
        html = await fetch_html_cached(url, mid)
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
async def translate_text(text: str, retries: int = 3) -> str:
    if len(text) > 120000:
        logger.warning("Текст слишком длинный, обрезаем")
        text = text[:120000] + "\n... [обрезано]"

    prompt = f"Переведи следующий текст художественной литературы на русский язык. Сохрани абзацы и форматирование. Текст:\n\n{text}"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json"
    }
    payload = {
        "model": "stepfun/step-3.5-flash:free",
        "messages": [
            {"role": "system", "content": "Ты профессиональный переводчик художественной литературы. Переводи точно и сохраняй стиль."},
            {"role": "user", "content": prompt}
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


# ======================== TELEGRAPH (создание страницы с водяным знаком) ========================
async def create_telegraph_page(
    title: str,
    content_html: str,
    author: str = "Shadow Slave Bot",
    chapter_number: Optional[str] = None,
    user_id: Optional[int] = None
) -> Optional[str]:
    """
    Создаёт страницу Telegraph с водяным знаком (user_id в комментарии).
    """
    # Добавляем номер главы в начале
    header = f"<p><b>Глава {chapter_number}</b></p>" if chapter_number else ""

    # Добавляем водяной знак в виде HTML-комментария
    watermark = f"<!-- uid:{user_id} -->" if user_id else ""

    full_html = header + content_html + watermark

    # Конвертируем в узлы Telegraph (упрощённо)
    soup = BeautifulSoup(full_html, 'html.parser')
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
        elif isinstance(elem, str) and elem.strip().startswith('<!--'):
            # Комментарии игнорируются, но мы можем добавить их как текст? Лучше оставить как есть,
            # но Telegraph может не поддерживать комментарии. Вставим как обычный текст с классом hidden.
            # Однако для простоты добавим как отдельный параграф с нулевой высотой? Не будем усложнять.
            # Просто добавим как текст, он будет виден, что плохо. Поэтому используем скрытый span.
            # Сделаем невидимый span:
            nodes.append({
                "tag": "p",
                "children": [{"tag": "span", "attrs": {"style": "display:none"}, "children": [f"uid:{user_id}"]}]
            })
        else:
            # Остальные теги (hr и т.д.) обрабатывать не будем
            pass

    if not nodes:
        nodes = [{"tag": "p", "children": [full_html]}]

    payload = {
        "access_token": TELEGRAPH_ACCESS_TOKEN,
        "title": title,
        "author_name": author,
        "content": nodes,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.telegra.ph/createPage", json=payload, timeout=30) as resp:
                data = await resp.json()
                if data.get("ok"):
                    url = data["result"]["url"]
                    logger.info(f"Создана страница Telegraph: {url} для пользователя {user_id}")
                    return url
                else:
                    logger.error(f"Telegraph error: {data}")
                    return None
    except Exception as e:
        logger.exception(f"Ошибка при создании страницы Telegraph: {e}")
        return None


# ======================== ОБЩАЯ ЛОГИКА ОБРАБОТКИ ГЛАВЫ (перевод) ========================
async def process_chapter_translation(ch: Dict[str, str], user_id: Optional[int] = None) -> tuple[str, bool]:
    """
    Возвращает (текст_сообщения, успех) для переведённой главы.
    """
    cid = ch['id']
    title = ch['title']
    chapter_num = int(cid)

    # Проверяем кэш Telegraph
    url = await get_cached_telegraph(cid)
    if url:
        logger.info(f"Глава {cid} найдена в кэше")
        # Можно также получить переведённый заголовок из кэша, но пока используем оригинальный
        return f"📖 <b>{title}</b>\n\n🔗 {url}", True

    # Загружаем текст главы
    try:
        text = await fetch_chapter_text(ch['link'])
    except Exception as e:
        logger.exception(f"Ошибка загрузки главы {cid}")
        return f"❌ Не удалось загрузить главу {title}\n🔗 Оригинал: {ch['link']}", False

    # Переводим текст и заголовок
    translated = await translate_text(text)
    translated_title = await translate_text(title)

    # Сохраняем переведённый заголовок
    await save_cached_title(cid, translated_title)

    # Создаём страницу с водяным знаком
    html = text_to_html(translated)
    new_url = await create_telegraph_page(
        title=translated_title,
        content_html=html,
        chapter_number=cid,
        user_id=user_id
    )

    if not new_url:
        logger.error(f"Не удалось создать Telegraph для главы {cid}")
        return f"❌ Перевод готов, но не удалось создать страницу.\n🔗 Оригинал: {ch['link']}", False

    # Сохраняем URL
    await save_telegraph_url(cid, new_url)
    return f"📖 <b>{title}</b>\n\n🔗 {new_url}", True


# ======================== РАССЫЛКА ========================
async def notify_all_subscribers(text: str, parse_mode: str = "HTML", reply_markup=None):
    subs = await load_subscribers()
    if not subs:
        logger.info("Нет подписчиков")
        return

    semaphore = asyncio.Semaphore(10)

    async def send_with_limit(uid):
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

    buttons = [
        [KeyboardButton(text="📌 Моя закладка"), KeyboardButton(text="📖 Выбор главы")],
        [KeyboardButton(text="⬅️ Предыдущая глава"), KeyboardButton(text="➡️ Следующая глава")],
        [KeyboardButton(text="📊 Статус"), KeyboardButton(text="❓ Помощь")],
    ]

    if is_subscribed:
        buttons.append([KeyboardButton(text="❌ Отписаться")])
    else:
        buttons.append([KeyboardButton(text="✅ Подписаться")])

    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


choice_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔗 Оригинал"), KeyboardButton(text="📚 Перевод")],
        [KeyboardButton(text="❌ Отмена")]
    ],
    resize_keyboard=True,
    input_field_placeholder="Выберите тип главы"
)


cancel_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
    input_field_placeholder="Введите номер главы или нажмите Отмена"
)


quick_actions = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить статус", callback_data="refresh_status")],
        [InlineKeyboardButton(text="🔗 Все главы", url="https://ranobes.net/chapters/1205249/")],
        [InlineKeyboardButton(text="📢 Поделиться", switch_inline_query="Shadow Slave")]
    ]
)


# ======================== ХЕНДЛЕРЫ ========================
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
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
    subs = await load_subscribers()
    last = await get_last_chapter() or "пока нет"
    text = f"📊 Подписчиков: {len(subs)}\nПоследняя глава: {last}"
    await message.answer(text, reply_markup=quick_actions)


async def refresh_status(callback: types.CallbackQuery):
    subs = await load_subscribers()
    last = await get_last_chapter() or "пока нет"
    text = f"📊 Подписчиков: {len(subs)}\nПоследняя глава: {last}"

    if callback.message.text == text:
        try:
            await callback.answer("Данные актуальны", show_alert=False)
        except TelegramBadRequest:
            pass
    else:
        try:
            await callback.message.edit_text(text, reply_markup=quick_actions)
            await callback.answer()
        except TelegramBadRequest as e:
            if "query is too old" in str(e) or "query ID is invalid" in str(e):
                pass
            else:
                raise


async def button_choose_chapter(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(ChapterSelection.waiting_for_choice)
    await message.answer(
        "Выберите тип главы:",
        reply_markup=choice_keyboard
    )


async def process_choice(message: types.Message, state: FSMContext):
    choice = message.text
    if choice == "❌ Отмена":
        await state.clear()
        await message.answer(
            "Выбор отменён.",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        return

    if choice not in ["🔗 Оригинал", "📚 Перевод"]:
        await message.answer(
            "Пожалуйста, выберите вариант из меню.",
            reply_markup=choice_keyboard
        )
        return

    await state.update_data(choice_type='original' if choice == "🔗 Оригинал" else 'translation')
    last_chapter = await get_last_chapter() or "?"
    await state.set_state(ChapterSelection.waiting_for_number)
    await message.answer(
        f"Введите номер главы (от 1 до {last_chapter}):",
        reply_markup=cancel_keyboard
    )


async def process_chapter_number(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer(
            "Ввод отменён.",
            reply_markup=await get_main_menu(message.from_user.id)
        )
        return

    if not message.text.isdigit():
        await message.answer(
            "Пожалуйста, введите число. Если хотите отменить ввод, нажмите кнопку «❌ Отмена».",
            reply_markup=cancel_keyboard
        )
        return

    chapter_num = int(message.text)
    uid = message.from_user.id

    data = await state.get_data()
    choice_type = data.get('choice_type', 'translation')

    if choice_type == 'translation':
        status_msg = await message.answer(f"🔍 Ищу перевод главы {chapter_num}...")
    else:
        status_msg = await message.answer(f"🔍 Ищу главу {chapter_num}...")

    if choice_type == 'translation':
        cached_url = await get_cached_telegraph(str(chapter_num))
        if cached_url:
            # Можно также получить переведённый заголовок, но для простоты используем оригинальный
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

    if choice_type == 'original':
        await status_msg.delete()
        await message.answer(
            f"📢 <b>{chapter['title']}</b>\n🔗 {chapter['link']}",
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        await state.clear()
        return

    # Перевод
    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {chapter_num}...")
    try:
        result_text, success = await process_chapter_translation(chapter, user_id=uid)
        await status_msg.delete()
        await message.answer(
            result_text,
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        if success:
            await save_user_bookmark(uid, chapter['id'])
    except Exception as e:
        logger.exception(f"process_chapter_number translation error: {e}")
        await message.answer(
            f"❌ Ошибка при обработке главы {chapter['title']}",
            reply_markup=await get_main_menu(uid)
        )
    finally:
        await state.clear()


async def button_bookmark(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
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
            "Закладка указывает на несуществующую главу. Возможно, она была удалена. Установите новую закладку.",
            reply_markup=await get_main_menu(uid)
        )
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {bookmark}...")
    try:
        result_text, success = await process_chapter_translation(chapter, user_id=uid)
        await status_msg.delete()
        await message.answer(
            result_text,
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        if success:
            await save_user_bookmark(uid, chapter['id'])
    except Exception as e:
        logger.exception(f"button_bookmark error: {e}")
        await message.answer(
            "❌ Ошибка при загрузке главы.",
            reply_markup=await get_main_menu(uid)
        )


async def button_prev(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
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
            f"Глава {prev_num} не найдена. Возможно, она ещё не вышла или была пропущена.",
            reply_markup=await get_main_menu(uid)
        )
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {prev_num}...")
    try:
        result_text, success = await process_chapter_translation(chapter, user_id=uid)
        await status_msg.delete()
        await message.answer(
            result_text,
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        if success:
            await save_user_bookmark(uid, chapter['id'])
    except Exception as e:
        logger.exception(f"button_prev error: {e}")
        await message.answer(
            "❌ Ошибка при загрузке главы.",
            reply_markup=await get_main_menu(uid)
        )


async def button_next(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
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
            f"Глава {next_num} не найдена. Возможно, она ещё не вышла.",
            reply_markup=await get_main_menu(uid)
        )
        return

    await status_msg.edit_text(f"📥 Загружаю и перевожу главу {next_num}...")
    try:
        result_text, success = await process_chapter_translation(chapter, user_id=uid)
        await status_msg.delete()
        await message.answer(
            result_text,
            parse_mode="HTML",
            reply_markup=await get_main_menu(uid)
        )
        if success:
            await save_user_bookmark(uid, chapter['id'])
    except Exception as e:
        logger.exception(f"button_next error: {e}")
        await message.answer(
            "❌ Ошибка при загрузке главы.",
            reply_markup=await get_main_menu(uid)
        )


async def button_help(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    await message.answer(
        "🤖 Доступные действия через кнопки:\n"
        "📌 Моя закладка – показать перевод текущей сохранённой главы\n"
        "📖 Выбор главы – выбрать главу (оригинал или перевод)\n"
        "⬅️ Предыдущая глава – перевод предыдущей главы (относительно закладки)\n"
        "➡️ Следующая глава – перевод следующей главы (относительно закладки)\n"
        "📊 Статус – статистика подписчиков\n"
        "✅ Подписаться / ❌ Отписаться – управление уведомлениями (кнопка меняется в зависимости от статуса)\n"
        "❓ Помощь – это сообщение",
        reply_markup=await get_main_menu(uid)
    )


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


# ======================== МОНИТОРИНГ (автоматический перевод) ========================
async def monitor():
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
                for ch in new_chapters:
                    cid = ch['id']
                    logger.info(f"Автоматическая обработка главы {cid}")
                    try:
                        # При автоматическом переводе не передаём user_id (водяной знак не ставится)
                        result_text, success = await process_chapter_translation(ch, user_id=None)
                        await notify_all_subscribers(result_text, parse_mode="HTML")
                    except Exception as e:
                        logger.exception(f"Ошибка обработки новой главы {cid}")
                        fallback = (
                            f"📢 <b>Новая глава!</b>\n"
                            f"<b>{ch['title']}</b>\n\n"
                            f"🔗 Оригинал: {ch['link']}\n"
                            f"(перевод временно недоступен)"
                        )
                        await notify_all_subscribers(fallback, parse_mode="HTML")

                max_id = max(int(ch['id']) for ch in new_chapters)
                await save_last_chapter(str(max_id))
                logger.info(f"last_chapter обновлён до {max_id}")

            else:
                logger.info("Новых глав нет")

        except Exception as e:
            logger.exception(f"Критическая ошибка мониторинга: {e}")
            await asyncio.sleep(60)

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

    dp.message.register(process_choice, ChapterSelection.waiting_for_choice)
    dp.message.register(process_chapter_number, ChapterSelection.waiting_for_number)

    dp.message.register(button_choose_chapter, lambda m: m.text == "📖 Выбор главы")
    dp.message.register(button_bookmark, lambda m: m.text == "📌 Моя закладка")
    dp.message.register(button_prev, lambda m: m.text == "⬅️ Предыдущая глава")
    dp.message.register(button_next, lambda m: m.text == "➡️ Следующая глава")
    dp.message.register(button_status, lambda m: m.text == "📊 Статус")
    dp.message.register(button_help, lambda m: m.text == "❓ Помощь")
    dp.message.register(button_subscribe, lambda m: m.text == "✅ Подписаться")
    dp.message.register(button_unsubscribe, lambda m: m.text == "❌ Отписаться")

    dp.message.register(handle_other_text)

    dp.callback_query.register(refresh_status, lambda c: c.data == "refresh_status")

    dp.message.register(cmd_start, Command("start"))

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
