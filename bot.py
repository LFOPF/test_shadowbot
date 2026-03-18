import os
import asyncio
import logging
import re
import json
import aiohttp
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ===== НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN должен быть задан!")
if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY должен быть задан для перевода!")

TARGET_URL = "https://ranobes.net/chapters/1205249/"
CHECK_INTERVAL = 2 * 60 * 60  # 2 часа
LAST_CHAPTER_FILE = "last_chapter.txt"
SUBSCRIBERS_FILE = "subscribers.json"
SITE_URL = "https://t.me/YourBot"
SITE_NAME = "ShadowSlaveTranslator"
# =============================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
def clean_title(raw_title: str) -> str:
    cleaned = re.sub(r'\d+\s*(?:minutes?|hours?|days?)\s*ago$', '', raw_title, flags=re.IGNORECASE)
    return cleaned.strip()


def format_chapter_count(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} главу"
    elif 2 <= n % 10 <= 4 and (n % 100 < 10 or n % 100 >= 20):
        return f"{n} главы"
    else:
        return f"{n} глав"


def split_text(text: str, max_len: int = 4096) -> list[str]:
    """Разбивает текст на части по max_len символов, стараясь не резать слова."""
    parts = []
    while len(text) > max_len:
        # Ищем последний пробел в пределах max_len
        split_pos = text.rfind(' ', 0, max_len)
        if split_pos == -1:
            split_pos = max_len
        parts.append(text[:split_pos].strip())
        text = text[split_pos:].strip()
    if text:
        parts.append(text)
    return parts


# ===== РАБОТА С ПОДПИСЧИКАМИ =====
def load_subscribers() -> set[int]:
    try:
        with open(SUBSCRIBERS_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("subscribers", []))
    except FileNotFoundError:
        return set()
    except Exception as e:
        logger.error(f"Ошибка загрузки подписчиков: {e}")
        return set()


def save_subscribers(subscribers: set[int]):
    try:
        with open(SUBSCRIBERS_FILE, "w") as f:
            json.dump({"subscribers": list(subscribers)}, f)
    except Exception as e:
        logger.error(f"Ошибка сохранения подписчиков: {e}")


# ===== РАБОТА С ПОСЛЕДНЕЙ ГЛАВОЙ =====
def get_last_chapter() -> str | None:
    try:
        with open(LAST_CHAPTER_FILE, "r") as f:
            content = f.read().strip()
            return content if content else None
    except FileNotFoundError:
        return None


def save_last_chapter(chapter_id: str):
    with open(LAST_CHAPTER_FILE, "w") as f:
        f.write(chapter_id)


# ===== ЗАГРУЗКА СТРАНИЦЫ СО СПИСКОМ ГЛАВ =====
async def fetch_html(url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        html = ""
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_selector('a:has-text("Chapter")', timeout=20000)
            html = await page.content()
        except PlaywrightTimeoutError:
            logger.warning("Таймаут ожидания элементов, но страница могла загрузиться.")
            html = await page.content()
        except Exception as e:
            logger.error(f"Ошибка Playwright: {e}")
            html = await page.content() if page else ""
        finally:
            await browser.close()

        if not html:
            raise Exception("Не удалось получить HTML")
        with open("debug.html", "w", encoding="utf-8") as f:
            f.write(html[:20000])
        return html


# ===== ЗАГРУЗКА ТЕКСТА КОНКРЕТНОЙ ГЛАВЫ =====
async def fetch_chapter_text(url: str) -> str:
    """Загружает текст главы из div.text#arrticle."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector('div.text#arrticle', timeout=30000)

            paragraphs = await page.evaluate('''() => {
                const container = document.querySelector('div.text#arrticle');
                if (!container) return [];
                const paras = Array.from(container.querySelectorAll('p'));
                return paras.map(p => p.innerText).filter(text => text.trim().length > 0);
            }''')

            if paragraphs:
                return '\n\n'.join(paragraphs)
            else:
                content = await page.text_content('div.text#arrticle')
                return content.strip() if content else "[Текст не найден]"
        except Exception as e:
            logger.warning(f"Ошибка загрузки {url}: {e}")
            return f"[Ошибка загрузки: {e}]"
        finally:
            await browser.close()


# ===== ПАРСИНГ СПИСКА ГЛАВ =====
def parse_chapters(html: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    chapters = []

    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if href.startswith('/shadow-slave-') and 'Chapter' in text:
            match = re.search(r'Chapter\s+(\d+)', text)
            if match:
                chapter_id = match.group(1)
                raw_title = text
                link = 'https://ranobes.net' + href
                chapters.append({'id': chapter_id, 'raw_title': raw_title, 'link': link})

    if not chapters:
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True)
            if ('/read-' in href or '/chapter-' in href) and text.startswith('Chapter'):
                match = re.search(r'Chapter\s+(\d+)', text)
                if match:
                    chapter_id = match.group(1)
                    raw_title = text
                    link = href if href.startswith('http') else 'https://ranobes.net' + href
                    chapters.append({'id': chapter_id, 'raw_title': raw_title, 'link': link})

    if not chapters:
        for a in soup.find_all('a', href=True):
            text = a.get_text(strip=True)
            if text.startswith('Chapter'):
                match = re.search(r'Chapter\s+(\d+)', text)
                if match:
                    chapter_id = match.group(1)
                    raw_title = text
                    href = a['href']
                    link = href if href.startswith('http') else 'https://ranobes.net' + href
                    chapters.append({'id': chapter_id, 'raw_title': raw_title, 'link': link})

    chapters.sort(key=lambda x: int(x['id']), reverse=True)
    logger.info(f"Найдено глав: {len(chapters)}")
    if chapters:
        logger.info(f"Пример ссылки на главу: {chapters[0]['link']}")
    return chapters


# ===== ПОЛУЧЕНИЕ ПОСЛЕДНИХ N ГЛАВ =====
async def get_latest_chapters(n: int) -> list[dict]:
    try:
        html = await fetch_html(TARGET_URL)
        all_chapters = parse_chapters(html)
        for ch in all_chapters:
            ch['title'] = clean_title(ch['raw_title'])
        return all_chapters[:n]
    except Exception as e:
        logger.exception(f"Ошибка в get_latest_chapters: {e}")
        return []


# ===== ПЕРЕВОД ЧЕРЕЗ OPENROUTER =====
async def translate_text(text: str, retries: int = 3) -> str:
    if len(text) > 120000:
        text = text[:120000] + "\n... [текст обрезан для перевода]"

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
                        translated = data["choices"][0]["message"]["content"]
                        return translated.strip()
                    elif resp.status == 429:
                        logger.warning(f"Rate limit, попытка {attempt+1}/{retries}")
                        if attempt == retries - 1:
                            return f"[Ошибка перевода: превышен лимит запросов. Оригинал:\n{text[:500]}...]"
                        await asyncio.sleep(2 ** attempt)
                    else:
                        error_text = await resp.text()
                        logger.error(f"Ошибка перевода {resp.status}: {error_text}")
                        return f"[Ошибка перевода. Оригинал:\n{text[:500]}...]"
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут перевода, попытка {attempt+1}/{retries}")
            if attempt == retries - 1:
                return f"[Ошибка: таймаут перевода. Оригинал:\n{text[:500]}...]"
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.exception(f"Исключение при переводе: {e}")
            return f"[Ошибка соединения. Оригинал:\n{text[:500]}...]"
    return "[Неизвестная ошибка перевода]"


# ===== РАССЫЛКА ВСЕМ ПОДПИСЧИКАМ =====
async def notify_all_subscribers(text: str, parse_mode: str = "HTML"):
    subscribers = load_subscribers()
    if not subscribers:
        logger.info("Нет подписчиков.")
        return

    removed = set()
    for uid in subscribers:
        try:
            await bot.send_message(chat_id=uid, text=text, parse_mode=parse_mode)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"Ошибка отправки {uid}: {e}, удаляем")
            removed.add(uid)
        except Exception as e:
            logger.error(f"Неизвестная ошибка {uid}: {e}")

    if removed:
        subscribers -= removed
        save_subscribers(subscribers)


# ===== КОМАНДЫ БОТА =====
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    subs = load_subscribers()
    if user_id in subs:
        await message.answer("Вы уже подписаны!")
    else:
        subs.add(user_id)
        save_subscribers(subs)
        await message.answer("✅ Вы подписались на уведомления о новых главах!")


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    user_id = message.from_user.id
    subs = load_subscribers()
    if user_id in subs:
        subs.remove(user_id)
        save_subscribers(subs)
        await message.answer("❌ Вы отписались.")
    else:
        await message.answer("Вы не были подписаны.")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    subs = load_subscribers()
    count = len(subs)
    last = get_last_chapter() or "пока нет"
    await message.answer(f"📊 Подписчиков: {count}\nПоследняя глава: {last}")


@dp.message(Command("last2"))
async def cmd_last2(message: types.Message):
    await message.answer("🔄 Загружаю 2 последние главы...")
    try:
        chapters = await get_latest_chapters(2)
        if not chapters:
            await message.answer("Не удалось найти главы.")
            return
        for ch in chapters:
            msg = f"📢 <b>{ch['title']}</b>\n🔗 {ch['link']}"
            await message.answer(msg, parse_mode="HTML")
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.exception(f"Ошибка /last2: {e}")
        await message.answer("❌ Ошибка.")


@dp.message(Command("last"))
async def cmd_last(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Укажите количество. Пример: /last 5")
        return
    try:
        n = int(parts[1])
        if n <= 0 or n > 50:
            await message.answer("Число от 1 до 50.")
            return
    except ValueError:
        await message.answer("Некорректное число.")
        return

    await message.answer(f"🔄 Загружаю {format_chapter_count(n)}...")
    try:
        chapters = await get_latest_chapters(n)
        if not chapters:
            await message.answer("Главы не найдены.")
            return
        for ch in chapters:
            msg = f"📢 <b>{ch['title']}</b>\n🔗 {ch['link']}"
            await message.answer(msg, parse_mode="HTML")
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.exception(f"Ошибка /last: {e}")
        await message.answer("❌ Ошибка.")


@dp.message(Command("tr2"))
async def cmd_translate_last2(message: types.Message):
    """Перевести 2 последние главы и отправить перевод в Telegram."""
    await message.answer("🔄 Загружаю и перевожу 2 последние главы...")
    try:
        chapters = await get_latest_chapters(2)
        if not chapters:
            await message.answer("Главы не найдены.")
            return

        for idx, ch in enumerate(chapters, 1):
            status = await message.answer(f"📥 Загружаю главу {idx}...")

            chapter_text = await fetch_chapter_text(ch['link'])
            await status.edit_text(f"🔄 Перевожу главу {idx}...")

            translated = await translate_text(chapter_text)

            await status.delete()

            # Отправляем перевод с заголовком
            header = f"📖 <b>{ch['title']} (перевод)</b>\n\n"
            full_message = header + translated
            parts = split_text(full_message)
            for part in parts:
                await message.answer(part, parse_mode="HTML")
                await asyncio.sleep(0.3)  # небольшая пауза между частями

    except Exception as e:
        logger.exception(f"Ошибка /tr2: {e}")
        await message.answer("❌ Произошла внутренняя ошибка.")


@dp.message(Command("tr"))
async def cmd_translate_last(message: types.Message):
    """Перевести N последних глав и отправить перевод в Telegram."""
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Укажите количество. Пример: /tr 5")
        return
    try:
        n = int(parts[1])
        if n <= 0 or n > 10:
            await message.answer("Количество от 1 до 10.")
            return
    except ValueError:
        await message.answer("Некорректное число.")
        return

    await message.answer(f"🔄 Загружаю и перевожу {format_chapter_count(n)}...")
    try:
        chapters = await get_latest_chapters(n)
        if not chapters:
            await message.answer("Главы не найдены.")
            return

        for idx, ch in enumerate(chapters, 1):
            status = await message.answer(f"📥 Глава {idx} из {n}: загрузка...")
            chapter_text = await fetch_chapter_text(ch['link'])
            await status.edit_text(f"🔄 Глава {idx} из {n}: перевод...")
            translated = await translate_text(chapter_text)
            await status.delete()

            header = f"📖 <b>{ch['title']} (перевод)</b>\n\n"
            full_message = header + translated
            parts = split_text(full_message)
            for part in parts:
                await message.answer(part, parse_mode="HTML")
                await asyncio.sleep(0.3)

    except Exception as e:
        logger.exception(f"Ошибка /tr: {e}")
        await message.answer("❌ Произошла внутренняя ошибка.")


# ===== МОНИТОРИНГ НОВЫХ ГЛАВ =====
async def monitor():
    while True:
        try:
            logger.info("Проверяем новые главы...")
            html = await fetch_html(TARGET_URL)
            chapters = parse_chapters(html)
            for ch in chapters:
                ch['title'] = clean_title(ch['raw_title'])

            last_id = get_last_chapter()
            last_int = int(last_id) if last_id else None

            new_chapters = []
            for ch in reversed(chapters):
                ch_int = int(ch['id'])
                if last_int is None or ch_int > last_int:
                    new_chapters.append(ch)
                else:
                    break

            if new_chapters:
                logger.info(f"Новых глав: {len(new_chapters)}")
                for ch in new_chapters:
                    msg = f"📢 <b>{ch['title']}</b>\n🔗 {ch['link']}"
                    await notify_all_subscribers(msg, parse_mode="HTML")
                    save_last_chapter(ch['id'])
                    await asyncio.sleep(0.5)
            else:
                logger.info("Новых глав нет.")
        except Exception as e:
            logger.exception(f"Ошибка в мониторинге: {e}")
        await asyncio.sleep(CHECK_INTERVAL)


# ===== ЗАПУСК =====
async def on_startup():
    logger.info("Бот запущен.")
    asyncio.create_task(monitor())


async def on_shutdown():
    logger.info("Бот остановлен.")


dp.startup.register(on_startup)
dp.shutdown.register(on_shutdown)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
