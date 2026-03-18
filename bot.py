import os
import asyncio
import logging
import re
import json
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ===== НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN должен быть задан в переменных окружения!")

TARGET_URL = "https://ranobes.net/chapters/1205249/"
CHECK_INTERVAL = 60 * 60  # 2 часа (можно изменить на 60 для теста)
LAST_CHAPTER_FILE = "last_chapter.txt"
SUBSCRIBERS_FILE = "subscribers.json"
# =============================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
def clean_title(raw_title: str) -> str:
    """
    Например: "Chapter 2885: Dying Flames2 hours ago" -> "Chapter 2885: Dying Flames"
    """
    
    cleaned = re.sub(r'\d+\s*(?:minutes?|hours?|days?)\s*ago$', '', raw_title, flags=re.IGNORECASE)
    return cleaned.strip()


def format_chapter_count(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} главу"
    elif 2 <= n % 10 <= 4 and (n % 100 < 10 or n % 100 >= 20):
        return f"{n} главы"
    else:
        return f"{n} глав"


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


# ===== ЗАГРУЗКА СТРАНИЦЫ ЧЕРЕЗ PLAYWRIGHT =====
async def fetch_html(url: str) -> str:
    """Загружает страницу, возвращает HTML даже при частичной загрузке."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        html = ""
        try:
            await page.goto(url, timeout=60000)
            # Ждём появления хотя бы одной ссылки с текстом "Chapter" (до 20 сек)
            await page.wait_for_selector('a:has-text("Chapter")', timeout=20000)
            html = await page.content()
        except PlaywrightTimeoutError:
            logger.warning("Таймаут ожидания элементов, но страница могла загрузиться.")
            html = await page.content()
        except Exception as e:
            logger.error(f"Ошибка Playwright при загрузке {url}: {e}")
            html = await page.content() if page else ""
        finally:
            await browser.close()

        if not html:
            raise Exception("Не удалось получить HTML страницы")

        # Сохраняем для отладки (первые 20000 символов)
        with open("debug.html", "w", encoding="utf-8") as f:
            f.write(html[:20000])
        return html


# ===== ПАРСИНГ ГЛАВ =====
def parse_chapters(html: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    chapters = []

    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if ('/read-' in href or '/chapter-' in href) and text.startswith('Chapter'):
            match = re.search(r'Chapter\s+(\d+)', text)
            if match:
                chapter_id = match.group(1)
                # Сохраняем полный текст для последующей очистки
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

    chapters.sort(key=lambda x: int(x['id']), reverse=True)  # новые первыми
    logger.info(f"Найдено глав: {len(chapters)}")
    return chapters


# ===== ПОЛУЧЕНИЕ ПОСЛЕДНИХ N ГЛАВ =====
async def get_latest_chapters(n: int) -> list[dict]:
    """Возвращает список последних n глав (самые свежие)."""
    try:
        html = await fetch_html(TARGET_URL)
        all_chapters = parse_chapters(html)
        # Очищаем заголовки от дат
        for ch in all_chapters:
            ch['title'] = clean_title(ch['raw_title'])
        return all_chapters[:n]
    except Exception as e:
        logger.exception(f"Ошибка в get_latest_chapters: {e}")
        return []


# ===== РАССЫЛКА ВСЕМ ПОДПИСЧИКАМ =====
async def notify_all_subscribers(text: str, parse_mode: str = "HTML"):
    subscribers = load_subscribers()
    if not subscribers:
        logger.info("Нет подписчиков для рассылки.")
        return

    removed = set()
    for uid in subscribers:
        try:
            await bot.send_message(chat_id=uid, text=text, parse_mode=parse_mode)
            logger.debug(f"Сообщение отправлено {uid}")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {uid} заблокировал бота. Удаляем.")
            removed.add(uid)
        except TelegramBadRequest as e:
            logger.error(f"Ошибка отправки {uid}: {e.message}")
            if "chat not found" in e.message:
                removed.add(uid)
        except Exception as e:
            logger.error(f"Неизвестная ошибка при отправке {uid}: {e}")

    if removed:
        subscribers -= removed
        save_subscribers(subscribers)
        logger.info(f"Удалено подписчиков: {len(removed)}")


# ===== КОМАНДЫ БОТА =====
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    subscribers = load_subscribers()
    if user_id in subscribers:
        await message.answer("Вы уже подписаны на уведомления о новых главах!")
    else:
        subscribers.add(user_id)
        save_subscribers(subscribers)
        await message.answer("Вы подписались на уведомления о новых главах Shadow Slave! ХАХАХАХАХАХ ЕБАТЬ ТЫ ЧМИЩЕ, РАБ ГОВНИЩЕ!!!! ФУУУУУУУ ПРОСТО ГОВНИЩЕНСКОЕ ГОВНО")
        logger.info(f"Новый подписчик: {user_id}")


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    user_id = message.from_user.id
    subscribers = load_subscribers()
    if user_id in subscribers:
        subscribers.remove(user_id)
        save_subscribers(subscribers)
        await message.answer("Вы отписались от уведомлений.")
        logger.info(f"Подписчик отписался: {user_id}")
    else:
        await message.answer("Вы не были подписаны.")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    subscribers = load_subscribers()
    count = len(subscribers)
    last = get_last_chapter() or "пока нет"
    await message.answer(f"📊 Статистика:\nПодписчиков: {count}\nПоследняя отправленная глава: {last}")


@dp.message(Command("last"))
async def cmd_last(message: types.Message):
    """Отправить N последних глав только этому пользователю. Использование: /last N"""
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Пожалуйста, укажите количество глав. Пример: /last 5")
        return
    try:
        n = int(parts[1])
        if n <= 0 or n > 50:
            await message.answer("Количество глав должно быть от 1 до 50.")
            return
    except ValueError:
        await message.answer("Некорректное число. Пример: /last 5")
        return

    await message.answer(f"Загружаю {format_chapter_count(n)}...")
    try:
        chapters = await get_latest_chapters(n)
        if not chapters:
            await message.answer("Не удалось найти главы. Возможно, сайт временно недоступен или изменилась структура.")
            return

        for ch in chapters:
            msg = f"📢 <b>{ch['title']}</b>\n🔗 {ch['link']}"
            await message.answer(msg, parse_mode="HTML")
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.exception(f"Ошибка в команде /last: {e}")
        await message.answer("Произошла внутренняя ошибка. Боня в курсе")


# ===== МОНИТОРИНГ =====
async def monitor():
    while True:
        try:
            logger.info("Проверяем новые главы...")
            html = await fetch_html(TARGET_URL)
            chapters = parse_chapters(html)

            # Очищаем заголовки от дат
            for ch in chapters:
                ch['title'] = clean_title(ch['raw_title'])

            last_id = get_last_chapter()
            logger.info(f"Последняя сохранённая глава: {last_id}")

            last_int = None
            if last_id is not None:
                try:
                    last_int = int(last_id)
                except ValueError:
                    logger.warning(f"Неверный формат last_id '{last_id}'. Сбрасываем.")
                    last_int = None

            new_chapters = []
            for ch in reversed(chapters):  # от старых к новым
                ch_int = int(ch['id'])
                if last_int is None or ch_int > last_int:
                    new_chapters.append(ch)
                else:
                    break

            if new_chapters:
                logger.info(f"Найдено новых глав: {len(new_chapters)}")
                for ch in new_chapters:
                    msg = f"📢 <b>{ch['title']}</b>\n🔗 {ch['link']}"
                    await notify_all_subscribers(msg, parse_mode="HTML")
                    save_last_chapter(ch['id'])
                    last_int = int(ch['id'])
                    await asyncio.sleep(0.5)
            else:
                logger.info("Новых глав нет.")

        except Exception as e:
            logger.exception(f"Ошибка в мониторинге: {e}")

        logger.info(f"Ожидание {CHECK_INTERVAL} секунд...")
        await asyncio.sleep(CHECK_INTERVAL)


# ===== ЗАПУСК =====
async def on_startup():
    logger.info("Бот запущен. Старт мониторинга...")
    asyncio.create_task(monitor())


async def on_shutdown():
    logger.info("Бот остановлен.")


dp.startup.register(on_startup)
dp.shutdown.register(on_shutdown)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
