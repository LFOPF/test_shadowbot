import os
import asyncio
import logging
import re
import json
import aiohttp
from aiohttp import FormData
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

# ===== ENV =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
TELEGRAPH_TOKEN = os.getenv("TELEGRAPH_TOKEN")  # можно не указывать

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN missing")
if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY missing")

TARGET_URL = "https://ranobes.net/chapters/1205249/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ===== UTILS =====
def clean_title(title: str) -> str:
    return re.sub(r'\d+\s*(minutes?|hours?|days?)\s*ago$', '', title).strip()

def text_to_html(text: str) -> str:
    parts = text.split("\n\n")
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in parts if p.strip())

# ===== PARSER =====
async def fetch_html(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.text()

def parse_chapters(html: str):
    soup = BeautifulSoup(html, "html.parser")
    chapters = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if "Chapter" in text:
            match = re.search(r"Chapter\s+(\d+)", text)
            if match:
                chapters.append({
                    "id": int(match.group(1)),
                    "title": clean_title(text),
                    "link": "https://ranobes.net" + a["href"]
                })

    return sorted(chapters, key=lambda x: x["id"], reverse=True)

# ===== CHAPTER TEXT =====
async def fetch_chapter_text(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            html = await resp.text()

    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#arrticle")

    if not container:
        return "Текст не найден"

    return "\n\n".join(p.get_text() for p in container.find_all("p"))

# ===== TRANSLATE =====
async def translate_text(text: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "stepfun/step-3.5-flash:free",
        "messages": [
            {"role": "user", "content": f"Переведи на русский:\n\n{text}"}
        ]
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload
        ) as resp:

            data = await resp.json()
            return data["choices"][0]["message"]["content"]

# ===== TELEGRAPH =====
async def create_telegraph_page(title: str, html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    nodes = []
    for p in soup.find_all("p"):
        text = p.get_text().strip()
        if text:
            nodes.append({"tag": "p", "children": [text]})

    content = json.dumps(nodes, ensure_ascii=False)

    form = FormData()
    form.add_field("title", title[:256])
    form.add_field("content", content)

    if TELEGRAPH_TOKEN:
        form.add_field("access_token", TELEGRAPH_TOKEN)

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.telegra.ph/createPage",
            data=form
        ) as resp:

            data = await resp.json()

            if not data.get("ok"):
                raise Exception(f"Telegraph error: {data}")

            return data["result"]["url"]

# ===== COMMAND =====
@dp.message(Command("tr"))
async def translate_last(message: types.Message):
    await message.answer("Загружаю...")

    html = await fetch_html(TARGET_URL)
    chapters = parse_chapters(html)[:1]

    if not chapters:
        await message.answer("Нет глав")
        return

    ch = chapters[0]

    text = await fetch_chapter_text(ch["link"])
    translated = await translate_text(text)
    html_content = text_to_html(translated)

    url = await create_telegraph_page(ch["title"], html_content)

    await message.answer(
        f"<b>{ch['title']}</b>\n\n<a href='{url}'>Читать</a>",
        parse_mode="HTML"
    )

# ===== START =====
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
