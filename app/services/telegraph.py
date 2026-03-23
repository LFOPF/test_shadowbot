from __future__ import annotations

import html
import logging
import re

import aiohttp
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential, wait_fixed

from app.config import Settings
from app.services.http import HttpSessionManager

logger = logging.getLogger(__name__)


class TelegraphService:
    def __init__(self, settings: Settings, http: HttpSessionManager):
        self.settings = settings
        self.http = http

    def clean_title(self, title: str) -> str:
        title = re.sub(r"\s*\([^)]*\)$", "", title).strip()
        if len(title) > self.settings.telegraph_title_max_length:
            title = title[: self.settings.telegraph_title_max_length - 3] + "..."
        return title

    def text_to_html(self, text: str) -> str:
        paragraphs = text.split("\n\n")
        return "".join("<p>" + html.escape(paragraph).replace("\n", "<br>") + "</p>" for paragraph in paragraphs if paragraph.strip())

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5) + wait_exponential(min=2, max=60), retry=retry_if_exception_type((aiohttp.ClientError, ConnectionError, Exception)))
    async def create_page(self, title: str, content_html: str, author: str = "Shadow Slave Bot") -> str | None:
        clean_title = self.clean_title(title)
        soup = BeautifulSoup(content_html, "html.parser")
        nodes = []
        for elem in soup.children:
            if getattr(elem, "name", None) == "p":
                children = []
                for sub in elem.children:
                    if getattr(sub, "name", None) == "a":
                        children.append({"tag": "a", "attrs": {"href": sub.get("href")}, "children": [sub.get_text()]})
                    else:
                        text = sub.string if getattr(sub, "string", None) else ""
                        if text:
                            children.append(text)
                nodes.append({"tag": "p", "children": children})
        if not nodes:
            nodes = [{"tag": "p", "children": [content_html]}]
        payload = {"access_token": self.settings.telegraph_access_token, "title": clean_title, "author_name": author, "content": nodes}
        session = await self.http.get()
        async with session.post("https://api.telegra.ph/createPage", json=payload, timeout=aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)) as response:
            data = await response.json()
            if data.get("ok"):
                return data["result"]["url"]
            logger.error("Telegraph error: %s", data)
            return None
