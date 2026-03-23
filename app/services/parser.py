from __future__ import annotations

import asyncio
import logging
import re
from typing import Awaitable, Callable

from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, Error as PlaywrightError, Page, Playwright, async_playwright
from tenacity import after_log, before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import Settings
from app.models import ChapterMeta
from app.repositories.redis_repo import RedisRepository

logger = logging.getLogger(__name__)

RETRY_WEB = dict(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.2, min=3, max=75),
    retry=retry_if_exception_type((asyncio.TimeoutError, ConnectionError, OSError, PlaywrightError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.INFO),
)


class PlaywrightManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._last_activity = 0.0
        self._keepalive_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._closing = False
        self._semaphore = asyncio.Semaphore(settings.playwright_concurrency)

    async def launch(self) -> None:
        async with self._lock:
            if self._browser is not None:
                self.touch()
                return
            logger.info("Starting Playwright browser")
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--single-process",
                    "--disable-extensions",
                ],
            )
            self._context = await self._browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1280, "height": 720},
                ignore_https_errors=True,
            )
            self.touch()

    def touch(self) -> None:
        self._last_activity = asyncio.get_running_loop().time()
        if self._keepalive_task is None or self._keepalive_task.done():
            self._keepalive_task = asyncio.create_task(self._keep_alive())

    async def _keep_alive(self) -> None:
        try:
            while True:
                idle = asyncio.get_running_loop().time() - self._last_activity
                remaining = self.settings.browser_idle_timeout - idle
                if remaining <= 0:
                    logger.info("Closing idle browser after %.0f seconds", idle)
                    await self.close()
                    return
                await asyncio.sleep(min(remaining, 60.0))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Playwright keepalive loop failed")

    def _should_block(self, route) -> bool:
        req = route.request
        if req.resource_type in {"image", "stylesheet", "font", "media", "other"}:
            return True
        url = req.url.lower()
        return any(pattern in url for pattern in ["googleads", "doubleclick", "adservice", "yandex.ru/ads", "mgid"])

    async def create_page(self) -> Page:
        if self._browser is None or self._context is None:
            await self.launch()
        assert self._context is not None
        self.touch()
        page = await self._context.new_page()
        await page.set_viewport_size({"width": 1280, "height": 720})
        await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9,ru;q=0.8"})
        await page.route("**/*", lambda route: route.abort() if self._should_block(route) else route.continue_())
        return page

    async def with_page(self, callback: Callable[[Page], Awaitable[str]]) -> str:
        async with self._semaphore:
            page = await self.create_page()
            try:
                return await callback(page)
            finally:
                if not page.is_closed():
                    await page.close()

    async def close(self) -> None:
        async with self._lock:
            if self._browser is None or self._closing:
                return
            self._closing = True
            try:
                if self._keepalive_task:
                    self._keepalive_task.cancel()
                    await asyncio.gather(self._keepalive_task, return_exceptions=True)
                    self._keepalive_task = None
                if self._context:
                    await self._context.close()
                if self._browser:
                    await self._browser.close()
                if self._playwright:
                    await self._playwright.stop()
            finally:
                self._playwright = None
                self._browser = None
                self._context = None
                self._closing = False


class ChapterParser:
    def __init__(self, settings: Settings, repo: RedisRepository, browser: PlaywrightManager):
        self.settings = settings
        self.repo = repo
        self.browser = browser

    @staticmethod
    def extract_chapter_id(text: str) -> str | None:
        text = text.strip()
        for pattern in [r"(?:Chapter|Ch\.?|Глава)\s*[:.\-–—]?\s*(\d+)", r"(\d{4,})"]:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def clean_title(raw_title: str) -> str:
        return re.sub(r"\s*\d+\s*(?:minute|hour|day|week|month|year)s?\s+ago$", "", raw_title, flags=re.IGNORECASE).strip()

    def get_page_url(self, page_num: int) -> str:
        if page_num == 1:
            return self.settings.target_url
        return f"{self.settings.target_url.rstrip('/')}/page/{page_num}/"

    @retry(**RETRY_WEB)
    async def fetch_html(self, url: str) -> str:
        async def _load(page: Page) -> str:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            try:
                await page.wait_for_selector('a:has-text("Chapter")', timeout=40000)
            except PlaywrightError:
                logger.warning("Chapter selector not found on %s", url)
            await page.wait_for_timeout(1500)
            return await page.content()

        return await self.browser.with_page(_load)

    @retry(**RETRY_WEB)
    async def fetch_chapter_text(self, url: str) -> str:
        async def _load(page: Page) -> str:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_selector("div.text#arrticle", timeout=60000)
            paragraphs = await page.evaluate(
                """() => {
                    const c = document.querySelector('div.text#arrticle');
                    if (!c) return [];
                    return Array.from(c.querySelectorAll('p')).map(p => p.innerText.trim()).filter(Boolean);
                }"""
            )
            if paragraphs:
                return "\n\n".join(paragraphs)
            content = await page.text_content("div.text#arrticle")
            return content.strip() if content else "[Текст не найден]"

        return await self.browser.with_page(_load)

    def parse_chapters(self, html: str) -> list[ChapterMeta]:
        soup = BeautifulSoup(html, "html.parser")
        chapters: list[ChapterMeta] = []
        for anchor in soup.find_all("a", href=True):
            text = anchor.get_text(strip=True)
            chapter_id = self.extract_chapter_id(text)
            if not chapter_id or not chapter_id.isdigit():
                continue
            href = anchor["href"]
            link = f"https://ranobes.net{href}" if not href.startswith("http") else href
            if self.settings.novel_id not in link:
                continue
            chapters.append(ChapterMeta(id=chapter_id, raw_title=text, title=self.clean_title(text), link=link))
        chapters.sort(key=lambda item: int(item.id), reverse=True)
        return chapters

    async def get_first_chapter(self) -> int | None:
        cached = await self.repo.get_first_chapter()
        if cached:
            return cached
        html = await self.fetch_html(self.settings.target_url)
        chapters = self.parse_chapters(html)
        if not chapters:
            return None
        first = int(chapters[0].id)
        await self.repo.save_first_chapter(first)
        return first

    async def find_chapter_by_number(self, chapter_number: int) -> ChapterMeta | None:
        first_chapter = await self.get_first_chapter()
        if first_chapter:
            estimate = max(1, min(self.settings.max_pages, 1 + (first_chapter - chapter_number) // self.settings.chapters_per_page))
            chapters = self.parse_chapters(await self.fetch_html(self.get_page_url(estimate)))
            for chapter in chapters:
                if int(chapter.id) == chapter_number:
                    return chapter
        return await self.find_chapter_by_number_binary(chapter_number)

    async def find_chapter_by_number_binary(self, chapter_number: int) -> ChapterMeta | None:
        left, right = 1, self.settings.max_pages
        while left <= right:
            mid = (left + right) // 2
            chapters = self.parse_chapters(await self.fetch_html(self.get_page_url(mid)))
            if not chapters:
                right = mid - 1
                continue
            first_id, last_id = int(chapters[0].id), int(chapters[-1].id)
            if chapter_number > first_id:
                right = mid - 1
            elif chapter_number < last_id:
                left = mid + 1
            else:
                for chapter in chapters:
                    if int(chapter.id) == chapter_number:
                        return chapter
                return None
            await asyncio.sleep(0.5)
        return None
