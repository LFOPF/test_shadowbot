from __future__ import annotations

import logging
import re
import time

import aiohttp
from tenacity import after_log, before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import Settings
from app.repositories.redis_repo import RedisRepository
from app.services.http import HttpSessionManager

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ты — профессиональный литературный переводчик веб-новелл, специализирующийся на Shadow Slave. "
    "Ты переводишь на уровне лучших русскоязычных команд. "
    "Верни только чистый литературный перевод главы без пояснений и мусора."
)

USER_PROMPT_TEMPLATE = (
    "Переведи следующий текст главы на русский язык максимально качественно и литературно. "
    "Сделай перевод живым, атмосферным и естественным.\n\nТекст для перевода:\n\n{text}"
)

RETRY_API = dict(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=4, max=90),
    retry=retry_if_exception_type((aiohttp.ClientError, ConnectionError, TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.INFO),
)


class TranslationService:
    def __init__(self, settings: Settings, repo: RedisRepository, http: HttpSessionManager):
        self.settings = settings
        self.repo = repo
        self.http = http
        self._glossary_cache: dict[str, str] | None = None
        self._glossary_cache_expires_at = 0.0

    async def preload_glossary(self, force: bool = False) -> int:
        return await self.repo.load_glossary(self.settings.glossary_path, force=force)

    async def get_glossary_terms(self, force_refresh: bool = False) -> dict[str, str]:
        now = time.time()
        if not force_refresh and self._glossary_cache is not None and now < self._glossary_cache_expires_at:
            return self._glossary_cache
        self._glossary_cache = await self.repo.get_glossary_terms()
        self._glossary_cache_expires_at = now + self.settings.glossary_cache_ttl
        return self._glossary_cache

    async def get_relevant_glossary(self, text: str) -> str:
        all_terms = await self.get_glossary_terms()
        relevant = []
        lower = text.lower()
        for eng, rus in all_terms.items():
            pattern = re.compile(r"(?i)\b" + re.escape(eng) + r"\b")
            if pattern.search(text) or eng.lower() in lower:
                relevant.append(f"{eng} → {rus}")
        if not relevant:
            return ""
        return "=== ГЛОССАРИЙ ===\n" + "\n".join(relevant)

    @retry(**RETRY_API)
    async def translate_text(self, text: str) -> str:
        if len(text) > 120000:
            text = text[:120000] + "\n... [обрезано]"
        glossary_section = await self.get_relevant_glossary(text)
        system_prompt = f"{glossary_section}\n\n{SYSTEM_PROMPT}" if glossary_section else SYSTEM_PROMPT
        payload = {
            "model": self.settings.openrouter_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(text=text)},
            ],
            "temperature": 0.9,
            "top_p": 0.92,
            "max_tokens": 8192,
            "presence_penalty": 0.15,
            "frequency_penalty": 0.08,
        }
        session = await self.http.get()
        async with session.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.settings.openrouter_api_key}",
                "HTTP-Referer": self.settings.site_url,
                "X-Title": self.settings.site_name,
                "Content-Type": "application/json",
            },
            json=payload,
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data["choices"][0]["message"]["content"].strip()
            if response.status == 429:
                raise aiohttp.ClientError("Rate limit")
            raise aiohttp.ClientError(f"OpenRouter HTTP {response.status}: {(await response.text())[:500]}")

    @retry(**RETRY_API)
    async def translate_title(self, title: str) -> str:
        if not title.strip():
            return "Без названия"
        if re.search(r"[А-Яа-яЁё]", title):
            return title.strip()
        session = await self.http.get()
        payload = {
            "model": self.settings.openrouter_model,
            "messages": [
                {"role": "system", "content": "Переведи ТОЛЬКО название главы новеллы на русский язык и верни только итоговое название."},
                {"role": "user", "content": title.strip()},
            ],
            "temperature": 0.2,
            "top_p": 0.9,
            "max_tokens": 120,
        }
        async with session.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.settings.openrouter_api_key}",
                "HTTP-Referer": self.settings.site_url,
                "X-Title": self.settings.site_name,
                "Content-Type": "application/json",
            },
            json=payload,
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data["choices"][0]["message"]["content"].strip()
            if response.status == 429:
                raise aiohttp.ClientError("Rate limit")
            logger.warning("Title translation fallback for %s", title)
            return title.strip()
