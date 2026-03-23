from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

import redis.asyncio as redis

from app.config import Settings
from app.models import ChapterMeta

logger = logging.getLogger(__name__)


class RedisRepository:
    def __init__(self, client: redis.Redis, settings: Settings):
        self.client = client
        self.settings = settings
        self._subscribers_key_ready = False

    @classmethod
    async def create(cls, settings: Settings) -> "RedisRepository":
        client = await redis.from_url(settings.redis_url, decode_responses=True)
        repo = cls(client, settings)
        await repo.ensure_subscribers_key()
        return repo

    async def close(self) -> None:
        await self.client.aclose()

    async def ensure_subscribers_key(self) -> None:
        if self._subscribers_key_ready:
            return
        key_type = await self.client.type("subscribers")
        if key_type in ("none", "set"):
            self._subscribers_key_ready = True
            return

        migrated_ids: set[int] = set()
        if key_type == "string":
            raw = await self.client.get("subscribers")
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        migrated_ids.update(int(item) for item in parsed)
                    elif isinstance(parsed, dict):
                        migrated_ids.update(int(item) for item in parsed.keys())
                except Exception:
                    for part in raw.split(","):
                        part = part.strip()
                        if part.isdigit():
                            migrated_ids.add(int(part))
        elif key_type == "list":
            for value in await self.client.lrange("subscribers", 0, -1):
                if str(value).isdigit():
                    migrated_ids.add(int(value))

        pipe = self.client.pipeline()
        pipe.delete("subscribers")
        if migrated_ids:
            pipe.sadd("subscribers", *[str(uid) for uid in migrated_ids])
        await pipe.execute()
        self._subscribers_key_ready = True
        logger.info("Migrated subscribers key to set with %s users", len(migrated_ids))

    async def load_glossary(self, glossary_path, force: bool = False) -> int:
        redis_key = "glossary:terms"
        existing_count = await self.client.hlen(redis_key)
        if existing_count > 0 and not force:
            return existing_count
        terms: dict[str, str] = {}
        with open(glossary_path, "r", encoding="utf-8") as file_obj:
            for line_num, line in enumerate(file_obj, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    logger.warning("glossary invalid line %s", line_num)
                    continue
                key, value = [item.strip() for item in line.split("=", 1)]
                if key and value:
                    terms[key] = value
        sorted_terms = dict(sorted(terms.items(), key=lambda item: len(item[0]), reverse=True))
        if sorted_terms:
            await self.client.delete(redis_key)
            await self.client.hset(redis_key, mapping=sorted_terms)
        return len(sorted_terms)

    async def get_glossary_terms(self) -> dict[str, str]:
        return await self.client.hgetall("glossary:terms")

    async def get_cached_telegraph(self, chapter_id: str) -> str | None:
        value = await self.client.hget("telegraph_urls", chapter_id)
        return value or None

    async def save_telegraph_url(self, chapter_id: str, url: str) -> None:
        await self.client.hset("telegraph_urls", chapter_id, url)

    async def get_cached_title(self, chapter_id: str) -> str | None:
        return await self.client.hget("chapter_titles", chapter_id)

    async def save_cached_title(self, chapter_id: str, title: str) -> None:
        await self.client.hset("chapter_titles", chapter_id, title)

    async def get_chapter_original_text(self, chapter_id: str) -> str | None:
        return await self.client.get(f"chapter:original:{chapter_id}")

    async def save_chapter_original_text(self, chapter_id: str, text: str) -> None:
        await self.client.set(f"chapter:original:{chapter_id}", text)

    async def get_chapter_translated_text(self, chapter_id: str) -> str | None:
        return await self.client.get(f"chapter:translated:{chapter_id}")

    async def save_chapter_translated_text(self, chapter_id: str, text: str) -> None:
        await self.client.set(f"chapter:translated:{chapter_id}", text)

    async def get_chapter_cache(self, chapter_id: str) -> dict[str, str]:
        return await self.client.hgetall(f"chapter:{chapter_id}")

    async def save_chapter_cache(self, chapter_id: str, mapping: dict[str, str]) -> None:
        payload = {k: str(v) for k, v in mapping.items() if v is not None}
        if payload:
            await self.client.hset(f"chapter:{chapter_id}", mapping=payload)

    async def set_chapter_status(self, chapter_id: str, status: str, error: str = "") -> None:
        await self.save_chapter_cache(chapter_id, {"status": status, "error": error, "updated_at": str(int(time.time()))})

    async def acquire_translation_lock(self, chapter_id: str) -> bool:
        return bool(await self.client.set(f"translation:lock:{chapter_id}", str(int(time.time())), ex=self.settings.translation_lock_ttl, nx=True))

    async def release_translation_lock(self, chapter_id: str) -> None:
        await self.client.delete(f"translation:lock:{chapter_id}")

    async def is_translation_in_progress(self, chapter_id: str) -> bool:
        return bool(await self.client.exists(f"translation:lock:{chapter_id}"))

    async def wait_for_ready_translation(self, chapter_id: str, timeout: int | None = None) -> str | None:
        timeout = timeout or self.settings.translation_wait_timeout
        deadline = time.time() + timeout
        while time.time() < deadline:
            url = await self.get_cached_telegraph(chapter_id)
            if url:
                return url
            cache = await self.get_chapter_cache(chapter_id)
            if cache.get("telegraph_url"):
                return cache["telegraph_url"]
            if cache.get("status") == "failed":
                return None
            await __import__("asyncio").sleep(self.settings.translation_wait_step)
        cache = await self.get_chapter_cache(chapter_id)
        return cache.get("telegraph_url")

    async def save_translation_error(self, chapter_id: str, error: str) -> None:
        await self.client.set(f"translation:error:{chapter_id}", error, ex=self.settings.error_ttl)

    async def get_translation_error(self, chapter_id: str) -> str | None:
        return await self.client.get(f"translation:error:{chapter_id}")

    async def save_chapter_meta(self, chapter: ChapterMeta) -> None:
        await self.client.hset(f"chapter_meta:{chapter.id}", mapping=chapter.to_mapping())

    async def get_chapter_meta(self, chapter_id: str) -> ChapterMeta | None:
        data = await self.client.hgetall(f"chapter_meta:{chapter_id}")
        if not data or not data.get("id") or not data.get("title") or not data.get("link"):
            return None
        return ChapterMeta.from_mapping(data)

    async def get_first_chapter(self) -> int | None:
        cached = await self.client.get("first_chapter")
        return int(cached) if cached and str(cached).isdigit() else None

    async def save_first_chapter(self, chapter_id: int) -> None:
        await self.client.set("first_chapter", str(chapter_id), ex=3600)

    async def get_last_chapter(self) -> str | None:
        return await self.client.get("last_chapter")

    async def save_last_chapter(self, chapter_id: str) -> None:
        await self.client.set("last_chapter", chapter_id)

    async def load_subscribers(self) -> set[int]:
        await self.ensure_subscribers_key()
        return {int(v) for v in await self.client.smembers("subscribers")}

    async def add_subscriber(self, user_id: int) -> None:
        await self.ensure_subscribers_key()
        await self.client.sadd("subscribers", str(user_id))

    async def remove_subscriber(self, user_id: int) -> bool:
        await self.ensure_subscribers_key()
        return bool(await self.client.srem("subscribers", str(user_id)))

    async def is_user_subscribed(self, user_id: int) -> bool:
        await self.ensure_subscribers_key()
        return bool(await self.client.sismember("subscribers", str(user_id)))

    async def is_user_blocked(self, user_id: int) -> bool:
        return bool(await self.client.sismember("blocked_users", str(user_id)))

    async def block_user(self, user_id: int) -> None:
        await self.client.sadd("blocked_users", str(user_id))
        await self.remove_subscriber(user_id)

    async def unblock_user(self, user_id: int) -> None:
        await self.client.srem("blocked_users", str(user_id))

    async def get_user_bookmark(self, user_id: int) -> str | None:
        return await self.client.hget("user_bookmarks", str(user_id))

    async def save_user_bookmark(self, user_id: int, chapter_id: str) -> None:
        await self.client.hset("user_bookmarks", str(user_id), chapter_id)

    async def add_chapter_waiter(self, identity: str, user_id: int) -> None:
        key = f"chapter:waiters:{identity}"
        await self.client.sadd(key, str(user_id))
        await self.client.expire(key, self.settings.job_dedupe_ttl * 4)

    async def pop_chapter_waiters(self, identity: str) -> set[int]:
        key = f"chapter:waiters:{identity}"
        values = {int(v) for v in await self.client.smembers(key)}
        await self.client.delete(key)
        return values

    def build_translation_signature(self, original_text: str, system_prompt: str, user_prompt: str) -> str:
        payload = json.dumps(
            {
                "cache_version": self.settings.translation_cache_version,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "text_sha256": hashlib.sha256(original_text.encode("utf-8")).hexdigest(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def invalidate_outdated_chapter_cache(self, chapter_id: str, cache: dict[str, str], translation_signature: str) -> dict[str, str]:
        cached_signature = cache.get("translation_signature")
        if not cached_signature or cached_signature == translation_signature:
            return cache
        await self.client.delete(f"chapter:translated:{chapter_id}")
        await self.client.hdel("telegraph_urls", chapter_id)
        await self.save_chapter_cache(
            chapter_id,
            {
                "translation_signature": translation_signature,
                "telegraph_url": "",
                "translated_title": "",
                "status": "stale",
                "error": "",
            },
        )
        refreshed = await self.get_chapter_cache(chapter_id)
        refreshed["translation_signature"] = translation_signature
        return refreshed
