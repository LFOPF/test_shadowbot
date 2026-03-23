from __future__ import annotations

import json
import logging
import time
from typing import Any

from app.repositories.redis_repo import RedisRepository

logger = logging.getLogger(__name__)


class JobQueue:
    def __init__(self, repo: RedisRepository):
        self.repo = repo
        self.redis = repo.client
        self.settings = repo.settings

    async def enqueue_json_job(self, queue_name: str, payload: dict[str, Any], dedupe_key: str | None = None, dedupe_ttl: int | None = None) -> bool:
        dedupe_ttl = dedupe_ttl or self.settings.job_dedupe_ttl
        if dedupe_key:
            acquired = await self.redis.set(dedupe_key, "1", ex=dedupe_ttl, nx=True)
            if not acquired:
                return False
        await self.redis.lpush(queue_name, json.dumps(payload, ensure_ascii=False))
        return True

    async def dequeue_json_job(self, queue_name: str, timeout: int = 5) -> dict[str, Any] | None:
        item = await self.redis.brpop(queue_name, timeout=timeout)
        if not item:
            return None
        _, raw_payload = item
        return json.loads(raw_payload)

    async def enqueue_chapter_job(
        self,
        *,
        chapter_number: int | None = None,
        chapter_id: str | None = None,
        requested_by: int | None = None,
        source: str = "user",
        notify_user: bool = False,
        broadcast: bool = False,
    ) -> bool:
        identity = chapter_id if chapter_id is not None else (str(chapter_number) if chapter_number is not None else None)
        if identity is None:
            return False
        if requested_by and notify_user:
            await self.repo.add_chapter_waiter(identity, requested_by)
        payload = {
            "chapter_number": chapter_number,
            "chapter_id": chapter_id,
            "requested_by": requested_by,
            "source": source,
            "notify_user": notify_user,
            "broadcast": broadcast,
            "enqueued_at": int(time.time()),
        }
        return await self.enqueue_json_job(
            self.settings.chapter_job_queue,
            payload,
            dedupe_key=f"queue:chapter_job:dedupe:{identity}",
        )

    async def enqueue_notification_job(self, payload: dict[str, Any]) -> bool:
        dedupe_key = None
        if payload.get("type") == "broadcast_new_chapter":
            dedupe_key = f"queue:notification:broadcast:{payload.get('chapter_id')}"
        return await self.enqueue_json_job(self.settings.notification_job_queue, payload, dedupe_key=dedupe_key)

    async def enqueue_monitor_trigger(self, reason: str = "manual") -> bool:
        return await self.enqueue_json_job(
            self.settings.monitor_trigger_queue,
            {"reason": reason, "created_at": int(time.time())},
            dedupe_key=f"queue:monitor_trigger:{reason}",
            dedupe_ttl=30,
        )
