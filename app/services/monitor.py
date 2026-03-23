from __future__ import annotations

import asyncio
import logging

from app.queues.jobs import JobQueue
from app.repositories.redis_repo import RedisRepository
from app.services.parser import ChapterParser

logger = logging.getLogger(__name__)


class MonitorService:
    def __init__(self, repo: RedisRepository, queue: JobQueue, parser: ChapterParser):
        self.repo = repo
        self.queue = queue
        self.parser = parser
        self._lock = asyncio.Lock()

    async def monitor_once(self) -> None:
        async with self._lock:
            html = await self.parser.fetch_html(self.parser.settings.target_url)
            chapters = self.parser.parse_chapters(html)
            for chapter in chapters:
                await self.repo.save_chapter_meta(chapter)
            last_str = await self.repo.get_last_chapter()
            last_int = int(last_str) if last_str and str(last_str).isdigit() else 0
            new_chapters = [chapter for chapter in reversed(chapters) if int(chapter.id) > last_int]
            if not new_chapters:
                logger.info("No new chapters found")
                return
            logger.info("Found %s new chapters", len(new_chapters))
            for chapter in new_chapters:
                await self.queue.enqueue_chapter_job(
                    chapter_id=chapter.id,
                    chapter_number=int(chapter.id),
                    source="monitor",
                    notify_user=False,
                    broadcast=True,
                )

    async def run(self) -> None:
        logger.info("Scheduler monitor loop started")
        while True:
            trigger = await self.queue.dequeue_json_job(self.repo.settings.monitor_trigger_queue, timeout=self.repo.settings.check_interval)
            if trigger:
                logger.info("Received monitor trigger: %s", trigger)
            try:
                await self.monitor_once()
            except Exception:
                logger.exception("Monitor iteration failed")
