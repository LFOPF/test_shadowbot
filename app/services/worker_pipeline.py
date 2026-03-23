from __future__ import annotations

import logging

from app.models import ChapterMeta
from app.queues.jobs import JobQueue
from app.repositories.redis_repo import RedisRepository
from app.services.chapter_flow import contains_cyrillic
from app.services.parser import ChapterParser
from app.services.telegraph import TelegraphService
from app.services.translation import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE, TranslationService

logger = logging.getLogger(__name__)


class WorkerService:
    def __init__(self, repo: RedisRepository, queue: JobQueue, parser: ChapterParser, translator: TranslationService, telegraph: TelegraphService):
        self.repo = repo
        self.queue = queue
        self.parser = parser
        self.translator = translator
        self.telegraph = telegraph

    async def process_chapter_translation(self, chapter: ChapterMeta) -> tuple[str | None, bool]:
        cid = chapter.id
        await self.repo.save_chapter_meta(chapter)
        if await self.repo.is_translation_in_progress(cid):
            ready = await self.repo.wait_for_ready_translation(cid)
            return ready, bool(ready)
        if not await self.repo.acquire_translation_lock(cid):
            ready = await self.repo.wait_for_ready_translation(cid)
            return ready, bool(ready)
        try:
            await self.repo.set_chapter_status(cid, "processing")
            cache = await self.repo.get_chapter_cache(cid)
            await self.repo.save_chapter_cache(cid, {"source_url": chapter.link, "source_title": chapter.title})

            original = await self.repo.get_chapter_original_text(cid)
            if not original:
                try:
                    original = await self.parser.fetch_chapter_text(chapter.link)
                except Exception:
                    await self.repo.set_chapter_status(cid, "failed", "fetch_failed")
                    await self.repo.save_translation_error(cid, "fetch_failed")
                    return None, False
                await self.repo.save_chapter_original_text(cid, original)

            signature = self.repo.build_translation_signature(original, SYSTEM_PROMPT, USER_PROMPT_TEMPLATE)
            cache = await self.repo.invalidate_outdated_chapter_cache(cid, cache, signature)
            await self.repo.save_chapter_cache(cid, {"translation_signature": signature})

            url = cache.get("telegraph_url") or await self.repo.get_cached_telegraph(cid)
            if url:
                await self.repo.save_chapter_cache(cid, {"telegraph_url": url, "status": "ready", "error": ""})
                return url, True

            translated = await self.repo.get_chapter_translated_text(cid)
            if not translated:
                translated = await self.translator.translate_text(original)
                await self.repo.save_chapter_translated_text(cid, translated)

            translated_title = cache.get("translated_title") or await self.repo.get_cached_title(cid)
            if translated_title and contains_cyrillic(translated_title):
                translated_title = self.telegraph.clean_title(translated_title)
            else:
                translated_title = self.telegraph.clean_title(await self.translator.translate_title(chapter.title))
            await self.repo.save_cached_title(cid, translated_title)
            await self.repo.save_chapter_cache(cid, {"translated_title": translated_title})

            new_url = await self.telegraph.create_page(translated_title, self.telegraph.text_to_html(translated))
            if not new_url:
                await self.repo.set_chapter_status(cid, "failed", "telegraph_failed")
                await self.repo.save_translation_error(cid, "telegraph_failed")
                return None, False

            await self.repo.save_telegraph_url(cid, new_url)
            await self.repo.save_chapter_cache(cid, {"telegraph_url": new_url, "status": "ready", "error": ""})
            await self.repo.set_chapter_status(cid, "ready")
            return new_url, True
        except Exception:
            logger.exception("Unexpected translation error for %s", cid)
            await self.repo.set_chapter_status(cid, "failed", "unexpected_error")
            await self.repo.save_translation_error(cid, "unexpected_error")
            raise
        finally:
            await self.repo.release_translation_lock(cid)

    async def run(self) -> None:
        logger.info("Worker loop started")
        while True:
            job = await self.queue.dequeue_json_job(self.repo.settings.chapter_job_queue, timeout=5)
            if not job:
                continue
            chapter_id = job.get("chapter_id")
            chapter_number = job.get("chapter_number")
            requested_by = job.get("requested_by")
            notify_user = bool(job.get("notify_user"))
            broadcast = bool(job.get("broadcast"))
            source = job.get("source", "user")
            identity = str(chapter_id) if chapter_id is not None else (str(chapter_number) if chapter_number is not None else None)
            waiters = await self.repo.pop_chapter_waiters(identity) if identity else set()
            if requested_by and notify_user:
                waiters.add(int(requested_by))
            chapter = await self.repo.get_chapter_meta(str(chapter_id)) if chapter_id else None
            try:
                if chapter is None and chapter_number is not None:
                    chapter = await self.parser.find_chapter_by_number(int(chapter_number))
                    if chapter:
                        await self.repo.save_chapter_meta(chapter)
                if chapter is None:
                    for user_id in waiters:
                        await self.queue.enqueue_notification_job({"type": "user_chapter_failed", "user_id": user_id, "chapter_number": chapter_number, "reason": "not_found"})
                    continue
                waiters.update(await self.repo.pop_chapter_waiters(chapter.id))
                url, success = await self.process_chapter_translation(chapter)
                if success and url:
                    if source == "monitor":
                        await self.repo.save_last_chapter(chapter.id)
                    for user_id in waiters:
                        await self.queue.enqueue_notification_job({"type": "user_chapter_ready", "user_id": user_id, "chapter_id": chapter.id, "chapter_title": chapter.title, "url": url})
                    if broadcast:
                        await self.queue.enqueue_notification_job({"type": "broadcast_new_chapter", "chapter_id": chapter.id, "chapter_title": chapter.title, "url": url})
                    continue
                for user_id in waiters:
                    await self.queue.enqueue_notification_job({"type": "user_chapter_failed", "user_id": user_id, "chapter_number": chapter_number or chapter.id, "reason": await self.repo.get_translation_error(chapter.id) or "unexpected_error"})
            except Exception:
                logger.exception("Worker job failed: %s", job)
                failure_reason = await self.repo.get_translation_error(chapter.id if chapter else str(chapter_id)) or "unexpected_error"
                for user_id in waiters:
                    await self.queue.enqueue_notification_job({"type": "user_chapter_failed", "user_id": user_id, "chapter_number": chapter_number or chapter_id, "reason": failure_reason})
