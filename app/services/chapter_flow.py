from __future__ import annotations

import logging
import re
from typing import Optional

from aiogram import Bot, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup

from app.models import ChapterMeta
from app.queues.jobs import JobQueue
from app.repositories.redis_repo import RedisRepository
from app.services.notifications import NotificationService

logger = logging.getLogger(__name__)


async def safe_edit_text(message: types.Message, text: str, parse_mode: str | None = None, reply_markup: InlineKeyboardMarkup | ReplyKeyboardMarkup | None = None) -> None:
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            logger.warning("Failed to edit message: %s", exc)


async def safe_delete(message: Optional[types.Message]) -> None:
    if message is None:
        return
    try:
        await message.delete()
    except Exception:
        return


class ChapterRequestService:
    def __init__(self, repo: RedisRepository, queue: JobQueue, notifications: NotificationService):
        self.repo = repo
        self.queue = queue
        self.notifications = notifications

    async def send_chapter_to_user(
        self,
        bot: Bot,
        user_id: int,
        chapter_num: int,
        status_msg: types.Message | None = None,
        initial_message: types.Message | None = None,
    ) -> bool:
        if await self.repo.is_user_blocked(user_id):
            await safe_delete(status_msg)
            if initial_message:
                await initial_message.answer("Вы заблокированы.")
            return False

        cached_url = await self.repo.get_cached_telegraph(str(chapter_num))
        if not cached_url:
            cached_url = (await self.repo.get_chapter_cache(str(chapter_num))).get("telegraph_url")
            if cached_url:
                await self.repo.save_telegraph_url(str(chapter_num), cached_url)
        if cached_url:
            await safe_delete(status_msg)
            await (initial_message or status_msg).answer(
                f"📖 <b>Глава {chapter_num}</b>\n\n🔗 {cached_url}",
                parse_mode="HTML",
                reply_markup=await self.notifications.get_main_menu(user_id),
            )
            await self.repo.save_user_bookmark(user_id, str(chapter_num))
            return True

        chapter = await self.repo.get_chapter_meta(str(chapter_num))
        if chapter:
            ready_url = await self.repo.wait_for_ready_translation(chapter.id, timeout=self.repo.settings.bot_ready_wait_timeout)
            if ready_url:
                await safe_delete(status_msg)
                await (initial_message or status_msg).answer(
                    f"📖 <b>{chapter.title}</b>\n\n🔗 {ready_url}",
                    parse_mode="HTML",
                    reply_markup=await self.notifications.get_main_menu(user_id),
                )
                await self.repo.save_user_bookmark(user_id, chapter.id)
                return True

        queued = await self.queue.enqueue_chapter_job(
            chapter_number=chapter_num,
            chapter_id=chapter.id if chapter else None,
            requested_by=user_id,
            source="user",
            notify_user=True,
            broadcast=False,
        )
        await safe_delete(status_msg)
        await (initial_message or status_msg).answer(
            (
                f"⏳ Глава {chapter_num} поставлена в обработку.\nЯ пришлю ссылку отдельным сообщением, как только worker закончит перевод."
                if queued
                else f"⏳ Глава {chapter_num} уже обрабатывается.\nКогда перевод будет готов, scheduler отправит вам ссылку."
            ),
            reply_markup=await self.notifications.get_main_menu(user_id),
        )
        return True


def contains_cyrillic(text: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", text or ""))
