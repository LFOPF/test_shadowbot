from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from app.config import Settings
from app.logging_utils import log_buffer
from app.queues.jobs import JobQueue
from app.repositories.redis_repo import RedisRepository

logger = logging.getLogger(__name__)


class NotificationService:
    def __init__(self, settings: Settings, repo: RedisRepository, queue: JobQueue, bot: Bot):
        self.settings = settings
        self.repo = repo
        self.queue = queue
        self.bot = bot

    async def get_main_menu(self, user_id: int) -> ReplyKeyboardMarkup:
        is_admin = self.settings.admin_id is not None and str(user_id) == self.settings.admin_id
        buttons = [
            [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="📖 Выбор главы")],
            [KeyboardButton(text="⬅️ Предыдущая глава"), KeyboardButton(text="➡️ Следующая глава")],
            [KeyboardButton(text="🤝 Поддержать")],
        ]
        row = [KeyboardButton(text="❓ Помощь")]
        if is_admin:
            row.insert(0, KeyboardButton(text="📊 Статус"))
        buttons.append(row)
        return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, input_field_placeholder="Выберите действие...")

    async def notify_all_subscribers(self, text: str, parse_mode: str = "HTML") -> None:
        subscribers = await self.repo.load_subscribers()
        semaphore = asyncio.Semaphore(10)

        async def send(uid: int) -> None:
            if await self.repo.is_user_blocked(uid):
                return
            async with semaphore:
                try:
                    await self.bot.send_message(uid, text, parse_mode=parse_mode)
                except (TelegramForbiddenError, TelegramBadRequest):
                    logger.info("User %s is unavailable", uid)
                except Exception:
                    logger.exception("Failed to send broadcast to %s", uid)

        await asyncio.gather(*(send(uid) for uid in subscribers), return_exceptions=True)

    async def process_notification_loop(self) -> None:
        logger.info("Notification loop started")
        while True:
            job = await self.queue.dequeue_json_job(self.settings.notification_job_queue, timeout=5)
            if not job:
                continue
            try:
                kind = job.get("type")
                if kind == "broadcast_new_chapter":
                    await self.notify_all_subscribers(f"📢 <b>Новая глава!</b>\n\n📖 <b>{job['chapter_title']}</b>\n\n🔗 {job['url']}")
                elif kind == "user_chapter_ready":
                    await self.repo.save_user_bookmark(int(job["user_id"]), str(job["chapter_id"]))
                    await self.bot.send_message(
                        int(job["user_id"]),
                        f"📖 <b>{job['chapter_title']}</b>\n\n🔗 {job['url']}",
                        parse_mode="HTML",
                        reply_markup=await self.get_main_menu(int(job["user_id"])),
                        disable_web_page_preview=True,
                    )
                elif kind == "user_chapter_failed":
                    error_map = {
                        "not_found": "❌ Глава не найдена.",
                        "fetch_failed": "❌ Не удалось загрузить текст главы. Попробуйте позже.",
                        "telegraph_failed": "❌ Перевод готов, но Telegraph не создал страницу. Попробуйте позже.",
                        "unexpected_error": "❌ Во время обработки главы произошла ошибка. Попробуйте позже.",
                    }
                    await self.bot.send_message(
                        int(job["user_id"]),
                        error_map.get(job.get("reason"), "❌ Не удалось создать перевод. Попробуйте позже."),
                        reply_markup=await self.get_main_menu(int(job["user_id"])),
                    )
            except Exception:
                logger.exception("Notification job failed: %s", job)

    def tail_logs(self, limit: int = 10) -> str:
        return "Последние 10 логов:\n" + "\n".join(list(log_buffer)[-limit:]) if log_buffer else "Логи отсутствуют."
