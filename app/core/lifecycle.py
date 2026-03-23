from __future__ import annotations

import asyncio
import logging

from aiogram import Bot

from app.config import Settings
from app.core.state import AppContainer
from app.queues.jobs import JobQueue
from app.repositories.redis_repo import RedisRepository
from app.services.chapter_flow import ChapterRequestService
from app.services.http import HttpSessionManager
from app.services.monitor import MonitorService
from app.services.notifications import NotificationService
from app.services.parser import ChapterParser, PlaywrightManager
from app.services.telegraph import TelegraphService
from app.services.translation import TranslationService
from app.services.worker_pipeline import WorkerService

logger = logging.getLogger(__name__)


async def build_container(settings: Settings) -> AppContainer:
    settings.validate()
    repo = await RedisRepository.create(settings)
    queue = JobQueue(repo)
    http = HttpSessionManager()
    browser = PlaywrightManager(settings)
    parser = ChapterParser(settings, repo, browser)
    translator = TranslationService(settings, repo, http)
    telegraph = TelegraphService(settings, http)
    bot = Bot(token=settings.bot_token) if settings.role in {"bot-api", "scheduler"} else None
    notifications = NotificationService(settings, repo, queue, bot) if bot else None
    chapter_requests = ChapterRequestService(repo, queue, notifications) if notifications else None
    worker = WorkerService(repo, queue, parser, translator, telegraph) if settings.role == "worker" else None
    monitor = MonitorService(repo, queue, parser) if settings.role == "scheduler" else None
    return AppContainer(
        settings=settings,
        repo=repo,
        queue=queue,
        http=http,
        browser=browser,
        parser=parser,
        translator=translator,
        telegraph=telegraph,
        bot=bot,
        notifications=notifications,
        chapter_requests=chapter_requests,
        worker=worker,
        monitor=monitor,
    )


async def startup(container: AppContainer) -> None:
    logger.info("Starting role %s", container.settings.role)
    if container.settings.role in {"worker", "scheduler"}:
        count = await container.translator.preload_glossary(force=False)
        logger.info("Glossary loaded: %s terms", count)


async def shutdown(container: AppContainer, background_tasks: list[asyncio.Task] | None = None) -> None:
    logger.info("Shutting down role %s", container.settings.role)
    if background_tasks:
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
    await container.browser.close()
    await container.http.close()
    if container.bot:
        await container.bot.session.close()
    await container.repo.close()
