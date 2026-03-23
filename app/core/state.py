from __future__ import annotations

from dataclasses import dataclass

from aiogram import Bot

from app.config import Settings
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


@dataclass(slots=True)
class AppContainer:
    settings: Settings
    repo: RedisRepository
    queue: JobQueue
    http: HttpSessionManager
    browser: PlaywrightManager
    parser: ChapterParser
    translator: TranslationService
    telegraph: TelegraphService
    bot: Bot | None = None
    notifications: NotificationService | None = None
    chapter_requests: ChapterRequestService | None = None
    worker: WorkerService | None = None
    monitor: MonitorService | None = None
