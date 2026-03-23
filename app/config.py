from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    role: str
    bot_token: str | None
    openrouter_api_key: str | None
    telegraph_access_token: str | None
    redis_url: str | None
    admin_id: str | None
    target_url: str
    site_url: str
    site_name: str
    novel_id: str
    check_interval: int
    browser_idle_timeout: int
    telegraph_title_max_length: int
    chapters_per_page: int
    max_pages: int
    translation_lock_ttl: int
    translation_wait_timeout: int
    translation_wait_step: int
    error_ttl: int
    playwright_concurrency: int
    glossary_cache_ttl: int
    translation_cache_version: str
    chapter_job_queue: str
    notification_job_queue: str
    monitor_trigger_queue: str
    job_dedupe_ttl: int
    bot_ready_wait_timeout: int
    glossary_path: Path
    openrouter_model: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            role=os.getenv("SHADOWBOT_ROLE", "bot-api"),
            bot_token=os.getenv("BOT_TOKEN"),
            openrouter_api_key=os.getenv("OPENROUTER_API_KEY"),
            telegraph_access_token=os.getenv("TELEGRAPH_ACCESS_TOKEN"),
            redis_url=os.getenv("REDIS_URL"),
            admin_id=os.getenv("ADMIN_ID"),
            target_url=os.getenv("TARGET_URL", "https://ranobes.net/chapters/1205249/"),
            site_url=os.getenv("SITE_URL", "https://t.me/SHDSlaveBot"),
            site_name=os.getenv("SITE_NAME", "ShadowSlaveTranslator"),
            novel_id=os.getenv("NOVEL_ID", "1205249"),
            check_interval=int(os.getenv("CHECK_INTERVAL", "10800")),
            browser_idle_timeout=int(os.getenv("BROWSER_IDLE_TIMEOUT", "180")),
            telegraph_title_max_length=int(os.getenv("TELEGRAPH_TITLE_MAX_LENGTH", "200")),
            chapters_per_page=int(os.getenv("CHAPTERS_PER_PAGE", "25")),
            max_pages=int(os.getenv("MAX_PAGES", "120")),
            translation_lock_ttl=int(os.getenv("TRANSLATION_LOCK_TTL", str(15 * 60))),
            translation_wait_timeout=int(os.getenv("TRANSLATION_WAIT_TIMEOUT", "90")),
            translation_wait_step=int(os.getenv("TRANSLATION_WAIT_STEP", "2")),
            error_ttl=int(os.getenv("ERROR_TTL", str(5 * 60))),
            playwright_concurrency=int(os.getenv("PLAYWRIGHT_CONCURRENCY", "2")),
            glossary_cache_ttl=int(os.getenv("GLOSSARY_CACHE_TTL", "300")),
            translation_cache_version=os.getenv("TRANSLATION_CACHE_VERSION", "v1"),
            chapter_job_queue=os.getenv("CHAPTER_JOB_QUEUE", "queue:chapter_jobs"),
            notification_job_queue=os.getenv("NOTIFICATION_JOB_QUEUE", "queue:notification_jobs"),
            monitor_trigger_queue=os.getenv("MONITOR_TRIGGER_QUEUE", "queue:monitor_triggers"),
            job_dedupe_ttl=int(os.getenv("JOB_DEDUP_TTL", "300")),
            bot_ready_wait_timeout=int(os.getenv("BOT_READY_WAIT_TIMEOUT", "3")),
            glossary_path=Path(os.getenv("GLOSSARY_PATH", "glossary.txt")).resolve(),
            openrouter_model=os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash-lite"),
        )

    def validate(self) -> None:
        required_by_role = {
            "bot-api": ["BOT_TOKEN", "REDIS_URL"],
            "worker": ["OPENROUTER_API_KEY", "TELEGRAPH_ACCESS_TOKEN", "REDIS_URL"],
            "scheduler": ["BOT_TOKEN", "REDIS_URL"],
        }
        required = required_by_role.get(self.role)
        if required is None:
            raise ValueError(f"Неизвестная роль SHADOWBOT_ROLE={self.role!r}")
        values = {
            "BOT_TOKEN": self.bot_token,
            "OPENROUTER_API_KEY": self.openrouter_api_key,
            "TELEGRAPH_ACCESS_TOKEN": self.telegraph_access_token,
            "REDIS_URL": self.redis_url,
        }
        missing = [name for name in required if not values.get(name)]
        if missing:
            raise ValueError(f"Для роли {self.role} не заданы обязательные переменные: {', '.join(missing)}")
