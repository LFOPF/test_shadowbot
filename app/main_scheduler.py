from __future__ import annotations

import asyncio

from app.config import Settings
from app.core.lifecycle import build_container, shutdown, startup


async def main() -> None:
    settings = Settings.from_env()
    container = await build_container(settings)
    await startup(container)
    tasks: list[asyncio.Task] = []
    try:
        assert container.notifications is not None
        assert container.monitor is not None
        tasks = [
            asyncio.create_task(container.notifications.process_notification_loop()),
            asyncio.create_task(container.monitor.run()),
        ]
        await asyncio.gather(*tasks)
    finally:
        await shutdown(container, tasks)
