from __future__ import annotations

from app.bot.app import BotApplication
from app.config import Settings
from app.core.lifecycle import build_container, shutdown, startup


async def main() -> None:
    settings = Settings.from_env()
    container = await build_container(settings)
    await startup(container)
    try:
        await BotApplication(container).run()
    finally:
        await shutdown(container)
