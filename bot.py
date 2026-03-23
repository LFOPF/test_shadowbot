"""Compatibility shim for legacy entrypoint.

Use bot_api.py, worker.py, or scheduler.py for role-specific startup.
"""

import asyncio
import os

from app.logging_utils import configure_logging
from app.main_bot import main as bot_main
from app.main_scheduler import main as scheduler_main
from app.main_worker import main as worker_main


async def main() -> None:
    role = os.getenv("SHADOWBOT_ROLE", "bot-api")
    if role == "bot-api":
        await bot_main()
    elif role == "worker":
        await worker_main()
    elif role == "scheduler":
        await scheduler_main()
    else:
        raise ValueError(f"Неизвестная роль SHADOWBOT_ROLE={role!r}")


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
