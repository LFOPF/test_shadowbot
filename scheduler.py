import asyncio
import os

os.environ.setdefault("SHADOWBOT_ROLE", "scheduler")

from app.logging_utils import configure_logging
from app.main_scheduler import main


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
