import asyncio
import os

os.environ.setdefault("SHADOWBOT_ROLE", "worker")

from app.logging_utils import configure_logging
from app.main_worker import main


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
