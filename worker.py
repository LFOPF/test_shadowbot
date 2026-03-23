import asyncio
import os

os.environ.setdefault("SHADOWBOT_ROLE", "worker")

from bot import main


if __name__ == "__main__":
    asyncio.run(main())
