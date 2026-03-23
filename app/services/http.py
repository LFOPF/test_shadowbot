from __future__ import annotations

import aiohttp


class HttpSessionManager:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=130, connect=20, sock_read=120)
            connector = aiohttp.TCPConnector(limit=20, limit_per_host=10, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
