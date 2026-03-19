# migrate_telegraph.py
import asyncio
import os
import redis.asyncio as redis

async def migrate():
    # Railway автоматически предоставит переменную окружения REDIS_URL
    redis_client = await redis.from_url(os.getenv("REDIS_URL"))
    print("Подключение к Redis установлено.")

    keys = await redis_client.keys("trans_url:*")
    print(f"Найдено старых ключей: {len(keys)}")

    if not keys:
        print("Старые ключи не найдены. Миграция не требуется.")
        await redis_client.aclose()
        return

    for key in keys:
        chapter_id = key.decode().split(":", 1)[1]
        url = await redis_client.get(key)
        if url:
            await redis_client.hset("telegraph_urls", chapter_id, url)
            print(f"✅ Перенесена глава {chapter_id}")

    print("✅ Миграция всех данных завершена.")
    await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(migrate())
