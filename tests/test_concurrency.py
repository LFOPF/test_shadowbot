import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault('BOT_TOKEN', 'test')
os.environ.setdefault('OPENROUTER_API_KEY', 'test')
os.environ.setdefault('TELEGRAPH_ACCESS_TOKEN', 'test')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')

import bot


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.kv:
            return False
        self.kv[key] = value.encode() if isinstance(value, str) else value
        return True

    async def get(self, key):
        return self.kv.get(key)

    async def delete(self, *keys):
        deleted = 0
        for key in keys:
            deleted += int(self.kv.pop(key, None) is not None)
            deleted += int(self.hashes.pop(key, None) is not None)
        return deleted

    async def exists(self, key):
        return int(key in self.kv or key in self.hashes)

    async def eval(self, script, numkeys, key, token):
        current = self.kv.get(key)
        token_b = token.encode() if isinstance(token, str) else token
        if current == token_b:
            del self.kv[key]
            return 1
        return 0

    async def hget(self, name, field):
        bucket = self.hashes.get(name, {})
        field_b = field.encode() if isinstance(field, str) else field
        return bucket.get(field_b)

    async def hset(self, name, mapping=None, *args):
        bucket = self.hashes.setdefault(name, {})
        if mapping:
            for key, value in mapping.items():
                key_b = key.encode() if isinstance(key, str) else key
                bucket[key_b] = value.encode() if isinstance(value, str) else value
        elif len(args) == 2:
            field, value = args
            field_b = field.encode() if isinstance(field, str) else field
            bucket[field_b] = value.encode() if isinstance(value, str) else value
        return 1

    async def hgetall(self, name):
        return self.hashes.get(name, {}).copy()

    async def hdel(self, name, field):
        bucket = self.hashes.get(name, {})
        field_b = field.encode() if isinstance(field, str) else field
        return int(bucket.pop(field_b, None) is not None)


class DummyState:
    def __init__(self):
        self.cleared = 0

    async def clear(self):
        self.cleared += 1


class DummyResponse:
    def __init__(self, owner):
        self.owner = owner
        self.deleted = False

    async def answer(self, text, **kwargs):
        return await self.owner.answer(text, **kwargs)

    async def delete(self):
        self.deleted = True


class DummyMessage:
    def __init__(self, user_id, text):
        self.from_user = SimpleNamespace(id=user_id)
        self.text = text
        self.answers = []

    async def answer(self, text, **kwargs):
        self.answers.append(text)
        return DummyResponse(self)


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.redis_client = FakeRedis()

    async def test_same_user_second_numeric_request_is_rejected(self):
        state1 = DummyState()
        state2 = DummyState()
        msg1 = DummyMessage(101, '123')
        msg2 = DummyMessage(101, '124')
        started = asyncio.Event()
        release = asyncio.Event()

        async def fake_send(uid, chapter_num, status_msg=None, initial_message=None):
            started.set()
            await release.wait()
            return True

        with patch.object(bot, 'is_user_blocked', AsyncMock(return_value=False)), \
             patch.object(bot, 'is_user_subscribed', AsyncMock(return_value=True)), \
             patch.object(bot, 'send_chapter_to_user', side_effect=fake_send), \
             patch.object(bot, 'get_main_menu', AsyncMock(return_value=None)):
            task1 = asyncio.create_task(bot.process_chapter_number(msg1, state1))
            await started.wait()
            await bot.process_chapter_number(msg2, state2)
            release.set()
            await task1

        self.assertTrue(any('уже обрабатывается запрос главы' in text.lower() for text in msg2.answers))

    async def test_two_users_same_chapter_translate_once(self):
        chapter = {'id': '200', 'title': 'Chapter 200', 'link': 'https://example/ch200'}
        calls = {'fetch': 0, 'translate': 0, 'telegraph': 0}
        fetch_started = asyncio.Event()
        release_fetch = asyncio.Event()

        async def fake_fetch(url):
            calls['fetch'] += 1
            fetch_started.set()
            await release_fetch.wait()
            return 'original text'

        async def fake_translate(text):
            calls['translate'] += 1
            return 'перевод'

        async def fake_telegraph(title, content_html, author='Shadow Slave Bot'):
            calls['telegraph'] += 1
            return 'https://telegra.ph/ch200'

        with patch.object(bot, 'fetch_chapter_text', side_effect=fake_fetch), \
             patch.object(bot, 'translate_text', side_effect=fake_translate), \
             patch.object(bot, 'translate_title', AsyncMock(return_value='Глава 200')), \
             patch.object(bot, 'create_telegraph_page', side_effect=fake_telegraph):
            task1 = asyncio.create_task(bot.process_chapter_translation(chapter))
            await fetch_started.wait()
            task2 = asyncio.create_task(bot.process_chapter_translation(chapter))
            await asyncio.sleep(0)
            release_fetch.set()
            result1, result2 = await asyncio.gather(task1, task2)

        self.assertEqual(result1, ('https://telegra.ph/ch200', True))
        self.assertEqual(result2, ('https://telegra.ph/ch200', True))
        self.assertEqual(calls, {'fetch': 1, 'translate': 1, 'telegraph': 1})

    async def test_two_users_different_chapters_can_run_in_parallel(self):
        lock1 = await bot.acquire_translation_lock_handle('301')
        lock2 = await bot.acquire_translation_lock_handle('302')
        self.assertIsNotNone(lock1)
        self.assertIsNotNone(lock2)
        self.assertNotEqual(lock1.key, lock2.key)
        await bot.release_translation_lock_handle(lock1)
        await bot.release_translation_lock_handle(lock2)

    async def test_user_lock_released_after_success(self):
        lock = await bot.acquire_user_chapter_lock_handle(501)
        self.assertIsNotNone(lock)
        await bot.release_user_chapter_lock(501, lock.token)
        self.assertFalse(await bot.is_user_chapter_request_in_progress(501))

    async def test_user_lock_released_after_exception(self):
        state = DummyState()
        msg = DummyMessage(601, '555')
        with patch.object(bot, 'is_user_blocked', AsyncMock(return_value=False)), \
             patch.object(bot, 'is_user_subscribed', AsyncMock(return_value=True)), \
             patch.object(bot, 'send_chapter_to_user', AsyncMock(side_effect=RuntimeError('boom'))), \
             patch.object(bot, 'get_main_menu', AsyncMock(return_value=None)):
            await bot.process_chapter_number(msg, state)
        self.assertFalse(await bot.is_user_chapter_request_in_progress(601))

    async def test_user_lock_released_after_cancel(self):
        lock = await bot.acquire_user_chapter_lock_handle(701)
        self.assertIsNotNone(lock)
        msg = DummyMessage(701, '❌ Отмена')
        state = DummyState()
        with patch.object(bot, 'is_user_blocked', AsyncMock(return_value=False)), \
             patch.object(bot, 'is_user_subscribed', AsyncMock(return_value=True)), \
             patch.object(bot, 'get_main_menu', AsyncMock(return_value=None)):
            await bot.process_chapter_number(msg, state)
        self.assertTrue(await bot.is_user_chapter_request_cancelled(701))
        await bot.release_user_chapter_lock(701, lock.token)
        await bot.clear_user_chapter_request_cancelled(701)
        self.assertFalse(await bot.is_user_chapter_request_in_progress(701))

    async def test_chapter_lock_released_after_completion(self):
        chapter = {'id': '801', 'title': 'Chapter 801', 'link': 'https://example/ch801'}
        with patch.object(bot, 'fetch_chapter_text', AsyncMock(return_value='text')), \
             patch.object(bot, 'translate_text', AsyncMock(return_value='перевод')), \
             patch.object(bot, 'translate_title', AsyncMock(return_value='Глава 801')), \
             patch.object(bot, 'create_telegraph_page', AsyncMock(return_value='https://telegra.ph/ch801')):
            await bot.process_chapter_translation(chapter)
        self.assertFalse(await bot.is_translation_in_progress('801'))

    async def test_waits_for_existing_chapter_lock_instead_of_retranslating(self):
        chapter = {'id': '901', 'title': 'Chapter 901', 'link': 'https://example/ch901'}
        holder = await bot.acquire_translation_lock_handle('901')
        self.assertIsNotNone(holder)

        async def release_later():
            await asyncio.sleep(0.05)
            await bot.save_telegraph_url('901', 'https://telegra.ph/ch901')
            await bot.save_chapter_cache('901', {'telegraph_url': 'https://telegra.ph/ch901', 'status': 'ready'})
            await bot.release_translation_lock_handle(holder)

        releaser = asyncio.create_task(release_later())
        with patch.object(bot, 'fetch_chapter_text', AsyncMock(side_effect=AssertionError('should not fetch'))), \
             patch.object(bot, 'translate_text', AsyncMock(side_effect=AssertionError('should not translate'))), \
             patch.object(bot, 'create_telegraph_page', AsyncMock(side_effect=AssertionError('should not publish'))):
            result = await bot.process_chapter_translation(chapter)
        await releaser
        self.assertEqual(result, ('https://telegra.ph/ch901', True))


if __name__ == '__main__':
    unittest.main()
