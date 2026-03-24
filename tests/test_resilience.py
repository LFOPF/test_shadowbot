import os
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

os.environ.setdefault('BOT_TOKEN', 'test')
os.environ.setdefault('OPENROUTER_API_KEY', 'test')
os.environ.setdefault('TELEGRAPH_ACCESS_TOKEN', 'test')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')

import bot


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.ping_error = None

    async def ping(self):
        if self.ping_error:
            raise self.ping_error
        return True

    async def hlen(self, name):
        return len(self.hashes.get(name, {}))

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


class FakeResponse:
    def __init__(self, *, status=200, json_data=None, text_data=""):
        self.status = status
        self._json_data = json_data if json_data is not None else {}
        self._text_data = text_data

    async def json(self):
        return self._json_data

    async def text(self):
        return self._text_data


class FakePostContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def post(self, *args, **kwargs):
        if self.calls >= len(self._responses):
            raise AssertionError('Unexpected post call')
        response = self._responses[self.calls]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        return FakePostContext(response)


class ResilienceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.redis_client = FakeRedis()

    async def test_startup_check_fails_when_system_prompt_missing(self):
        with patch.object(bot, '_is_prompt_file_ready', side_effect=[False, True]):
            with self.assertRaises(RuntimeError):
                await bot.run_startup_checks()

    async def test_startup_check_fails_when_user_prompt_missing(self):
        with patch.object(bot, '_is_prompt_file_ready', side_effect=[True, False]):
            with self.assertRaises(RuntimeError):
                await bot.run_startup_checks()

    async def test_startup_check_fails_when_glossary_is_broken(self):
        with patch.object(bot, '_is_prompt_file_ready', side_effect=[True, True]), \
             patch('builtins.open', side_effect=OSError('broken glossary')):
            with self.assertRaises(RuntimeError):
                await bot.run_startup_checks()

    async def test_startup_check_fails_when_redis_unavailable(self):
        bot.redis_client.ping_error = ConnectionError('redis down')
        with self.assertRaises(ConnectionError):
            await bot.run_startup_checks()

    async def test_create_telegraph_page_retries_on_temporary_error(self):
        session = FakeSession([
            FakeResponse(status=502, json_data={}),
            FakeResponse(status=200, json_data={'ok': True, 'result': {'url': 'https://telegra.ph/x'}}),
        ])
        with patch.object(bot, 'get_http_session', AsyncMock(return_value=session)):
            url = await bot.create_telegraph_page('Title', '<p>body</p>')
        self.assertEqual(url, 'https://telegra.ph/x')
        self.assertEqual(session.calls, 2)

    async def test_create_telegraph_page_title_too_long_retries_with_trim(self):
        session = FakeSession([
            FakeResponse(status=200, json_data={'ok': False, 'error': 'TITLE_TOO_LONG'}),
            FakeResponse(status=200, json_data={'ok': True, 'result': {'url': 'https://telegra.ph/title-ok'}}),
        ])
        with patch.object(bot, 'get_http_session', AsyncMock(return_value=session)):
            url = await bot.create_telegraph_page('X' * 400, '<p>body</p>')
        self.assertEqual(url, 'https://telegra.ph/title-ok')
        self.assertEqual(session.calls, 2)

    async def test_find_chapter_fallback_after_page_limit(self):
        async def fake_fetch_html(url):
            if url.endswith('/page/120/'):
                return '<html>p120</html>'
            if url.endswith('/page/180/'):
                return '<html>p180</html>'
            if url.endswith('/page/150/'):
                return '<html>p150</html>'
            if url.endswith('/page/121/') or url.endswith('/page/239/'):
                return '<html>window-edge</html>'
            return None

        def fake_parse(html):
            if html == '<html>p120</html>':
                return [{'id': '1000'}, {'id': '900'}]
            if html == '<html>p180</html>':
                return [{'id': '600'}, {'id': '500'}]
            if html == '<html>p150</html>':
                return [{'id': '820'}, {'id': '790'}]
            if html == '<html>window-edge</html>':
                return [{'id': '850'}, {'id': '700'}]
            return []

        with patch.object(bot, 'fetch_html', AsyncMock(side_effect=fake_fetch_html)), \
             patch.object(bot, 'parse_chapters', side_effect=fake_parse), \
             patch('asyncio.sleep', AsyncMock()):
            chapter = await bot.find_chapter_by_number_binary(790)

        self.assertIsNotNone(chapter)
        self.assertEqual(chapter['id'], '790')

    async def test_request_translation_completion_empty_response(self):
        session = FakeSession([
            FakeResponse(status=200, json_data={'choices': [{'message': {'content': ''}}]}),
        ])
        with self.assertRaises(ValueError):
            await bot.request_translation_completion(
                session=session,
                headers={},
                messages=[{'role': 'user', 'content': 'x'}],
                stage_name='test_stage',
                temperature=0.1,
                top_p=1.0,
            )

    async def test_request_translation_completion_broken_response(self):
        session = FakeSession([
            FakeResponse(status=200, json_data={'oops': 'bad'}),
        ])
        with self.assertRaises(ValueError):
            await bot.request_translation_completion(
                session=session,
                headers={},
                messages=[{'role': 'user', 'content': 'x'}],
                stage_name='test_stage',
                temperature=0.1,
                top_p=1.0,
            )

    async def test_translate_text_retries_on_temporary_openrouter_error(self):
        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[
                 aiohttp.ClientError('temp'),
                 'draft translation',
                 'edited translation',
             ])) as completion_mock, \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())):
            result = await bot.translate_text('hello')

        self.assertEqual(result, 'edited translation')
        self.assertEqual(completion_mock.call_count, 3)


if __name__ == '__main__':
    unittest.main()
