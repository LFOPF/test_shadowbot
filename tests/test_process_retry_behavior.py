import os
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

os.environ.setdefault('BOT_TOKEN', 'test')
os.environ.setdefault('OPENROUTER_API_KEY', 'test')
os.environ.setdefault('TELEGRAPH_ACCESS_TOKEN', 'test')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')

import bot
from tests.test_concurrency import FakeRedis


class ProcessRetryBehaviorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.redis_client = FakeRedis()

    @staticmethod
    def _valid_parsed(cid: str, body: str = 'original text') -> bot.ParsedChapterPage:
        return bot.ParsedChapterPage(
            title=f'Chapter {cid}',
            title_source='h1[itemprop="headline"]',
            body=body,
            body_source='div.text#arrticle',
            chapter_number=int(cid),
            valid_title=True,
            valid_body=True,
            reasons=[],
        )

    async def test_non_retriable_error_does_not_retry_full_pipeline(self):
        chapter = {'id': '1001', 'title': 'Chapter 1001', 'link': 'https://example/1001'}
        bad_page = bot.ParsedChapterPage(
            title='Chapter 1001',
            title_source='h1[itemprop="headline"]',
            body='',
            body_source='missing',
            chapter_number=1001,
            valid_title=True,
            valid_body=False,
            reasons=['invalid_body'],
        )
        fetch_mock = AsyncMock(return_value=bad_page)

        with patch.object(bot, 'fetch_chapter_page_data', fetch_mock), \
             patch.object(bot, 'translate_text', AsyncMock(side_effect=AssertionError('should not translate'))), \
             patch.object(bot, 'create_telegraph_page', AsyncMock(side_effect=AssertionError('should not publish'))):
            result = await bot.process_chapter_translation(chapter)

        self.assertEqual(result, (None, False))
        self.assertEqual(fetch_mock.await_count, 1)
        cache = await bot.get_chapter_cache('1001')
        self.assertEqual(cache.get('status'), 'failed')
        self.assertEqual(cache.get('error'), 'invalid_body')

    async def test_temporary_telegraph_error_is_retried(self):
        chapter = {'id': '1002', 'title': 'Chapter 1002', 'link': 'https://example/1002'}
        create_mock = AsyncMock(side_effect=[aiohttp.ClientError('temp'), 'https://telegra.ph/ch1002'])

        with patch.object(bot, 'fetch_chapter_page_data', AsyncMock(return_value=self._valid_parsed('1002'))), \
             patch.object(bot, 'translate_text', AsyncMock(return_value='перевод')), \
             patch.object(bot, 'translate_title', AsyncMock(return_value='Глава 1002')), \
             patch.object(bot, 'create_telegraph_page', create_mock):
            result = await bot.process_chapter_translation(chapter)

        self.assertEqual(result, ('https://telegra.ph/ch1002', True))
        self.assertEqual(create_mock.await_count, 2)

    async def test_temporary_translation_error_does_not_refetch_original_text(self):
        chapter = {'id': '1003', 'title': 'Chapter 1003', 'link': 'https://example/1003'}

        with patch.object(bot, 'fetch_chapter_page_data', AsyncMock(return_value=self._valid_parsed('1003'))), \
             patch.object(bot, 'translate_text', AsyncMock(side_effect=aiohttp.ClientError('temp'))):
            first = await bot.process_chapter_translation(chapter)

        self.assertEqual(first, (None, False))

        with patch.object(bot, 'fetch_chapter_page_data', AsyncMock(side_effect=AssertionError('must not fetch again'))), \
             patch.object(bot, 'translate_text', AsyncMock(return_value='перевод')), \
             patch.object(bot, 'translate_title', AsyncMock(return_value='Глава 1003')), \
             patch.object(bot, 'create_telegraph_page', AsyncMock(return_value='https://telegra.ph/ch1003')):
            second = await bot.process_chapter_translation(chapter)

        self.assertEqual(second, ('https://telegra.ph/ch1003', True))

    async def test_restart_after_partial_success_uses_cached_results(self):
        chapter = {'id': '1004', 'title': 'Chapter 1004', 'link': 'https://example/1004'}
        await bot.save_chapter_original_text('1004', 'original from cache')
        await bot.save_chapter_translated_text('1004', 'перевод из кеша')
        await bot.save_chapter_cache('1004', {'translated_title': 'Глава 1004'})

        with patch.object(bot, 'fetch_chapter_page_data', AsyncMock(side_effect=AssertionError('must not refetch'))), \
             patch.object(bot, 'translate_text', AsyncMock(side_effect=AssertionError('must not retranslate text'))), \
             patch.object(bot, 'translate_title', AsyncMock(side_effect=AssertionError('must not retranslate title'))), \
             patch.object(bot, 'create_telegraph_page', AsyncMock(return_value='https://telegra.ph/ch1004')) as create_mock:
            result = await bot.process_chapter_translation(chapter)

        self.assertEqual(result, ('https://telegra.ph/ch1004', True))
        self.assertEqual(create_mock.await_count, 1)


if __name__ == '__main__':
    unittest.main()
