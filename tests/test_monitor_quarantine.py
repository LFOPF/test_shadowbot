import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault('BOT_TOKEN', 'test')
os.environ.setdefault('OPENROUTER_API_KEY', 'test')
os.environ.setdefault('TELEGRAPH_ACCESS_TOKEN', 'test')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')

import bot
from tests.test_concurrency import FakeRedis


class MonitorQuarantineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.redis_client = FakeRedis()

    async def test_quarantine_chapter_does_not_break_cycle_in_permissive_mode(self):
        chapters = [
            {'id': '3', 'title': 'Chapter 3', 'link': 'https://example/3'},
            {'id': '2', 'title': 'Chapter 2', 'link': 'https://example/2'},
        ]
        process_mock = AsyncMock(return_value=('https://telegra.ph/ch3', True))

        with patch.dict(os.environ, {'MONITOR_STRICT_ORDER': 'false'}), \
             patch.object(bot, 'fetch_html', AsyncMock(return_value='<html/>')), \
             patch.object(bot, 'parse_chapters', return_value=chapters), \
             patch.object(bot, 'save_chapter_meta', AsyncMock()), \
             patch.object(bot, 'get_last_chapter', AsyncMock(return_value='1')), \
             patch.object(bot, 'validate_monitor_candidate', AsyncMock(side_effect=[(False, 'invalid_body'), (True, 'ok')])), \
             patch.object(bot, 'process_chapter_translation', process_mock), \
             patch.object(bot, 'get_chapter_cache', AsyncMock(return_value={})), \
             patch.object(bot, 'notify_all_subscribers', AsyncMock()), \
             patch.object(bot, 'save_last_chapter', AsyncMock()), \
             patch.object(bot, 'ADMIN_ID', None), \
             patch('asyncio.sleep', AsyncMock()):
            await bot.monitor(check_once=True)

        self.assertEqual(process_mock.await_count, 1)
        self.assertEqual(process_mock.await_args_list[0].args[0]['id'], '3')

    async def test_temporary_failure_does_not_block_next_in_permissive_mode(self):
        chapters = [
            {'id': '3', 'title': 'Chapter 3', 'link': 'https://example/3'},
            {'id': '2', 'title': 'Chapter 2', 'link': 'https://example/2'},
        ]
        process_mock = AsyncMock(side_effect=[(None, False), ('https://telegra.ph/ch3', True)])

        async def fake_error(cid):
            return 'fetch_temporary_error' if cid == '2' else None

        with patch.dict(os.environ, {'MONITOR_STRICT_ORDER': 'false'}), \
             patch.object(bot, 'fetch_html', AsyncMock(return_value='<html/>')), \
             patch.object(bot, 'parse_chapters', return_value=chapters), \
             patch.object(bot, 'save_chapter_meta', AsyncMock()), \
             patch.object(bot, 'get_last_chapter', AsyncMock(return_value='1')), \
             patch.object(bot, 'validate_monitor_candidate', AsyncMock(return_value=(True, 'ok'))), \
             patch.object(bot, 'process_chapter_translation', process_mock), \
             patch.object(bot, 'get_translation_error', AsyncMock(side_effect=fake_error)), \
             patch.object(bot, 'get_chapter_cache', AsyncMock(return_value={})), \
             patch.object(bot, 'save_chapter_cache', AsyncMock()), \
             patch.object(bot, 'notify_all_subscribers', AsyncMock()), \
             patch.object(bot, 'save_last_chapter', AsyncMock()), \
             patch.object(bot, 'ADMIN_ID', None), \
             patch('asyncio.sleep', AsyncMock()):
            await bot.monitor(check_once=True)

        self.assertEqual(process_mock.await_count, 2)
        self.assertEqual([call.args[0]['id'] for call in process_mock.await_args_list], ['2', '3'])

    async def test_strict_order_keeps_cautious_behavior(self):
        chapters = [
            {'id': '3', 'title': 'Chapter 3', 'link': 'https://example/3'},
            {'id': '2', 'title': 'Chapter 2', 'link': 'https://example/2'},
        ]
        process_mock = AsyncMock(return_value=(None, False))

        with patch.dict(os.environ, {'MONITOR_STRICT_ORDER': 'true'}), \
             patch.object(bot, 'fetch_html', AsyncMock(return_value='<html/>')), \
             patch.object(bot, 'parse_chapters', return_value=chapters), \
             patch.object(bot, 'save_chapter_meta', AsyncMock()), \
             patch.object(bot, 'get_last_chapter', AsyncMock(return_value='1')), \
             patch.object(bot, 'validate_monitor_candidate', AsyncMock(return_value=(True, 'ok'))), \
             patch.object(bot, 'process_chapter_translation', process_mock), \
             patch.object(bot, 'get_translation_error', AsyncMock(return_value='fetch_temporary_error')), \
             patch.object(bot, 'get_chapter_cache', AsyncMock(return_value={})), \
             patch.object(bot, 'save_chapter_cache', AsyncMock()), \
             patch.object(bot, 'notify_all_subscribers', AsyncMock()), \
             patch.object(bot, 'save_last_chapter', AsyncMock()), \
             patch.object(bot, 'ADMIN_ID', None), \
             patch('asyncio.sleep', AsyncMock()):
            await bot.monitor(check_once=True)

        self.assertEqual(process_mock.await_count, 1)
        self.assertEqual(process_mock.await_args_list[0].args[0]['id'], '2')

    async def test_last_chapter_updated_for_contiguous_success(self):
        chapters = [
            {'id': '3', 'title': 'Chapter 3', 'link': 'https://example/3'},
            {'id': '2', 'title': 'Chapter 2', 'link': 'https://example/2'},
        ]
        save_last_mock = AsyncMock()

        with patch.dict(os.environ, {'MONITOR_STRICT_ORDER': 'false'}), \
             patch.object(bot, 'fetch_html', AsyncMock(return_value='<html/>')), \
             patch.object(bot, 'parse_chapters', return_value=chapters), \
             patch.object(bot, 'save_chapter_meta', AsyncMock()), \
             patch.object(bot, 'get_last_chapter', AsyncMock(return_value='1')), \
             patch.object(bot, 'validate_monitor_candidate', AsyncMock(return_value=(True, 'ok'))), \
             patch.object(bot, 'process_chapter_translation', AsyncMock(side_effect=[
                 ('https://telegra.ph/ch2', True),
                 ('https://telegra.ph/ch3', True),
             ])), \
             patch.object(bot, 'get_chapter_cache', AsyncMock(return_value={})), \
             patch.object(bot, 'save_chapter_cache', AsyncMock()), \
             patch.object(bot, 'notify_all_subscribers', AsyncMock()), \
             patch.object(bot, 'save_last_chapter', save_last_mock), \
             patch.object(bot, 'ADMIN_ID', None), \
             patch('asyncio.sleep', AsyncMock()):
            await bot.monitor(check_once=True)

        save_last_mock.assert_awaited_once_with('3')


if __name__ == '__main__':
    unittest.main()
