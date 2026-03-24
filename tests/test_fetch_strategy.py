import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

os.environ.setdefault('BOT_TOKEN', 'test')
os.environ.setdefault('OPENROUTER_API_KEY', 'test')
os.environ.setdefault('TELEGRAPH_ACCESS_TOKEN', 'test')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')
os.environ.setdefault('PREFER_HTTP_FIRST', 'true')

import bot


class FetchStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_html_http_fast_path_skips_playwright(self):
        fixture = Path(__file__).with_name("fixtures") / "Shadow Slave _ Chapters.html"
        html = fixture.read_text(encoding="utf-8")

        with patch.object(bot, 'fetch_html_http', AsyncMock(return_value=html)), \
             patch.object(bot, 'fetch_html_playwright', AsyncMock(side_effect=AssertionError('playwright must not be called'))), \
             patch.object(bot, 'prefer_http_first_enabled', return_value=True):
            result = await bot.fetch_html("https://ranobes.net/chapters/1205249/")

        self.assertEqual(result, html)

    async def test_fetch_html_fallback_to_playwright_on_incomplete_http(self):
        incomplete_html = "<html><body><div>No chapters here</div></body></html>"
        playwright_html = "<html><body>playwright content</body></html>"

        with patch.object(bot, 'fetch_html_http', AsyncMock(return_value=incomplete_html)), \
             patch.object(bot, 'fetch_html_playwright', AsyncMock(return_value=playwright_html)) as playwright_mock, \
             patch.object(bot, 'prefer_http_first_enabled', return_value=True):
            result = await bot.fetch_html("https://ranobes.net/chapters/1205249/")

        self.assertEqual(result, playwright_html)
        playwright_mock.assert_awaited_once()

    async def test_fetch_chapter_page_data_fallback_preserves_parsing(self):
        invalid_html = "<html><body><h1>Bad title</h1><div>No chapter text</div></body></html>"
        valid_html = """
        <html><body>
          <h1 itemprop='headline'>Chapter 2898: Malign and Destructive</h1>
          <div class='text' id='arrticle'>
            <p>""" + ("A" * 250) + """</p>
            <p>""" + ("B" * 250) + """</p>
          </div>
        </body></html>
        """
        expected = bot.parse_chapter_page_html(valid_html)
        self.assertTrue(expected.valid_body)

        with patch.object(bot, 'fetch_html_http', AsyncMock(return_value=invalid_html)), \
             patch.object(bot, 'fetch_chapter_page_data_playwright', AsyncMock(return_value=expected)) as playwright_mock, \
             patch.object(bot, 'prefer_http_first_enabled', return_value=True):
            parsed = await bot.fetch_chapter_page_data("https://ranobes.net/shadow-slave-v741610-1205249/3132434.html")

        self.assertEqual(parsed.title, expected.title)
        self.assertEqual(parsed.body, expected.body)
        self.assertTrue(parsed.valid_title)
        self.assertTrue(parsed.valid_body)
        playwright_mock.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
