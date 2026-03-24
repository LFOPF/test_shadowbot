import os
import unittest
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


class ParserGuardrailsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.redis_client = FakeRedis()

    def test_parse_chapter_page_ignores_comments_and_extracts_body(self):
        html = """
        <html><body>
          <h1 itemprop='headline'>Chapter 2898: Malign and Destructive</h1>
          <div class='text' id='arrticle'>
            <p>""" + ("A" * 250) + """</p>
            <blockquote>4500 is what I'm hoping for</blockquote>
            <p>""" + ("B" * 250) + """</p>
          </div>
          <div id='comments'><p>4500 is what I'm hoping for</p></div>
        </body></html>
        """
        parsed = bot.parse_chapter_page_html(html)

        self.assertTrue(parsed.valid_title)
        self.assertTrue(parsed.valid_body)
        self.assertEqual(parsed.title, "Chapter 2898: Malign and Destructive")
        self.assertNotIn("4500 is what", parsed.body)

    def test_invalid_title_from_comment_text_is_rejected(self):
        html = """
        <html><body>
          <h1 itemprop='headline'>4500 is what I'm hoping for</h1>
          <div class='text' id='arrticle'><p>""" + ("X" * 500) + """</p></div>
        </body></html>
        """
        parsed = bot.parse_chapter_page_html(html)
        self.assertFalse(parsed.valid_title)
        self.assertIn('invalid_title', parsed.reasons)

    def test_parse_chapters_ignores_non_chapter_links(self):
        html = """
        <html><body>
          <div class='recent-comments'>
            <a href='/shadow-slave-v741610-1205249/3123434.html#comment-id-1'>4500 is what I'm hoping for</a>
          </div>
          <a href='/chapters/1205249/2898.html'>Chapter 2898: Malign and Destructive</a>
          <a href='/chapters/1205249/2899.html'>lol finally</a>
        </body></html>
        """
        chapters = bot.parse_chapters(html)
        self.assertEqual(len(chapters), 1)
        self.assertEqual(chapters[0]['id'], '2898')

    async def test_no_stale_body_fallback_when_source_url_changes(self):
        chapter = {'id': '2898', 'title': 'Chapter 2898: Malign and Destructive', 'link': 'https://example/new'}
        await bot.save_chapter_original_text('2898', 'STALE_BODY')
        await bot.save_chapter_cache('2898', {'source_url': 'https://example/old'})

        bad_page = bot.ParsedChapterPage(
            title='Chapter 2898: Malign and Destructive',
            title_source='h1[itemprop="headline"]',
            body='',
            body_source='missing',
            chapter_number=2898,
            valid_title=True,
            valid_body=False,
            reasons=['invalid_body'],
        )

        with patch.object(bot, 'fetch_chapter_page_data', AsyncMock(return_value=bad_page)), \
             patch.object(bot, 'create_telegraph_page', AsyncMock(side_effect=AssertionError('must not publish'))):
            url, success = await bot.process_chapter_translation(chapter)

        self.assertEqual((url, success), (None, False))

    async def test_duplicate_body_detection(self):
        await bot.save_chapter_original_text('2898', 'same body' * 100)
        candidate = {'id': '2899', 'title': 'Chapter 2899: Next', 'link': 'https://example/ch2899'}
        parsed = bot.ParsedChapterPage(
            title='Chapter 2899: Next',
            title_source='h1[itemprop="headline"]',
            body='same body' * 100,
            body_source='div.text#arrticle',
            chapter_number=2899,
            valid_title=True,
            valid_body=True,
            reasons=[],
        )
        with patch.object(bot, 'fetch_chapter_page_data', AsyncMock(return_value=parsed)):
            ok, reason = await bot.validate_monitor_candidate(candidate, previous_latest_id=2898)
        self.assertFalse(ok)
        self.assertEqual(reason, 'duplicate_body')


if __name__ == '__main__':
    unittest.main()
