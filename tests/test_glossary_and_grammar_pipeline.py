import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

os.environ.setdefault('BOT_TOKEN', 'test')
os.environ.setdefault('OPENROUTER_API_KEY', 'test')
os.environ.setdefault('TELEGRAPH_ACCESS_TOKEN', 'test')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')

import bot


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    async def hlen(self, name):
        return len(self.hashes.get(name, {}))

    async def hset(self, name, mapping=None, *args):
        bucket = self.hashes.setdefault(name, {})
        if mapping:
            for key, value in mapping.items():
                key_b = key.encode() if isinstance(key, str) else key
                value_b = value.encode() if isinstance(value, str) else value
                bucket[key_b] = value_b
        elif len(args) == 2:
            field, value = args
            field_b = field.encode() if isinstance(field, str) else field
            value_b = value.encode() if isinstance(value, str) else value
            bucket[field_b] = value_b
        return 1

    async def hgetall(self, name):
        return self.hashes.get(name, {}).copy()


class GlossaryAndGrammarPipelineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.redis_client = FakeRedis()
        bot._glossary_cache = None
        bot._glossary_cache_expires_at = 0.0
        bot._glossary_notes_cache = None
        bot._glossary_notes_cache_expires_at = 0.0

    def test_parse_glossary_value_floating_gender_note(self):
        clean, note = bot.parse_glossary_value('Святой (плавающий род: святой/святая по контексту)')
        self.assertEqual(clean, 'Святой')
        self.assertEqual(note, 'плавающий род: святой/святая по контексту')

    def test_classify_glossary_term(self):
        self.assertEqual(bot.classify_glossary_term('Saint'), 'generic')
        self.assertEqual(bot.classify_glossary_term('Jade Saint'), 'specific')

    async def test_glossary_keeps_specific_priority_and_gender_notes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            glossary_file = Path(tmpdir) / 'glossary.txt'
            glossary_file.write_text(
                'Saint=Святой (плавающий род: святой/святая по контексту)\n'
                'Jade Saint=Нефритовая Святая (женский)\n'
                'Saint Kai the Dragonslayer=Святой Кай Драконоборец (мужской)\n',
                encoding='utf-8',
            )
            with patch.object(bot, 'GLOSSARY_PATH', str(glossary_file)):
                await bot.load_glossary_to_redis(force=True)

        terms = await bot.get_glossary_terms(force_refresh=True)
        notes = await bot.get_glossary_notes(force_refresh=True)

        self.assertEqual(terms['Saint'], 'Святой')
        self.assertEqual(terms['Jade Saint'], 'Нефритовая Святая')
        self.assertEqual(terms['Saint Kai the Dragonslayer'], 'Святой Кай Драконоборец')
        self.assertEqual(notes['Saint'], 'плавающий род: святой/святая по контексту')
        self.assertNotIn('(', terms['Jade Saint'])

    async def test_relevant_glossary_mentions_specific_before_generic(self):
        await bot.redis_client.hset('glossary:terms', mapping={
            'Saint Kai the Dragonslayer': 'Святой Кай Драконоборец',
            'Jade Saint': 'Нефритовая Святая',
            'Saint': 'Святой',
        })
        await bot.redis_client.hset('glossary:notes', mapping={
            'Saint': 'плавающий род: святой/святая по контексту',
            'Jade Saint': 'женский',
            'Saint Kai the Dragonslayer': 'мужской',
        })
        section = await bot.get_relevant_glossary('Jade Saint met Saint Kai the Dragonslayer. A saint watched silently.')

        self.assertIn('type=specific', section)
        self.assertIn('type=generic', section)
        self.assertLess(section.index('Saint Kai the Dragonslayer'), section.index('Saint → Святой'))

    def test_detect_grammar_suspicion_triggers_on_obvious_error(self):
        bad = 'Пламя горевшее в большом жаровне, почти погасло.'
        reasons = bot.detect_grammar_suspicion(bad)
        self.assertTrue(reasons)

    def test_should_run_grammar_fix_is_false_for_clean_text(self):
        clean = 'Пламя в большой жаровне почти погасло, но жар всё ещё держался.'
        self.assertFalse(bot.should_run_grammar_fix(clean))

    async def test_translate_text_runs_conditional_grammar_fix(self):
        first = 'Черновик'
        second = 'Пламя горевшее в большом жаровне, почти погасло.'
        fixed = 'Пламя в большой жаровне почти погасло.'

        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[first, second, fixed])) as mocked:
            translated = await bot.translate_text('Source text for translation.')

        self.assertEqual(translated, fixed)
        self.assertEqual(mocked.await_count, 3)

    async def test_translate_text_skips_grammar_fix_for_clean_chunk(self):
        first = 'Черновик'
        second = 'Пламя в большой жаровне почти погасло.'

        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[first, second])) as mocked:
            translated = await bot.translate_text('Source text for translation.')

        self.assertEqual(translated, second)
        self.assertEqual(mocked.await_count, 2)


if __name__ == '__main__':
    unittest.main()
