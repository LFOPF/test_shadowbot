import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import json

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

    def test_build_glossary_constraints_extracts_entity_fields(self):
        constraints = bot.build_glossary_constraints(
            {"Saint Mordret": "Святой Мордрет", "Saint": "Святой"},
            {"Saint Mordret": "мужской, locked", "Saint": "плавающий род"},
        )
        self.assertEqual(constraints["entries"][0]["source"], "Saint Mordret")
        self.assertEqual(constraints["entries"][0]["entity_gender"], "male")
        self.assertTrue(constraints["entries"][0]["locked_entity"])

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

    def test_looks_like_english_leak(self):
        self.assertTrue(bot.looks_like_english_leak('You would smile blissfully at him too, and never hold it against him.'))
        self.assertFalse(bot.looks_like_english_leak('Ты бы блаженно улыбался ему и не держал бы на него зла.'))

    async def test_translate_text_runs_conditional_grammar_fix(self):
        entity_map = '{"entities":[],"quoted_blocks":[],"pronoun_bindings":[]}'
        first = 'Черновик'
        second = 'Пламя горевшее в большом жаровне, почти погасло.'
        fixed = 'Пламя в большой жаровне почти погасло.'

        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'get_relevant_glossary_constraints', AsyncMock(return_value={"entries": []})), \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[entity_map, first, second, fixed])) as mocked:
            translated = await bot.translate_text('Source text for translation.')

        self.assertEqual(translated, fixed)
        self.assertEqual(mocked.await_count, 4)

    async def test_translate_text_skips_grammar_fix_for_clean_chunk(self):
        entity_map = '{"entities":[],"quoted_blocks":[],"pronoun_bindings":[]}'
        first = 'Черновик'
        second = 'Пламя в большой жаровне почти погасло.'

        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'get_relevant_glossary_constraints', AsyncMock(return_value={"entries": []})), \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[entity_map, first, second])) as mocked:
            translated = await bot.translate_text('Source text for translation.')

        self.assertEqual(translated, second)
        self.assertEqual(mocked.await_count, 3)

    def test_detect_reference_conflict_on_mordret_case(self):
        fixtures = Path(__file__).parent / 'fixtures'
        source = (fixtures / 'mordret_regression_source.txt').read_text(encoding='utf-8')
        bad_translation = (fixtures / 'mordret_regression_bad_ru.txt').read_text(encoding='utf-8')
        entity_map = {
            "entities": [
                {
                    "canonical_name": "Saint Mordret",
                    "grammatical_gender": "male",
                    "aliases": ["Мордрет", "Святой Мордрет"],
                    "mentions": ["him", "he"],
                }
            ],
            "quoted_blocks": [{"quote_excerpt": "You'd smile blissfully at him too", "addressee_gender": "male"}],
            "pronoun_bindings": [{"pronoun": "you", "entity": "Saint Mordret", "confidence": "high"}],
        }
        conflicts = bot.detect_reference_conflicts(
            source_text=source,
            translated_text=bad_translation,
            entity_map=entity_map,
        )
        self.assertIn('second_person_feminine_in_male_addressee_context', conflicts)

    def test_mordret_expected_behavior_fixture(self):
        fixtures = Path(__file__).parent / 'fixtures'
        expected = (fixtures / 'mordret_regression_expected_ru.txt').read_text(encoding='utf-8')
        self.assertIn('улыбался', expected)
        self.assertIn('не смог', expected)
        self.assertIn('Святой Мордрет', expected)
        self.assertNotIn('улыбалась', expected)
        self.assertNotIn('не смогла', expected)

    async def test_translate_text_triggers_reference_fix_even_without_grammar_suspicion(self):
        source = 'You would smile blissfully at him too.'
        entity_map = {
            "entities": [{"canonical_name": "Saint Mordret", "grammatical_gender": "male", "aliases": ["Мордрет"]}],
            "quoted_blocks": [{"quote_excerpt": source, "addressee_gender": "male"}],
            "pronoun_bindings": [{"pronoun": "you", "entity": "Saint Mordret", "confidence": "high"}],
        }
        first = 'Черновик'
        second = 'Ты бы блаженно улыбалась ему.'
        fixed = 'Ты бы блаженно улыбался ему.'
        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'get_relevant_glossary_constraints', AsyncMock(return_value={"entries": []})), \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())), \
             patch.object(bot, 'should_run_grammar_fix', return_value=False), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[json.dumps(entity_map, ensure_ascii=False), first, second, fixed])) as mocked:
            translated = await bot.translate_text(source)

        self.assertEqual(translated, fixed)
        self.assertEqual(mocked.await_count, 4)

    async def test_translate_text_recovers_if_second_pass_is_english(self):
        source = 'Source text for translation.'
        entity_map = '{"entities":[],"quoted_blocks":[],"pronoun_bindings":[]}'
        first = 'Русский черновик перевода с достаточной длиной для проверки.'
        second_leak = 'This paragraph mistakenly stayed in English and should be forced back into Russian prose.'
        recovered = 'Этот абзац исправлен и полностью возвращён на русский язык.'
        with patch.object(bot, 'SYSTEM_PROMPT', 'sys'), \
             patch.object(bot, 'USER_PROMPT_TEMPLATE', '{text}'), \
             patch.object(bot, 'get_relevant_glossary', AsyncMock(return_value='')), \
             patch.object(bot, 'get_relevant_glossary_constraints', AsyncMock(return_value={"entries": []})), \
             patch.object(bot, 'get_http_session', AsyncMock(return_value=object())), \
             patch.object(bot, 'should_run_grammar_fix', return_value=False), \
             patch.object(bot, 'request_translation_completion', AsyncMock(side_effect=[entity_map, first, second_leak, recovered])) as mocked:
            translated = await bot.translate_text(source)

        self.assertEqual(translated, recovered)
        self.assertEqual(mocked.await_count, 4)


if __name__ == '__main__':
    unittest.main()
