from __future__ import annotations

import asyncio
import logging

from aiogram import Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.types import CallbackQuery, KeyboardButton, Message, ReplyKeyboardMarkup

from app.bot.handlers.common import AdminActions, ChapterSelection, admin_status_buttons, admin_user_manage_buttons, cancel_keyboard
from app.core.state import AppContainer
from app.services.chapter_flow import safe_delete, safe_edit_text

logger = logging.getLogger(__name__)


class BotApplication:
    def __init__(self, container: AppContainer):
        self.container = container
        assert container.bot is not None
        assert container.notifications is not None
        assert container.chapter_requests is not None
        self.bot = container.bot
        self.notifications = container.notifications
        self.chapter_requests = container.chapter_requests
        self.repo = container.repo
        self.queue = container.queue
        self.dp = Dispatcher(storage=RedisStorage(container.repo.client))
        self._register()

    async def ensure_subscription(self, message: Message) -> bool:
        uid = message.from_user.id
        if await self.repo.is_user_blocked(uid):
            await message.answer("Вы заблокированы.")
            return False
        if await self.repo.is_user_subscribed(uid):
            return True
        await message.answer(
            "🔒 Этот раздел доступен только подписчикам бота.\n\nНажмите «✅ Подписаться на рассылку» в профиле.",
            reply_markup=await self.notifications.get_main_menu(uid),
        )
        return False

    def _register(self) -> None:
        self.dp.message.register(self.process_chapter_number, ChapterSelection.waiting_for_chapter)
        self.dp.message.register(self.process_admin_user_id, AdminActions.waiting_for_user_id)
        self.dp.message.register(self.button_profile, F.text == "👤 Мой профиль")
        self.dp.message.register(self.button_choose_chapter, F.text == "📖 Выбор главы")
        self.dp.message.register(self.button_bookmark, F.text == "📌 Моя закладка")
        self.dp.message.register(self.button_prev, F.text == "⬅️ Предыдущая глава")
        self.dp.message.register(self.button_next, F.text == "➡️ Следующая глава")
        self.dp.message.register(self.button_support, F.text == "🤝 Поддержать")
        self.dp.message.register(self.button_status, F.text == "📊 Статус")
        self.dp.message.register(self.button_help, F.text == "❓ Помощь")
        self.dp.message.register(self.button_profile_subscribe, F.text == "✅ Подписаться на рассылку")
        self.dp.message.register(self.button_profile_unsubscribe, F.text == "❌ Отписаться от рассылки")
        self.dp.message.register(self.button_back_to_main, F.text == "⬅️ Назад")
        self.dp.message.register(self.cmd_start, Command("start"))
        self.dp.message.register(self.handle_other_text)

        self.dp.callback_query.register(self.admin_clear_cache, F.data == "admin_clear_cache")
        self.dp.callback_query.register(self.admin_force_check, F.data == "admin_force_check")
        self.dp.callback_query.register(self.admin_show_subscribers, F.data == "admin_subscribers")
        self.dp.callback_query.register(self.admin_show_logs, F.data == "admin_logs")
        self.dp.callback_query.register(self.admin_user_manage, F.data == "admin_user_manage")
        self.dp.callback_query.register(self.admin_back_to_main, F.data == "admin_back_to_main")
        self.dp.callback_query.register(self.admin_close, F.data == "admin_close")
        self.dp.callback_query.register(self.admin_block, F.data == "admin_block")
        self.dp.callback_query.register(self.admin_unblock, F.data == "admin_unblock")
        self.dp.callback_query.register(self.admin_remove_sub, F.data == "admin_remove_sub")
        self.dp.callback_query.register(self.admin_cancel, F.data == "admin_cancel")

    async def cmd_start(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        uid = message.from_user.id
        if await self.repo.is_user_blocked(uid):
            await message.answer("Вы заблокированы. Обратитесь к администратору.")
            return
        text = "С возвращением! Используйте кнопки меню для навигации." if await self.repo.is_user_subscribed(uid) else "👋 Добро пожаловать! Подпишитесь через профиль, чтобы получить доступ к главам."
        await message.answer(text, reply_markup=await self.notifications.get_main_menu(uid))

    async def button_support(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("❤️ Спасибо, что пользуетесь ботом!\n\nBoosty: https://boosty.to/1h8u", reply_markup=await self.notifications.get_main_menu(message.from_user.id))

    async def button_profile(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        uid = message.from_user.id
        subscribed = await self.repo.is_user_subscribed(uid)
        button = "❌ Отписаться от рассылки" if subscribed else "✅ Подписаться на рассылку"
        await message.answer(
            "👤 Профиль",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📌 Моя закладка")], [KeyboardButton(text=button)], [KeyboardButton(text="⬅️ Назад")]],
                resize_keyboard=True,
                input_field_placeholder="Выберите действие...",
            ),
        )

    async def button_profile_subscribe(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        await self.repo.add_subscriber(message.from_user.id)
        await self.button_profile(message, state)

    async def button_profile_unsubscribe(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        await self.repo.remove_subscriber(message.from_user.id)
        await self.button_profile(message, state)

    async def button_back_to_main(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("↩️ Возвращаю в главное меню.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))

    async def button_choose_chapter(self, message: Message, state: FSMContext) -> None:
        if not await self.ensure_subscription(message):
            return
        await state.set_state(ChapterSelection.waiting_for_chapter)
        last_chapter = await self.repo.get_last_chapter() or "?"
        await message.answer(f"Введите номер главы (1 — {last_chapter}):", reply_markup=cancel_keyboard)

    async def button_bookmark(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        if not await self.ensure_subscription(message):
            return
        bookmark = await self.repo.get_user_bookmark(message.from_user.id)
        if not bookmark:
            await message.answer("У вас ещё нет закладки.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))
            return
        await self.chapter_requests.send_chapter_to_user(self.bot, message.from_user.id, int(bookmark), initial_message=message)

    async def button_prev(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        if not await self.ensure_subscription(message):
            return
        bookmark = await self.repo.get_user_bookmark(message.from_user.id)
        if not bookmark or int(bookmark) <= 1:
            await message.answer("Это первая глава или закладки нет.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))
            return
        status = await message.answer(f"🔍 Обработка главы {int(bookmark) - 1}...")
        await self.chapter_requests.send_chapter_to_user(self.bot, message.from_user.id, int(bookmark) - 1, status_msg=status)

    async def button_next(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        if not await self.ensure_subscription(message):
            return
        bookmark = await self.repo.get_user_bookmark(message.from_user.id)
        if not bookmark:
            await message.answer("Нет закладки.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))
            return
        status = await message.answer(f"🔍 Обработка главы {int(bookmark) + 1}...")
        await self.chapter_requests.send_chapter_to_user(self.bot, message.from_user.id, int(bookmark) + 1, status_msg=status)

    async def process_chapter_number(self, message: Message, state: FSMContext) -> None:
        if message.text == "❌ Отмена":
            await state.clear()
            await message.answer("Ввод отменён.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))
            return
        if not message.text.isdigit():
            await message.answer("Введите число или нажмите Отмена.", reply_markup=cancel_keyboard)
            return
        status = await message.answer(f"🔍 Обработка главы {int(message.text)}...")
        await self.chapter_requests.send_chapter_to_user(self.bot, message.from_user.id, int(message.text), status_msg=status)
        await state.clear()

    async def button_help(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer(
            "🤖 Доступные команды:\n👤 Мой профиль\n📌 Моя закладка\n📖 Выбор главы\n⬅️ / ➡️ Навигация\n✅ / ❌ Подписка\n🤝 Поддержать\n❓ Помощь",
            reply_markup=await self.notifications.get_main_menu(message.from_user.id),
        )

    async def button_status(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        if self.container.settings.admin_id is None or str(message.from_user.id) != self.container.settings.admin_id:
            await message.answer("Доступ запрещён.")
            return
        await message.answer("Выберите действие:", reply_markup=admin_status_buttons)

    async def admin_clear_cache(self, callback: CallbackQuery) -> None:
        await self.repo.client.delete("first_chapter")
        await safe_edit_text(callback.message, "✅ Кэш первой главы очищен.", reply_markup=admin_status_buttons)
        await callback.answer("Кэш очищен")

    async def admin_force_check(self, callback: CallbackQuery) -> None:
        await self.queue.enqueue_monitor_trigger("manual")
        await safe_edit_text(callback.message, "✅ Проверка поставлена в очередь scheduler.", reply_markup=admin_status_buttons)
        await callback.answer()

    async def admin_show_subscribers(self, callback: CallbackQuery) -> None:
        await safe_edit_text(callback.message, f"Всего подписчиков: {len(await self.repo.load_subscribers())}", reply_markup=admin_status_buttons)
        await callback.answer()

    async def admin_show_logs(self, callback: CallbackQuery) -> None:
        await safe_edit_text(callback.message, self.notifications.tail_logs(), reply_markup=admin_status_buttons)
        await callback.answer()

    async def admin_user_manage(self, callback: CallbackQuery) -> None:
        await safe_edit_text(callback.message, "Выберите действие:", reply_markup=admin_user_manage_buttons)
        await callback.answer()

    async def admin_back_to_main(self, callback: CallbackQuery) -> None:
        await safe_edit_text(callback.message, "Выберите действие:", reply_markup=admin_status_buttons)
        await callback.answer()

    async def admin_close(self, callback: CallbackQuery) -> None:
        await safe_delete(callback.message)
        await callback.answer()

    async def _admin_action_start(self, callback: CallbackQuery, state: FSMContext, action: str) -> None:
        await state.set_state(AdminActions.waiting_for_user_id)
        await state.update_data(action_type=action, request_msg_id=callback.message.message_id)
        await safe_edit_text(callback.message, "Введите ID пользователя:", reply_markup=admin_status_buttons)
        await callback.answer()

    async def admin_block(self, callback: CallbackQuery, state: FSMContext) -> None:
        await self._admin_action_start(callback, state, "block")

    async def admin_unblock(self, callback: CallbackQuery, state: FSMContext) -> None:
        await self._admin_action_start(callback, state, "unblock")

    async def admin_remove_sub(self, callback: CallbackQuery, state: FSMContext) -> None:
        await self._admin_action_start(callback, state, "remove")

    async def process_admin_user_id(self, message: Message, state: FSMContext) -> None:
        if not message.text.isdigit():
            await message.answer("Введите число или Отмена.")
            return
        data = await state.get_data()
        action = data.get("action_type")
        user_id = int(message.text)
        if action == "block":
            await self.repo.block_user(user_id)
            response = f"Пользователь {user_id} заблокирован."
        elif action == "unblock":
            await self.repo.unblock_user(user_id)
            response = f"Пользователь {user_id} разблокирован."
        else:
            removed = await self.repo.remove_subscriber(user_id)
            response = f"Пользователь {user_id} удалён из подписчиков." if removed else f"Пользователь {user_id} не найден."
        await state.clear()
        await message.answer(response, reply_markup=admin_status_buttons)

    async def admin_cancel(self, callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await safe_edit_text(callback.message, "Действие отменено.", reply_markup=admin_status_buttons)
        await callback.answer()

    async def handle_other_text(self, message: Message, state: FSMContext) -> None:
        if await state.get_state() is not None:
            await state.clear()
            await message.answer("Ввод отменён.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))
        else:
            await message.answer("Пожалуйста, используйте кнопки меню.", reply_markup=await self.notifications.get_main_menu(message.from_user.id))

    async def run(self) -> None:
        await self.dp.start_polling(self.bot)
