from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.fsm.state import State, StatesGroup


class ChapterSelection(StatesGroup):
    waiting_for_chapter = State()


class AdminActions(StatesGroup):
    waiting_for_user_id = State()


cancel_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
    input_field_placeholder="Введите номер главы или нажмите Отмена",
)

admin_status_buttons = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Очистить кэш", callback_data="admin_clear_cache")],
        [InlineKeyboardButton(text="🚀 Принудительная проверка", callback_data="admin_force_check")],
        [InlineKeyboardButton(text="📋 Список подписчиков", callback_data="admin_subscribers")],
        [InlineKeyboardButton(text="📜 Последние логи", callback_data="admin_logs")],
        [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_user_manage")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close")],
    ]
)

admin_user_manage_buttons = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Заблокировать пользователя", callback_data="admin_block")],
        [InlineKeyboardButton(text="✅ Разблокировать пользователя", callback_data="admin_unblock")],
        [InlineKeyboardButton(text="🗑 Удалить подписку", callback_data="admin_remove_sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back_to_main")],
    ]
)
