"""Сценарий удаления подписок."""
from telegram import Update
from telegram.ext import ContextTypes

from app import database
from app.constants import UD_DEL_PROFILES, UD_REMOVE
from app.keyboards import BACK_TO_MENU, delete_one_markup, delete_profile_markup
from app.ui import safe_edit_message


async def del_one_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.effective_user.id
    rows = database.get_user_subscriptions(uid)
    if not rows:
        await safe_edit_message(update.callback_query, "❌ Нет подписок для удаления.", BACK_TO_MENU)
        return
    context.user_data[UD_REMOVE] = rows
    await safe_edit_message(update.callback_query, "Что удалить?", delete_one_markup(rows))


async def del_one_oly_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx = int(update.callback_query.data.split("|", 1)[1])
    rows = context.user_data.get(UD_REMOVE, [])
    if idx < 0 or idx >= len(rows):
        await safe_edit_message(update.callback_query, "❌ Неверный выбор.", BACK_TO_MENU)
        return
    oid, name, prof = rows[idx]
    database.remove_subscription(update.effective_user.id, oid, prof)
    await safe_edit_message(update.callback_query, f"✅ Удалено: {name} ({prof})", BACK_TO_MENU)


async def del_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.effective_user.id
    profiles = database.get_user_profiles(uid)
    if not profiles:
        await safe_edit_message(update.callback_query, "❌ Нет подписок.", BACK_TO_MENU)
        return
    context.user_data[UD_DEL_PROFILES] = profiles
    await safe_edit_message(
        update.callback_query, "Выберите профиль для удаления всех подписок:", delete_profile_markup(profiles)
    )


async def del_profile_sel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx = int(update.callback_query.data.split("|", 1)[1])
    profiles = context.user_data.get(UD_DEL_PROFILES, [])
    if idx < 0 or idx >= len(profiles):
        await safe_edit_message(update.callback_query, "❌ Неверный выбор.", BACK_TO_MENU)
        return
    prof = profiles[idx]
    database.remove_subscriptions_by_profile(update.effective_user.id, prof)
    await safe_edit_message(update.callback_query, f"✅ Удалены все подписки профиля «{prof}».", BACK_TO_MENU)
