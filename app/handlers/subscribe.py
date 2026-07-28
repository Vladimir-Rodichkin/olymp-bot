"""Сценарий подписки."""
from telegram import Update
from telegram.ext import ContextTypes

from app import config, database
from app.constants import (
    UD_ACTIVE_MSG_ID,
    UD_CHOSEN,
    UD_CURRENT_PROFILE,
    UD_MANUAL_LIST,
    UD_MANUAL_SEL,
    UD_OLYS,
    UD_PROFILE_LIST,
    UD_PROFILES,
    UD_SELECTION,
)
from app.excel_data import filter_by_profile, get_profiles
from app.keyboards import BACK_TO_MENU, manual_olympiads_markup, profile_option_markup, profiles_markup
from app.ui import safe_edit_message


async def show_profiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    olys = context.user_data[UD_OLYS]
    profiles = get_profiles(olys)
    context.user_data[UD_PROFILES] = profiles
    markup = profiles_markup(profiles, context.user_data[UD_SELECTION])
    if update.callback_query:
        context.user_data[UD_ACTIVE_MSG_ID] = update.callback_query.message.message_id
        await safe_edit_message(update.callback_query, "Выберите профиль(и):", markup)
    else:
        m = await update.message.reply_text("Выберите профиль(и):", reply_markup=markup)
        context.user_data[UD_ACTIVE_MSG_ID] = m.message_id


async def toggle_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx = int(update.callback_query.data.split("|", 1)[1])
    prof = context.user_data[UD_PROFILES][idx]
    sel = context.user_data[UD_SELECTION]
    if prof in sel:
        sel.remove(prof)
    else:
        sel.append(prof)
    await show_profiles(update, context)


async def profiles_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    if not context.user_data[UD_SELECTION]:
        await update.callback_query.answer("Нужно выбрать хотя бы один.", show_alert=True)
        return
    context.user_data[UD_PROFILE_LIST] = context.user_data[UD_SELECTION][:]
    context.user_data[UD_CURRENT_PROFILE] = 0
    await ask_profile_option(update, context)


async def ask_profile_option(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prof = context.user_data[UD_PROFILE_LIST][context.user_data[UD_CURRENT_PROFILE]]
    await safe_edit_message(
        update.callback_query,
        f"Профиль: {prof}. Учитывать все олимпиады этого профиля?",
        profile_option_markup(),
    )


async def include_all_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    prof = context.user_data[UD_PROFILE_LIST][context.user_data[UD_CURRENT_PROFILE]]
    for o in filter_by_profile(context.user_data[UD_OLYS], prof):
        context.user_data[UD_CHOSEN].append((o, prof))
    await proceed_next(update, context)


async def include_manual_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    prof = context.user_data[UD_PROFILE_LIST][context.user_data[UD_CURRENT_PROFILE]]
    context.user_data[UD_MANUAL_LIST] = filter_by_profile(context.user_data[UD_OLYS], prof)
    context.user_data[UD_MANUAL_SEL] = []
    await show_manual(update, context)


async def show_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    olys = context.user_data[UD_MANUAL_LIST]
    markup = manual_olympiads_markup(olys, context.user_data[UD_MANUAL_SEL])
    await safe_edit_message(update.callback_query, "Выберите олимпиады вручную:", markup)


async def toggle_oly_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx = int(update.callback_query.data.split("|", 1)[1])
    sel = context.user_data[UD_MANUAL_SEL]
    if idx in sel:
        sel.remove(idx)
    else:
        sel.append(idx)
    await show_manual(update, context)


async def manual_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    prof = context.user_data[UD_PROFILE_LIST][context.user_data[UD_CURRENT_PROFILE]]
    manual_list = context.user_data[UD_MANUAL_LIST]
    for i in context.user_data[UD_MANUAL_SEL]:
        context.user_data[UD_CHOSEN].append((manual_list[i], prof))
    await proceed_next(update, context)


async def proceed_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data[UD_CURRENT_PROFILE] += 1
    if context.user_data[UD_CURRENT_PROFILE] < len(context.user_data[UD_PROFILE_LIST]):
        return await ask_profile_option(update, context)

    uid = update.effective_user.id
    database.add_subscriptions(uid, context.user_data[UD_CHOSEN])

    await safe_edit_message(
        update.callback_query,
        f"✅ Подписки сохранены. Напоминания — в {config.DAILY_NOTIFY_TIME} по МСК.",
        BACK_TO_MENU,
    )
