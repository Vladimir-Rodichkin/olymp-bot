"""Главное меню и навигация."""
from telegram import Update
from telegram.ext import ContextTypes

from app import config, database
from app.constants import UD_ACTIVE_MSG_ID, UD_CHOSEN, UD_LIST_EXTRA_IDS, UD_LIST_ROOT_ID, UD_OLYS, UD_SELECTION
from app.excel_data import build_lookup, fetch_olympiads
from app.handlers.subscribe import show_profiles
from app.keyboards import BACK_TO_MENU, delete_menu_markup, main_menu_markup
from app.ui import cleanup_list_messages, safe_edit_message
from app.dates import next_upcoming_from_cell
from datetime import datetime


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    database.ensure_user(update.effective_user)
    text = (
        "👋 Привет! Я бот-напоминалка об олимпиадах.\n\n"
        f"🔔 Напоминаю о ближайших олимпиадах каждый день в {config.DAILY_NOTIFY_TIME} по МСК.\n\n"
        "➡️ Могу напомнить уровень олимпиады, когда начинаются отборочные и заключительные этапы.\n\n"
        f"🔗 Таблица: {config.GOOGLE_SHEET_LINK}\n\n"
        "❗ Если обнаружили ошибку и/или хотите предложить новую идею для бота, пишите мне: @Vladimir_Rodichkin. \n\n"
        "Выберите действие:"
    )

    if update.callback_query:
        # Возврат в меню по кнопке — редактируем текущее сообщение
        await update.callback_query.answer()
        cur_id = update.callback_query.message.message_id
        context.user_data[UD_ACTIVE_MSG_ID] = cur_id
        await cleanup_list_messages(update, context, exclude_id=cur_id)
        await safe_edit_message(update.callback_query, text, main_menu_markup())
    else:
        # /start из чата — удаляем прежнее активное меню, шлём новое
        chat_id = update.effective_chat.id
        prev_id = context.user_data.get(UD_ACTIVE_MSG_ID)
        if prev_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=prev_id)
            except Exception:
                pass
        await cleanup_list_messages(update, context, exclude_id=None)
        m = await update.message.reply_text(text, reply_markup=main_menu_markup())
        context.user_data[UD_ACTIVE_MSG_ID] = m.message_id


async def menu_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def menu_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    cur_id = update.callback_query.message.message_id
    context.user_data[UD_ACTIVE_MSG_ID] = cur_id
    await cleanup_list_messages(update, context, exclude_id=cur_id)
    # НЕ чистим полностью user_data, чтобы не потерять служебные ключи
    context.user_data[UD_OLYS] = fetch_olympiads()
    context.user_data[UD_SELECTION] = []
    context.user_data[UD_CHOSEN] = []  # list[(o, profile)]
    await show_profiles(update, context)


async def menu_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    cur_id = update.callback_query.message.message_id
    context.user_data[UD_ACTIVE_MSG_ID] = cur_id
    await cleanup_list_messages(update, context, exclude_id=cur_id)

    olys = fetch_olympiads()
    lookup = build_lookup(olys)

    uid = update.effective_user.id
    rows = database.get_user_subscriptions(uid)

    if not rows:
        await safe_edit_message(update.callback_query, "❌ У вас нет подписок.", BACK_TO_MENU)
        context.user_data[UD_LIST_ROOT_ID] = update.callback_query.message.message_id
        context.user_data[UD_LIST_EXTRA_IDS] = []
        return

    today = datetime.now(config.TIMEZONE).date()
    blocks = []
    for oid, name, prof in rows:
        o = lookup.get((oid, prof))
        if not o:
            continue
        nxt = next_upcoming_from_cell(o["date_desc"], today)
        human = f"{nxt[0].strftime('%d.%m.%Y')} — {nxt[1]}" if nxt else (o["date_desc"] or "ПОКА РАНО")
        blocks.append(
            f"• {o['name']}\n"
            f"  Профиль: {prof}\n"
            f"  Уровень: {o['level']}\n"
            f"  Ближайшее: {human}\n"
            f"  Описание: {o['description']}\n"
            f"  Сайт: {o['link']}\n"
        )

    chunks = []
    cur_txt = "📋 Ваши подписки:\n\n"
    for blk in blocks:
        if len(cur_txt) + len(blk) > config.MAX_MESSAGE_LENGTH:
            chunks.append(cur_txt.rstrip())
            cur_txt = ""
        cur_txt += blk + "\n"
    if cur_txt.strip():
        chunks.append(cur_txt.rstrip())

    await safe_edit_message(update.callback_query, chunks[0], BACK_TO_MENU)
    context.user_data[UD_LIST_ROOT_ID] = update.callback_query.message.message_id
    extra_ids = []
    chat_id = update.effective_chat.id
    for chunk in chunks[1:]:
        m = await context.bot.send_message(chat_id=chat_id, text=chunk, reply_markup=BACK_TO_MENU)
        extra_ids.append(m.message_id)
    context.user_data[UD_LIST_EXTRA_IDS] = extra_ids


async def menu_delete_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    cur_id = update.callback_query.message.message_id
    context.user_data[UD_ACTIVE_MSG_ID] = cur_id
    await cleanup_list_messages(update, context, exclude_id=cur_id)
    await safe_edit_message(update.callback_query, "Выберите действие для удаления:", delete_menu_markup())
