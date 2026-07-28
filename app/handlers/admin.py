"""Админ-команды."""
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from app import config, database
from app.constants import UD_AWAIT_BROADCAST
from app.excel_data import build_lookup, fetch_olympiads
from app.reminders import build_user_reminders
from app.ui import chunk_messages, split_text


async def test_notify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in config.ADMIN_IDS:
        await update.message.reply_text("⛔ Только для админа.")
        return

    today = datetime.now(config.TIMEZONE).date()
    olys = fetch_olympiads()
    lookup = build_lookup(olys)

    uid = update.effective_user.id
    items = database.get_user_subscription_pairs(uid)

    lines = build_user_reminders(lookup, items, today)
    if lines and lines != ["🔔 Напоминание:"]:
        for ch in chunk_messages(lines):
            await update.message.reply_text("🧪 TEST:\n\n" + ch)
    else:
        await update.message.reply_text("🧪 TEST: Сегодня напоминаний бы не было по текущей политике.")


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in config.ADMIN_IDS:
        await update.message.reply_text("⛔ Эта команда доступна только администратору.")
        return

    database.ensure_user(update.effective_user)

    if context.args:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text(
                "Введите текст после команды или отправьте /broadcast и затем текст отдельным сообщением."
            )
            return
        await do_broadcast(update, context, text)
    else:
        context.user_data[UD_AWAIT_BROADCAST] = True
        await update.message.reply_text("✍️ Отправьте текст рассылки одним сообщением. Для отмены — /start.")


async def maybe_handle_broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.effective_user and update.effective_user.id in config.ADMIN_IDS and context.user_data.get(
        UD_AWAIT_BROADCAST
    ):
        context.user_data.pop(UD_AWAIT_BROADCAST, None)
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text("Пустой текст. Рассылка отменена.")
            return True
        await do_broadcast(update, context, text)
        return True
    return False


async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    admin_chat = update.effective_chat.id
    user_ids = sorted(database.get_all_user_ids())
    sent = failed = 0
    chunks = split_text(text)
    for uid in user_ids:
        try:
            for ch in chunks:
                await context.bot.send_message(chat_id=uid, text=ch)
            sent += 1
        except Exception:
            failed += 1
    await context.bot.send_message(
        chat_id=admin_chat,
        text=f"✅ Рассылка завершена.\nПолучателей: {len(user_ids)}\nУспешно: {sent}\nОшибок: {failed}",
    )
