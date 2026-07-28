"""Обработка неизвестных команд."""
import logging

from telegram import Update
from telegram.ext import ContextTypes

from app.handlers.admin import maybe_handle_broadcast_text


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Неизвестная команда. Напишите /start.")


async def catch_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maybe_handle_broadcast_text(update, context):
        return
    await update.message.reply_text("Напишите /start, чтобы начать.")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.error("Ошибка при обработке обновления:", exc_info=context.error)
