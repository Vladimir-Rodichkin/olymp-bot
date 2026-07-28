"""Общие утилиты интерфейса."""
from typing import Iterable, List, Optional

from telegram import InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from app import config
from app.constants import UD_LIST_EXTRA_IDS, UD_LIST_ROOT_ID


async def safe_edit_message(cb_query, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    """Редактирует сообщение."""
    try:
        msg = cb_query.message
        if msg:
            cur_text = msg.text or msg.caption or ""
            same_text = cur_text == text
            same_markup = msg.reply_markup == reply_markup
            if same_text and same_markup:
                await cb_query.answer("Без изменений.")
                return None
        return await cb_query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            try:
                await cb_query.edit_message_reply_markup(reply_markup=reply_markup)
            except Exception:
                pass
            return None
        raise


async def cleanup_list_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, exclude_id: Optional[int] = None):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    extra_ids = context.user_data.pop(UD_LIST_EXTRA_IDS, [])
    root_id = context.user_data.pop(UD_LIST_ROOT_ID, None)
    for mid in extra_ids:
        if exclude_id is not None and mid == exclude_id:
            continue
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    if root_id and (exclude_id is None or root_id != exclude_id):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=root_id)
        except Exception:
            pass


def chunk_messages(lines: Iterable[str], max_len: int = config.MAX_MESSAGE_LENGTH) -> List[str]:
    chunks, cur = [], ""
    for ln in lines:
        if len(cur) + len(ln) + 2 > max_len:
            if cur:
                chunks.append(cur.strip())
                cur = ""
        cur += ln + "\n\n"
    if cur.strip():
        chunks.append(cur.strip())
    return chunks


def split_text(text: str, max_len: int = config.MAX_MESSAGE_LENGTH) -> List[str]:
    if len(text) <= max_len:
        return [text]
    chunks, cur = [], ""
    for line in text.splitlines(keepends=True):
        if len(cur) + len(line) > max_len:
            chunks.append(cur)
            cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    return chunks
