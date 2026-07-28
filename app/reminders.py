"""Построение и рассылка ежедневных напоминаний."""
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

from telegram.error import BadRequest, Forbidden
from telegram.ext import Application, ContextTypes

from app import config, database
from app.dates import parse_dates_from_cell
from app.excel_data import build_lookup, fetch_olympiads
from app.ui import chunk_messages


def due_by_policy(delta: int) -> bool:
    if config.REMIND_MODE == "WINDOW":
        return 0 <= delta <= config.REMIND_WINDOW_DAYS
    return delta in config.REMIND_DAYS_SET


def build_user_reminders(lookup: Dict[tuple, Dict], items: List[Tuple[str, str]], today: date) -> List[str]:
    lines: List[str] = ["🔔 Напоминание:"]
    for oid, prof in items:
        o = lookup.get((oid, prof))
        if not o:
            continue
        for dt, label in parse_dates_from_cell(o["date_desc"], today):
            delta = (dt - today).days
            if due_by_policy(delta):
                when = "сегодня" if delta == 0 else "завтра" if delta == 1 else f"осталось {delta} дн. {dt}."
                lines.append(f"🔔 {o['name']} ({prof}, ур. {o['level']}): {when} — {label}\n{o['link']}")
    return lines


async def send_daily(context: ContextTypes.DEFAULT_TYPE) -> None:
    today = datetime.now(config.TIMEZONE).date()
    olys = fetch_olympiads()
    lookup = build_lookup(olys)

    subs = database.get_all_subscriptions()
    by_user: Dict[int, List[Tuple[str, str]]] = {}
    for uid, oid, prof in subs:
        by_user.setdefault(uid, []).append((oid, prof))

    for uid, items in by_user.items():
        lines = build_user_reminders(lookup, items, today)
        if lines != ["🔔 Напоминание:"]:
            for ch in chunk_messages(lines):
                try:
                    await context.bot.send_message(chat_id=uid, text=ch)
                except (Forbidden, BadRequest):
                    pass
                except Exception:
                    pass
        elif config.SEND_EMPTY_INFO:
            try:
                await context.bot.send_message(chat_id=uid, text="ℹ️ Сегодня напоминаний нет.")
            except Exception:
                pass


async def fallback_daily_scheduler(app: Application, notify_tm) -> None:
    """Если JobQueue недоступен - отправляем send_daily ежедневно в указанное время."""

    class DummyCtx:
        def __init__(self, bot):
            self.bot = bot

    while True:
        now = datetime.now(config.TIMEZONE)
        target = now.replace(hour=notify_tm.hour, minute=notify_tm.minute, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            await send_daily(DummyCtx(app.bot))
        except Exception:
            logging.exception("Ошибка в fallback_daily_scheduler.send_daily")
