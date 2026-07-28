"""
Конфигурация бота.
"""
import os
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on", "да"}


def _get_int_set(name: str, default: set[int]) -> set[int]:
    val = os.getenv(name)
    if not val:
        return default
    return {int(x) for x in val.split(",") if x.strip()}


def _get_admin_ids(name: str) -> set[int]:
    val = os.getenv(name, "")
    return {int(x) for x in val.split(",") if x.strip().isdigit()}


def _resolve_path(value: str) -> str:
    p = Path(value)
    return str(p if p.is_absolute() else BASE_DIR / p)


# --- Telegram ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_IDS = _get_admin_ids("ADMIN_IDS")

# --- Источники данных ---
EXCEL_FILE = _resolve_path(os.getenv("EXCEL_FILE", "data/Расписание олимпиад.xlsx"))
DB_FILE = _resolve_path(os.getenv("DB_FILE", "subscriptions.db"))

# --- Время и напоминания ---
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
DAILY_NOTIFY_TIME = os.getenv("DAILY_NOTIFY_TIME", "12:00")


def _parse_daily_time(s: str) -> time:
    try:
        hh, mm = s.strip().split(":")
        return time(int(hh), int(mm), tzinfo=TIMEZONE)
    except Exception:
        return time(12, 0, tzinfo=TIMEZONE)


NOTIFY_TIME = _parse_daily_time(DAILY_NOTIFY_TIME)

# Режим напоминаний:
#   "WINDOW"     — каждый день, если 0 <= delta <= REMIND_WINDOW_DAYS
#   "MILESTONES" — только если delta ∈ REMIND_DAYS_SET
REMIND_MODE = os.getenv("REMIND_MODE", "MILESTONES").upper()
REMIND_WINDOW_DAYS = int(os.getenv("REMIND_WINDOW_DAYS", "30"))
REMIND_DAYS_SET = _get_int_set(
    "REMIND_DAYS_SET", {60, 30, 21, 14, 10, 7, 5, 3, 2, 1, 0}
)

# Сообщать ли "Сегодня напоминаний нет", когда ничего не подошло
SEND_EMPTY_INFO = _get_bool("SEND_EMPTY_INFO", False)

# --- Разное ---
GOOGLE_SHEET_LINK = os.getenv(
    "GOOGLE_SHEET_LINK",
    "https://docs.google.com/spreadsheets/d/1yZumxqRXi7eD1XjAAxU5LCjBzPKcNnDTLiu43CxGyjc/",
)

MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "4000"))
