"""Слой доступа к SQLite: пользователи и подписки."""
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator, List, Optional, Tuple

from telegram import User

from app import config


@contextmanager
def db_conn() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(config.DB_FILE)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id       INTEGER,
                olympiad_id   TEXT,
                olympiad_name TEXT,
                profile       TEXT,
                UNIQUE(user_id, olympiad_id, profile)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id    INTEGER PRIMARY KEY,
                first_name TEXT,
                username   TEXT,
                joined_at  TEXT
            )
            """
        )


def ensure_user(user: Optional[User]) -> None:
    if not user:
        return
    with db_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, first_name, username, joined_at) VALUES (?,?,?,?)",
            (user.id, user.first_name or "", user.username or "", datetime.now(config.TIMEZONE).isoformat()),
        )
        conn.execute(
            "UPDATE users SET first_name=?, username=? WHERE user_id=?",
            (user.first_name or "", user.username or "", user.id),
        )


def get_all_user_ids() -> set:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users")
        a = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT DISTINCT user_id FROM subscriptions")
        b = {r[0] for r in cur.fetchall()}
    return a | b


def add_subscription(user_id: int, olympiad_id: str, olympiad_name: str, profile: str) -> None:
    with db_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO subscriptions (user_id, olympiad_id, olympiad_name, profile) VALUES (?,?,?,?)",
            (user_id, olympiad_id, olympiad_name, profile),
        )


def add_subscriptions(user_id: int, items: List[Tuple[dict, str]]) -> None:
    with db_conn() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO subscriptions (user_id, olympiad_id, olympiad_name, profile) VALUES (?,?,?,?)",
            [(user_id, o["id"], o["name"], prof) for o, prof in items],
        )


def get_user_subscriptions(user_id: int) -> List[Tuple[str, str, str]]:
    """Возвращает список (olympiad_id, olympiad_name, profile)."""
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT olympiad_id, olympiad_name, profile FROM subscriptions WHERE user_id = ?",
            (user_id,),
        )
        return cur.fetchall()


def get_user_subscription_pairs(user_id: int) -> List[Tuple[str, str]]:
    """Возвращает список (olympiad_id, profile) — используется для построения напоминаний."""
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT olympiad_id, profile FROM subscriptions WHERE user_id=?", (user_id,))
        return cur.fetchall()


def get_user_profiles(user_id: int) -> List[str]:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT profile FROM subscriptions WHERE user_id = ?", (user_id,))
        return [r[0] for r in cur.fetchall()]


def remove_subscription(user_id: int, olympiad_id: str, profile: str) -> None:
    with db_conn() as conn:
        conn.execute(
            "DELETE FROM subscriptions WHERE user_id=? AND olympiad_id=? AND profile=?",
            (user_id, olympiad_id, profile),
        )


def remove_subscriptions_by_profile(user_id: int, profile: str) -> None:
    with db_conn() as conn:
        conn.execute("DELETE FROM subscriptions WHERE user_id=? AND profile=?", (user_id, profile))


def get_all_subscriptions() -> List[Tuple[int, str, str]]:
    """Возвращает список (user_id, olympiad_id, profile) — для ежедневной рассылки напоминаний."""
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, olympiad_id, profile FROM subscriptions")
        return cur.fetchall()
