
"""
Олимпиадный бот с надёжными напоминаниями.

Главное:
- Бот каждый раз читает Excel заново, НИЧЕГО из дат не кеширует в БД.
- Парсит ячейку с датами «на всё»: несколько дат через ; , / или переносы строк,
  диапазоны (12.09–14.09.2025, 12.09-14.09, «с 12.09 по 14.09»), даты без года (год подбирается автоматически),
  текст ярлыка события после «/» (например: 16.02.2025/финал).
- REMIND_DAYS определяет, за сколько дней шлём напоминания (0=сегодня, 1=завтра и т.д.).
- Время отправки с минутами: DAILY_NOTIFY_TIME = "HH:MM".
- «Мои подписки» разбивается на чанки, в каждом — кнопка «Главное меню».
- Команда /broadcast для админа отправляет сообщение всем активировавшим бот.
- админ-рассылка /broadcast и тест /testnotify
"""
import logging
import sqlite3
import re
from datetime import datetime, date, time
from zoneinfo import ZoneInfo
from typing import Iterable, List, Tuple, Optional, Dict

import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.error import Conflict, Forbidden, BadRequest
import asyncio
from datetime import datetime, date, time, timedelta

#Конфигурация
TELEGRAM_TOKEN    = ''
EXCEL_FILE        = "Список олимпиад.xlsx"
DB_FILE           = 'subscriptions.db'
TIMEZONE            = ZoneInfo('Europe/Moscow')
DAILY_NOTIFY_TIME   = "12:00" 
# Режим напоминаний (для тестов, сейчас она не работает):
#   "WINDOW"   — каждый день, если 0 <= delta <= REMIND_WINDOW_DAYS
#   "MILESTONES" — только если delta ∈ REMIND_DAYS_SET
REMIND_MODE         = "MILESTONES"
REMIND_WINDOW_DAYS  = 30
REMIND_DAYS_SET     = {60, 30, 21, 14, 10, 7, 5, 3, 2, 1, 0}

# Сообщать ли "Сегодня напоминаний нет", когда ничего не подошло
SEND_EMPTY_INFO     = False

# Ссылка на Гугл-таблицу
GOOGLE_SHEET_LINK   = (
    'https://docs.google.com/spreadsheets/'
    'd/1yZumxqRXi7eD1XjAAxU5LCjBzPKcNnDTLiu43CxGyjc/'
)

MAX_MESSAGE_LENGTH  = 4000  # безопасная длина (поскольку в тг есть ограничения на количество текста в одном сообщении)

# Админы (для /broadcast и /testnotify)
ADMIN_IDS           = {}

# Ключи
UD_LIST_ROOT_ID     = 'list_root_msg_id'
UD_LIST_EXTRA_IDS   = 'list_message_ids'
UD_AWAIT_BROADCAST  = 'await_broadcast_text'
UD_ACTIVE_MSG_ID = 'active_msg_id'

#ВСПОМОГАТЕЛЬНОЕ
def detect_col(df: pd.DataFrame, keywords) -> Optional[str]:
    for col in df.columns:
        low = col.lower()
        if any(kw.lower() in low for kw in keywords):
            return col
    return None

def parse_daily_time(s: str) -> time:
    try:
        hh, mm = s.strip().split(":")
        return time(int(hh), int(mm), tzinfo=TIMEZONE)
    except Exception:
        return time(12, 0, tzinfo=TIMEZONE)

def year_for_day_month(d: int, m: int, today: date) -> int:
    try:
        candidate = date(today.year, m, d)
    except ValueError:
        return today.year
    return candidate.year if candidate >= today else today.year + 1

DATE_RE = re.compile(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?')
RANGE_SEP_RE = re.compile(r'\s*[–—-]\s*')

def parse_dates_from_cell(cell: str, today: date) -> List[Tuple[date, str]]:
    if not cell:
        return []
    text = str(cell).strip()
    if not text or text.upper().startswith("ПОКА"):
        return []

    chunks = re.split(r'[\n;]+', text.replace('\r', '\n'))
    refined = []
    for ch in chunks:
        parts = [p.strip() for p in re.split(r',(?!\s*\d{1,2}\.\d{1,2})', ch) if p.strip()]
        refined.extend(parts)

    out: List[Tuple[date, str]] = []
    for entry in refined:
        entry = entry.strip()
        if not entry:
            continue

        # отделяем ярлык после '/'
        if '/' in entry:
            left, label = entry.split('/', 1)
            label = label.strip() or 'событие'
        else:
            left, label = entry, 'событие'

        # "с 12.09 по 14.09"
        if 'с ' in left.lower() and ' по ' in left.lower():
            m = DATE_RE.findall(left)
            if m:
                d1, m1, y1 = m[0]
                dd, mm = int(d1), int(m1)
                yy = int(y1) + (2000 if y1 and int(y1) < 100 else 0) if y1 else year_for_day_month(dd, mm, today)
                try:
                    dt = date(yy, mm, dd)
                    if dt >= today:
                        out.append((dt, label))
                except ValueError:
                    pass
            continue

        # диапазон "12.11–14.11(.2025)"
        if RANGE_SEP_RE.search(left):
            sides = RANGE_SEP_RE.split(left)
            if sides:
                m = DATE_RE.search(sides[0])
                if m:
                    d1, m1, y1 = m.groups()
                    dd, mm = int(d1), int(m1)
                    yy = int(y1) + (2000 if y1 and int(y1) < 100 else 0) if y1 else year_for_day_month(dd, mm, today)
                    try:
                        dt = date(yy, mm, dd)
                        if dt >= today:
                            out.append((dt, label or 'начало'))
                    except ValueError:
                        pass
            continue

        # одиночная дата
        m = DATE_RE.search(left) or DATE_RE.search(entry)
        if m:
            d, m_, y = m.groups()
            dd, mm = int(d), int(m_)
            yy = int(y) + (2000 if y and int(y) < 100 else 0) if y else year_for_day_month(dd, mm, today)
            try:
                dt = date(yy, mm, dd)
                if dt >= today:
                    out.append((dt, label))
            except ValueError:
                pass

    uniq = {}
    for dt, lab in out:
        if dt in uniq and lab not in uniq[dt]:
            uniq[dt] = uniq[dt] + f"; {lab}"
        else:
            uniq.setdefault(dt, lab)
    return [(dt, uniq[dt]) for dt in sorted(uniq.keys())]

def next_upcoming_from_cell(cell: str, today: date) -> Optional[Tuple[date, str]]:
    items = parse_dates_from_cell(cell, today)
    return items[0] if items else None

# ===================== БАЗА ДАННЫХ =====================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id       INTEGER,
            olympiad_id   TEXT,
            olympiad_name TEXT,
            profile       TEXT,
            UNIQUE(user_id, olympiad_id, profile)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            first_name TEXT,
            username   TEXT,
            joined_at  TEXT
        )
    ''')
    conn.commit()
    conn.close()

def ensure_user_in_db(update: Update):
    u = update.effective_user
    if not u:
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO users (user_id, first_name, username, joined_at) VALUES (?,?,?,?)",
        (u.id, u.first_name or '', u.username or '', datetime.now(TIMEZONE).isoformat())
    )
    cur.execute(
        "UPDATE users SET first_name=?, username=? WHERE user_id=?",
        (u.first_name or '', u.username or '', u.id)
    )
    conn.commit()
    conn.close()

def get_all_user_ids() -> set[int]:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    a = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT user_id FROM subscriptions")
    b = {r[0] for r in cur.fetchall()}
    conn.close()
    return a | b

# ===================== ЧТЕНИЕ EXCEL =====================
def fetch_olympiads():
    df = pd.read_excel(EXCEL_FILE, sheet_name=0)

    id_col    = detect_col(df, ['название', 'олимпиад'])
    prof_col  = detect_col(df, ['профиль'])
    date_col  = detect_col(df, ['дат'])
    lvl_col   = detect_col(df, ['уровень'])
    desc_col  = detect_col(df, ['описан'])
    link_col  = detect_col(df, ['ссыл'])

    if not id_col or not prof_col or not date_col:
        raise RuntimeError("Не найдены обязательные столбцы в Excel (название/профиль/дата).")

    olympiads = []
    for _, row in df.iterrows():
        oid = str(row[id_col]).strip()
        raw_profiles = str(row.get(prof_col, '') or '')
        profiles = [p.strip() for p in re.split(r'[;,/]', raw_profiles) if p.strip()] or ['—']
        olympiads.append({
            'id':          oid,
            'profiles':    profiles,
            'name':        oid,
            'date_desc':   str(row.get(date_col, '') or '').strip(),
            'level':       str(row.get(lvl_col, '') or '—').strip(),
            'description': str(row.get(desc_col, '') or '—').strip(),
            'link':        str(row.get(link_col, '') or '—').strip(),
        })
    return olympiads

# ===================== UI УТИЛИТЫ =====================
def get_profiles(olys):
    s = set()
    for o in olys: s.update(o['profiles'])
    return sorted(s)

def filter_by_profile(olys, profile):
    return [o for o in olys if profile in o['profiles']]

async def cleanup_list_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, exclude_id: int | None = None):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id: return
    extra_ids = context.user_data.pop(UD_LIST_EXTRA_IDS, [])
    root_id   = context.user_data.pop(UD_LIST_ROOT_ID, None)
    for mid in extra_ids:
        if exclude_id is not None and mid == exclude_id: continue
        try: await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception: pass
    if root_id and (exclude_id is None or root_id != exclude_id):
        try: await context.bot.delete_message(chat_id=chat_id, message_id=root_id)
        except Exception: pass

def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Выбрать олимпиаду", callback_data="menu_select")],
        [InlineKeyboardButton("📋 Мои подписки",      callback_data="menu_list")],
        [InlineKeyboardButton("🗑️ Удалить подписку",  callback_data="menu_delete")],
    ])

async def safe_edit_message(cb_query, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    """
    Безопасное редактирование: если контент и разметка не меняются — игнорируем BadRequest: Message is not modified.
    """
    try:
        msg = cb_query.message
        if msg:
            cur_text = msg.text or msg.caption or ""
            same_text = (cur_text == text)
            same_markup = (msg.reply_markup == reply_markup)
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

# ===================== ХЕНДЛЕРЫ МЕНЮ =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_in_db(update)
    text = (
        "👋 Привет! Я бот-напоминалка об олимпиадах.\n\n"
        f"🔔 Напоминаю о ближайших олимпиадах каждый день в {DAILY_NOTIFY_TIME} по МСК.\n\n"
        "➡️ Могу напомнить уровень олимпиады, когда начинаются отборочные и заключительные этапы.\n\n"
        f"🔗 Таблица: {GOOGLE_SHEET_LINK}\n\n"
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
    context.user_data['olys']      = fetch_olympiads()
    context.user_data['selection'] = []
    context.user_data['chosen']    = []  # list[(o, profile)]
    await show_profiles(update, context)

async def menu_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    cur_id = update.callback_query.message.message_id
    context.user_data[UD_ACTIVE_MSG_ID] = cur_id
    await cleanup_list_messages(update, context, exclude_id=cur_id)

    olys   = fetch_olympiads()
    lookup = {(o['id'], p): o for o in olys for p in o['profiles']}

    uid = update.effective_user.id
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("SELECT olympiad_id, olympiad_name, profile FROM subscriptions WHERE user_id = ?", (uid,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await safe_edit_message(
            update.callback_query,
            "❌ У вас нет подписок.",
            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]])
        )
        context.user_data[UD_LIST_ROOT_ID]   = update.callback_query.message.message_id
        context.user_data[UD_LIST_EXTRA_IDS] = []
        return

    today = datetime.now(TIMEZONE).date()
    blocks = []
    for oid, name, prof in rows:
        o = lookup.get((oid, prof))
        if not o: continue
        nxt = next_upcoming_from_cell(o['date_desc'], today)
        human = (f"{nxt[0].strftime('%d.%m.%Y')} — {nxt[1]}" if nxt else (o['date_desc'] or "ПОКА РАНО"))
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
        if len(cur_txt) + len(blk) > MAX_MESSAGE_LENGTH:
            chunks.append(cur_txt.rstrip()); cur_txt = ""
        cur_txt += blk + "\n"
    if cur_txt.strip(): chunks.append(cur_txt.rstrip())

    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]])
    await safe_edit_message(update.callback_query, chunks[0], back_kb)
    context.user_data[UD_LIST_ROOT_ID] = update.callback_query.message.message_id
    extra_ids = []
    chat_id = update.effective_chat.id
    for chunk in chunks[1:]:
        m = await context.bot.send_message(chat_id=chat_id, text=chunk, reply_markup=back_kb)
        extra_ids.append(m.message_id)
    context.user_data[UD_LIST_EXTRA_IDS] = extra_ids

async def menu_delete_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    cur_id = update.callback_query.message.message_id
    context.user_data[UD_ACTIVE_MSG_ID] = cur_id
    await cleanup_list_messages(update, context, exclude_id=cur_id)
    kb = [
        [InlineKeyboardButton("Удалить конкретную", callback_data="del_one")],
        [InlineKeyboardButton("Удалить по профилю", callback_data="del_profile")],
        [InlineKeyboardButton("↩️ Главное меню",    callback_data="menu_back")],
    ]
    await safe_edit_message(update.callback_query, "Выберите действие для удаления:", InlineKeyboardMarkup(kb))

# ===================== УДАЛЕНИЕ =====================
async def del_one_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.effective_user.id
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT olympiad_id, olympiad_name, profile FROM subscriptions WHERE user_id = ?", (uid,))
    rows = cur.fetchall(); conn.close()
    if not rows:
        await safe_edit_message(update.callback_query, "❌ Нет подписок для удаления.",
                                InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))
        return
    context.user_data['remove'] = rows
    kb = [[InlineKeyboardButton(f"{name} ({prof})", callback_data=f"del_one_oly|{i}")] for i, (_, name, prof) in enumerate(rows)]
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    await safe_edit_message(update.callback_query, "Что удалить?", InlineKeyboardMarkup(kb))

async def del_one_oly_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx  = int(update.callback_query.data.split("|",1)[1])
    rows = context.user_data.get('remove', [])
    if idx < 0 or idx >= len(rows):
        await safe_edit_message(update.callback_query, "❌ Неверный выбор.",
                                InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))
        return
    oid, name, prof = rows[idx]
    conn = sqlite3.connect(DB_FILE)
    conn.execute("DELETE FROM subscriptions WHERE user_id=? AND olympiad_id=? AND profile=?", (update.effective_user.id, oid, prof))
    conn.commit(); conn.close()
    await safe_edit_message(update.callback_query, f"✅ Удалено: {name} ({prof})",
                            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))

async def del_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.effective_user.id
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT DISTINCT profile FROM subscriptions WHERE user_id = ?", (uid,))
    profiles = [r[0] for r in cur.fetchall()]; conn.close()
    if not profiles:
        await safe_edit_message(update.callback_query, "❌ Нет подписок.",
                                InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))
        return
    context.user_data['del_profiles'] = profiles
    kb = [[InlineKeyboardButton(prof, callback_data=f"del_profile_sel|{i}")] for i, prof in enumerate(profiles)]
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    await safe_edit_message(update.callback_query, "Выберите профиль для удаления всех подписок:", InlineKeyboardMarkup(kb))

async def del_profile_sel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx = int(update.callback_query.data.split("|",1)[1])
    profiles = context.user_data.get('del_profiles', [])
    if idx < 0 or idx >= len(profiles):
        await safe_edit_message(update.callback_query, "❌ Неверный выбор.",
                                InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))
        return
    prof = profiles[idx]
    conn = sqlite3.connect(DB_FILE); conn.execute("DELETE FROM subscriptions WHERE user_id=? AND profile=?", (update.effective_user.id, prof)); conn.commit(); conn.close()
    await safe_edit_message(update.callback_query, f"✅ Удалены все подписки профиля «{prof}».",
                            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))

# ===================== ПОДПИСКА =====================
async def show_profiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    olys = context.user_data['olys']
    profiles = get_profiles(olys)
    context.user_data['profiles'] = profiles
    kb = [[InlineKeyboardButton(f"{'✅' if p in context.user_data['selection'] else '☐'} {p}", callback_data=f"toggle_profile|{i}")]
          for i, p in enumerate(profiles)]
    kb.append([InlineKeyboardButton("Готово", callback_data="profiles_done")])
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    markup = InlineKeyboardMarkup(kb)
    if update.callback_query:
        context.user_data[UD_ACTIVE_MSG_ID] = update.callback_query.message.message_id
        await safe_edit_message(update.callback_query, "Выберите профиль(и):", markup)
    else:
        m = await update.message.reply_text("Выберите профиль(и):", reply_markup=markup)
        context.user_data[UD_ACTIVE_MSG_ID] = m.message_id

async def toggle_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx  = int(update.callback_query.data.split("|",1)[1])
    prof = context.user_data['profiles'][idx]
    sel  = context.user_data['selection']
    if prof in sel: sel.remove(prof)
    else:           sel.append(prof)
    await show_profiles(update, context)

async def profiles_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    if not context.user_data['selection']:
        await update.callback_query.answer("Нужно выбрать хотя бы один.", show_alert=True); return
    context.user_data['profile_list']    = context.user_data['selection'][:]
    context.user_data['current_profile'] = 0
    await ask_profile_option(update, context)

async def ask_profile_option(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prof = context.user_data['profile_list'][context.user_data['current_profile']]
    kb   = [
        [InlineKeyboardButton("Учитывать все",    callback_data="include_all")],
        [InlineKeyboardButton("Выбрать вручную", callback_data="include_manual")],
        [InlineKeyboardButton("↩️ Главное меню",  callback_data="menu_back")],
    ]
    await safe_edit_message(update.callback_query, f"Профиль: {prof}. Учитывать все олимпиады этого профиля?", InlineKeyboardMarkup(kb))

async def include_all_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    prof = context.user_data['profile_list'][context.user_data['current_profile']]
    for o in filter_by_profile(context.user_data['olys'], prof):
        context.user_data['chosen'].append((o, prof))
    await proceed_next(update, context)

async def include_manual_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    prof = context.user_data['profile_list'][context.user_data['current_profile']]
    context.user_data['manual_list'] = filter_by_profile(context.user_data['olys'], prof)
    context.user_data['manual_sel']  = []
    await show_manual(update, context)

async def show_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    olys = context.user_data['manual_list']
    kb = [[InlineKeyboardButton(f"{'✅' if i in context.user_data['manual_sel'] else '☐'} {o['name']}", callback_data=f"toggle_oly|{i}")]
          for i, o in enumerate(olys)]
    kb.append([InlineKeyboardButton("Готово", callback_data="manual_done")])
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    await safe_edit_message(update.callback_query, "Выберите олимпиады вручную:", InlineKeyboardMarkup(kb))

async def toggle_oly_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    idx = int(update.callback_query.data.split("|",1)[1])
    sel = context.user_data['manual_sel']
    if idx in sel: sel.remove(idx)
    else:          sel.append(idx)
    await show_manual(update, context)

async def manual_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    prof        = context.user_data['profile_list'][context.user_data['current_profile']]
    manual_list = context.user_data['manual_list']
    for i in context.user_data['manual_sel']:
        context.user_data['chosen'].append((manual_list[i], prof))
    await proceed_next(update, context)

async def proceed_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data['current_profile'] += 1
    if context.user_data['current_profile'] < len(context.user_data['profile_list']):
        return await ask_profile_option(update, context)

    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    uid  = update.effective_user.id
    for o, prof in context.user_data['chosen']:
        cur.execute("INSERT OR IGNORE INTO subscriptions (user_id, olympiad_id, olympiad_name, profile) VALUES (?,?,?,?)",
                    (uid, o['id'], o['name'], prof))
    conn.commit(); conn.close()
    await safe_edit_message(update.callback_query,
                            f"✅ Подписки сохранены. Напоминания — в {DAILY_NOTIFY_TIME} по МСК.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]]))

# ===================== НАПОМИНАНИЯ =====================
def chunk_messages(lines: Iterable[str], max_len=MAX_MESSAGE_LENGTH) -> List[str]:
    chunks, cur = [], ""
    for ln in lines:
        if len(cur) + len(ln) + 2 > max_len:
            if cur: chunks.append(cur.strip()); cur = ""
        cur += ln + "\n\n"
    if cur.strip(): chunks.append(cur.strip())
    return chunks

def due_by_policy(delta: int) -> bool:
    if REMIND_MODE.upper() == "WINDOW":
        return 0 <= delta <= REMIND_WINDOW_DAYS
    # MILESTONES
    return delta in REMIND_DAYS_SET

def build_user_reminders(lookup, items, today: date) -> List[str]:
    lines: List[str] = ["🔔 Напоминание:"]
    for oid, prof in items:
        o = lookup.get((oid, prof))
        if not o:
            continue
        for dt, label in parse_dates_from_cell(o['date_desc'], today):
            delta = (dt - today).days
            if due_by_policy(delta):
                when = "сегодня" if delta == 0 else "завтра" if delta == 1 else f"осталось {delta} дн. {dt}."
                lines.append(f"🔔 {o['name']} ({prof}, ур. {o['level']}): {when} — {label}\n{o['link']}")
    return lines

async def send_daily(context: ContextTypes.DEFAULT_TYPE):
    today  = datetime.now(TIMEZONE).date()
    olys   = fetch_olympiads()
    lookup = {(o['id'], p): o for o in olys for p in o['profiles']}

    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("SELECT user_id, olympiad_id, profile FROM subscriptions")
    subs = cur.fetchall()
    conn.close()

    by_user = {}
    for uid, oid, prof in subs:
        by_user.setdefault(uid, []).append((oid, prof))

    for uid, items in by_user.items():
        lines = build_user_reminders(lookup, items, today)
        if lines != ["🔔 Напоминание:"]:
            for ch in chunk_messages(lines):
                try:    await context.bot.send_message(chat_id=uid, text=ch)
                except (Forbidden, BadRequest): pass
                except Exception:               pass
        elif SEND_EMPTY_INFO:
            try:
                await context.bot.send_message(chat_id=uid, text="ℹ️ Сегодня напоминаний нет.")
            except Exception:
                pass

# ===================== Админ: ручной прогон прямо сейчас =====================
async def test_notify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Только для админа."); return

    today  = datetime.now(TIMEZONE).date()
    olys   = fetch_olympiads()
    lookup = {(o['id'], p): o for o in olys for p in o['profiles']}

    uid = update.effective_user.id
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("SELECT olympiad_id, profile FROM subscriptions WHERE user_id=?", (uid,))
    items = cur.fetchall()
    conn.close()

    lines = build_user_reminders(lookup, items, today)
    if lines and lines != ["🔔 Напоминание:"]:
        for ch in chunk_messages(lines):
            await update.message.reply_text("🧪 TEST:\n\n" + ch)
    else:
        await update.message.reply_text("🧪 TEST: Сегодня напоминаний бы не было по текущей политике.")

# ===================== АДМИН-РАССЫЛКА =====================
def split_text(text: str, max_len=MAX_MESSAGE_LENGTH) -> List[str]:
    if len(text) <= max_len: return [text]
    chunks, cur = [], ""
    for line in text.splitlines(keepends=True):
        if len(cur) + len(line) > max_len:
            chunks.append(cur); cur = ""
        cur += line
    if cur: chunks.append(cur)
    return chunks

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Эта команда доступна только администратору."); return

    ensure_user_in_db(update)

    if context.args:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text("Введите текст после команды или отправьте /broadcast и затем текст отдельным сообщением.")
            return
        await do_broadcast(update, context, text)
    else:
        context.user_data[UD_AWAIT_BROADCAST] = True
        await update.message.reply_text("✍️ Отправьте текст рассылки одним сообщением. Для отмены — /start.")

async def maybe_handle_broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and update.effective_user.id in ADMIN_IDS and context.user_data.get(UD_AWAIT_BROADCAST):
        context.user_data.pop(UD_AWAIT_BROADCAST, None)
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text("Пустой текст. Рассылка отменена."); return True
        await do_broadcast(update, context, text); return True
    return False

async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    admin_chat = update.effective_chat.id
    user_ids = sorted(get_all_user_ids())
    sent = failed = 0
    chunks = split_text(text)
    for uid in user_ids:
        try:
            for ch in chunks:
                await context.bot.send_message(chat_id=uid, text=ch)
            sent += 1
        except Exception:
            failed += 1
    await context.bot.send_message(chat_id=admin_chat, text=f"✅ Рассылка завершена.\nПолучателей: {len(user_ids)}\nУспешно: {sent}\nОшибок: {failed}")

# ===================== ПРОЧЕЕ =====================
async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Неизвестная команда. Напишите /start.")

async def catch_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maybe_handle_broadcast_text(update, context): return
    await update.message.reply_text("Напишите /start, чтобы начать.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.error("Ошибка при обработке обновления:", exc_info=context.error)

# ===================== FALLBACK-ПЛАНИРОВЩИК =====================
async def fallback_daily_scheduler(app: Application, notify_tm: time):
    """Если JobQueue недоступен — отправляем send_daily ежедневно в указанное время."""
    class DummyCtx:
        def __init__(self, bot): self.bot = bot

    while True:
        now = datetime.now(TIMEZONE)
        target = now.replace(hour=notify_tm.hour, minute=notify_tm.minute, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            await send_daily(DummyCtx(app.bot))
        except Exception:
            logging.exception("Ошибка в fallback_daily_scheduler.send_daily")

async def _post_init(app: Application):
    """Запускаем fallback-планировщик, если нет JobQueue."""
    if getattr(app, "job_queue", None) is None:
        notify_time = parse_daily_time(DAILY_NOTIFY_TIME)
        app.create_task(fallback_daily_scheduler(app, notify_time))
        logging.warning("JobQueue не найден — используется fallback-планировщик.")
    else:
        logging.info("JobQueue доступен — используется стандартный планировщик.")

# ===================== ЗАПУСК =====================
def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    init_db()

    notify_time = parse_daily_time(DAILY_NOTIFY_TIME)
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(_post_init)
        .build()
    )

    # Меню
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(menu_back_cb,   pattern="^menu_back$"))
    app.add_handler(CallbackQueryHandler(menu_select_cb, pattern="^menu_select$"))
    app.add_handler(CallbackQueryHandler(menu_list_cb,   pattern="^menu_list$"))
    app.add_handler(CallbackQueryHandler(menu_delete_cb, pattern="^menu_delete$"))

    # Удаление
    app.add_handler(CallbackQueryHandler(del_one_cb,        pattern="^del_one$"))
    app.add_handler(CallbackQueryHandler(del_one_oly_cb,    pattern="^del_one_oly\\|"))
    app.add_handler(CallbackQueryHandler(del_profile_cb,    pattern="^del_profile$"))
    app.add_handler(CallbackQueryHandler(del_profile_sel_cb,pattern="^del_profile_sel\\|"))

    # Подписка
    app.add_handler(CallbackQueryHandler(toggle_profile_cb, pattern="^toggle_profile\\|"))
    app.add_handler(CallbackQueryHandler(profiles_done_cb,  pattern="^profiles_done$"))
    app.add_handler(CallbackQueryHandler(include_all_cb,    pattern="^include_all$"))
    app.add_handler(CallbackQueryHandler(include_manual_cb, pattern="^include_manual$"))
    app.add_handler(CallbackQueryHandler(toggle_oly_cb,     pattern="^toggle_oly\\|"))
    app.add_handler(CallbackQueryHandler(manual_done_cb,    pattern="^manual_done$"))

    # Админ
    app.add_handler(CommandHandler("broadcast",  broadcast_cmd))
    app.add_handler(CommandHandler("testnotify", test_notify_cmd))

    # Текст и неизвестные команды
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, catch_all))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_error_handler(error_handler)

    # Планировщик
    if getattr(app, "job_queue", None) is not None:
        app.job_queue.run_daily(send_daily, time=notify_time, name="send_daily_job")

    try:
        app.run_polling(drop_pending_updates=True)
    except Conflict:
        logging.error("Запуск не удался: другой экземпляр бота уже запущен.")

if __name__ == "__main__":
    main()
