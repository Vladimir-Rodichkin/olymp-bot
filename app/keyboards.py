"""Все InlineKeyboardMarkup."""
from typing import Dict, List, Sequence, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

BACK_TO_MENU = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")]])


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🎯 Выбрать олимпиаду", callback_data="menu_select")],
            [InlineKeyboardButton("📋 Мои подписки", callback_data="menu_list")],
            [InlineKeyboardButton("🗑️ Удалить подписку", callback_data="menu_delete")],
        ]
    )


def delete_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Удалить конкретную", callback_data="del_one")],
            [InlineKeyboardButton("Удалить по профилю", callback_data="del_profile")],
            [InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")],
        ]
    )


def profiles_markup(profiles: Sequence[str], selection: Sequence[str]) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(f"{'✅' if p in selection else '☐'} {p}", callback_data=f"toggle_profile|{i}")]
        for i, p in enumerate(profiles)
    ]
    kb.append([InlineKeyboardButton("Готово", callback_data="profiles_done")])
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(kb)


def profile_option_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Учитывать все", callback_data="include_all")],
            [InlineKeyboardButton("Выбрать вручную", callback_data="include_manual")],
            [InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")],
        ]
    )


def manual_olympiads_markup(olys: Sequence[Dict], selection: Sequence[int]) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(f"{'✅' if i in selection else '☐'} {o['name']}", callback_data=f"toggle_oly|{i}")]
        for i, o in enumerate(olys)
    ]
    kb.append([InlineKeyboardButton("Готово", callback_data="manual_done")])
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(kb)


def delete_one_markup(rows: Sequence[Tuple[str, str, str]]) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(f"{name} ({prof})", callback_data=f"del_one_oly|{i}")]
        for i, (_, name, prof) in enumerate(rows)
    ]
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(kb)


def delete_profile_markup(profiles: Sequence[str]) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(prof, callback_data=f"del_profile_sel|{i}")] for i, prof in enumerate(profiles)
    ]
    kb.append([InlineKeyboardButton("↩️ Главное меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(kb)
