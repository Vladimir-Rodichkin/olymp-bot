"""
Чтение списка олимпиад из Excel.
"""
import re
from typing import Dict, List, Optional

import pandas as pd

from app import config

REQUIRED_COLUMN_KEYWORDS = {
    "id": ["название", "олимпиад"],
    "profile": ["профиль"],
    "date": ["дат"],
}


def detect_col(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    for col in df.columns:
        low = col.lower()
        if any(kw.lower() in low for kw in keywords):
            return col
    return None


def fetch_olympiads() -> List[Dict]:
    df = pd.read_excel(config.EXCEL_FILE, sheet_name=0)

    id_col = detect_col(df, REQUIRED_COLUMN_KEYWORDS["id"])
    prof_col = detect_col(df, REQUIRED_COLUMN_KEYWORDS["profile"])
    date_col = detect_col(df, REQUIRED_COLUMN_KEYWORDS["date"])
    lvl_col = detect_col(df, ["уровень"])
    desc_col = detect_col(df, ["описан"])
    link_col = detect_col(df, ["ссыл"])

    if not id_col or not prof_col or not date_col:
        raise RuntimeError("Не найдены обязательные столбцы в Excel (название/профиль/дата).")

    olympiads = []
    for _, row in df.iterrows():
        oid = str(row[id_col]).strip()
        raw_profiles = str(row.get(prof_col, "") or "")
        profiles = [p.strip() for p in re.split(r"[;,/]", raw_profiles) if p.strip()] or ["—"]
        olympiads.append(
            {
                "id": oid,
                "profiles": profiles,
                "name": oid,
                "date_desc": str(row.get(date_col, "") or "").strip(),
                "level": str(row.get(lvl_col, "") or "—").strip(),
                "description": str(row.get(desc_col, "") or "—").strip(),
                "link": str(row.get(link_col, "") or "—").strip(),
            }
        )
    return olympiads


def get_profiles(olys: List[Dict]) -> List[str]:
    s = set()
    for o in olys:
        s.update(o["profiles"])
    return sorted(s)


def filter_by_profile(olys: List[Dict], profile: str) -> List[Dict]:
    return [o for o in olys if profile in o["profiles"]]


def build_lookup(olys: List[Dict]) -> Dict[tuple, Dict]:
    """Индекс (olympiad_id, profile) -> запись олимпиады, используется при показе подписок."""
    return {(o["id"], p): o for o in olys for p in o["profiles"]}
