"""
Разбор дат из ячеек Excel.
"""
import re
from datetime import date
from typing import List, Optional, Tuple

DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?")
RANGE_SEP_RE = re.compile(r"\s*[–—-]\s*")


def year_for_day_month(d: int, m: int, today: date) -> int:
    try:
        candidate = date(today.year, m, d)
    except ValueError:
        return today.year
    return candidate.year if candidate >= today else today.year + 1


def _resolve_year(dd: int, mm: int, y: Optional[str], today: date) -> int:
    if y:
        return int(y) + (2000 if int(y) < 100 else 0)
    return year_for_day_month(dd, mm, today)


def parse_dates_from_cell(cell: str, today: date) -> List[Tuple[date, str]]:
    if not cell:
        return []
    text = str(cell).strip()
    if not text or text.upper().startswith("ПОКА"):
        return []

    chunks = re.split(r"[\n;]+", text.replace("\r", "\n"))
    refined = []
    for ch in chunks:
        parts = [p.strip() for p in re.split(r",(?!\s*\d{1,2}\.\d{1,2})", ch) if p.strip()]
        refined.extend(parts)

    out: List[Tuple[date, str]] = []
    for entry in refined:
        entry = entry.strip()
        if not entry:
            continue

        # отделяем ярлык после '/'
        if "/" in entry:
            left, label = entry.split("/", 1)
            label = label.strip() or "событие"
        else:
            left, label = entry, "событие"

        # "с 12.09 по 14.09"
        if "с " in left.lower() and " по " in left.lower():
            m = DATE_RE.findall(left)
            if m:
                d1, m1, y1 = m[0]
                dd, mm = int(d1), int(m1)
                yy = _resolve_year(dd, mm, y1, today)
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
                    yy = _resolve_year(dd, mm, y1, today)
                    try:
                        dt = date(yy, mm, dd)
                        if dt >= today:
                            out.append((dt, label or "начало"))
                    except ValueError:
                        pass
            continue

        # одиночная дата
        m = DATE_RE.search(left) or DATE_RE.search(entry)
        if m:
            d, m_, y = m.groups()
            dd, mm = int(d), int(m_)
            yy = _resolve_year(dd, mm, y, today)
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
