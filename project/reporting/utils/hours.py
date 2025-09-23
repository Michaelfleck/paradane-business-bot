from __future__ import annotations

"""
Business hours utilities.

- to12h: Convert "HHMM" or "HH:MM" (24h) strings to "h:MM AM/PM".
- formatBusinessHours: Normalize Yelp-style or Google opening_hours into a single string:
  "Monday (..), Tuesday (..), ..., Sunday (..)" with Mon-Sun order.
  Handles overnight when end < start or is_overnight is True by formatting as
  "5:00 PM - 1:00 AM" on the same weekday context.
"""

from typing import Any, Dict, List, Optional


WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def to12h(value: str) -> str:
    """
    Convert "HHMM" or "HH:MM" (00-23) to "h:MM AM/PM".
    Handles midnight (00:00 -> 12:00 AM) and noon (12:00 -> 12:00 PM).

    Args:
        value: String in "HHMM" or "HH:MM".

    Returns:
        String like "h:MM AM/PM". If parsing fails, returns value unchanged.
    """
    try:
        v = value.strip()
        if ":" in v:
            hh_str, mm_str = v.split(":", 1)
        else:
            if len(v) not in (3, 4):
                return value
            # 900 -> 09:00, 1300 -> 13:00
            if len(v) == 3:
                hh_str, mm_str = v[0], v[1:]
            else:
                hh_str, mm_str = v[:2], v[2:]
        hh = int(hh_str)
        mm = int(mm_str)
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return value
        suffix = "AM" if hh < 12 else "PM"
        h12 = hh % 12
        if h12 == 0:
            h12 = 12
        return f"{h12}:{mm:02d} {suffix}"
    except Exception:
        return value


def _format_range(start: str, end: str, is_overnight: bool = False) -> str:
    start12 = to12h(start)
    end12 = to12h(end)
    # Overnight if explicit or end time is numerically less than start (e.g., 1700 -> 0100)
    try:
        start_n = int(start.replace(":", "")) if ":" in start else int(start)
        end_n = int(end.replace(":", "")) if ":" in end else int(end)
        overnight = is_overnight or (end_n < start_n)
    except Exception:
        overnight = is_overnight
    if overnight:
        return f"{start12} - {end12}"
    return f"{start12} - {end12}"


def _init_week_schedule() -> Dict[str, List[str]]:
    return {day: [] for day in WEEKDAYS}


def _normalize_yelp_hours(hours: Any) -> Dict[str, List[str]]:
    """
    Yelp-style hours structure examples:
    - {"hours": [{"open": [{"day": 0, "start": "0900", "end": "1700", "is_overnight": false}, ...]}]}
    - or top-level [{"open": [...]}] as businesses.business_hours per spec

    Returns dict of weekday -> list of "h:MM AM/PM - h:MM AM/PM"
    """
    schedule = _init_week_schedule()
    if not hours:
        return schedule

    blocks: List[Dict[str, Any]] = []

    # Case 1: businesses.business_hours -> list with first element containing 'open'
    if isinstance(hours, list) and hours and isinstance(hours[0], dict) and isinstance(hours[0].get("open"), list):
        blocks = hours[0]["open"]

    # Case 2: Yelp 'hours' dict
    elif isinstance(hours, dict):
        if "hours" in hours and isinstance(hours["hours"], list) and hours["hours"]:
            h0 = hours["hours"][0]
            if isinstance(h0, dict) and isinstance(h0.get("open"), list):
                blocks = h0["open"]
        elif "open" in hours and isinstance(hours["open"], list):
            blocks = hours["open"]

    for b in blocks:
        try:
            day_num = int(b.get("day", -1))
            if not (0 <= day_num <= 6):
                continue
            start = str(b.get("start", "")).strip()
            end = str(b.get("end", "")).strip()
            is_overnight = bool(b.get("is_overnight", False))
            day = WEEKDAYS[day_num]
            if start and end:
                schedule[day].append(_format_range(start, end, is_overnight))
        except Exception:
            continue

    return schedule


def _normalize_google_opening_hours(opening_hours: Any) -> Dict[str, List[str]]:
    """
    Google-style opening_hours:
    - {"periods": [{"open": {"day": 0, "time": "0900"}, "close": {"day": 0, "time": "1700"}}, ...]}
      Note: Google 'day' uses 0=Sunday .. 6=Saturday. We convert to 0=Monday .. 6=Sunday for output.
    - or {"weekday_text": ["Monday: 9:00 AM – 5:00 PM", ...]}

    Returns dict of weekday -> list of "h:MM AM/PM - h:MM AM/PM" or a single textual line if given.
    """
    schedule = _init_week_schedule()
    if not opening_hours:
        return schedule

    if isinstance(opening_hours, dict):
        periods = opening_hours.get("periods")
        weekday_text = opening_hours.get("weekday_text")
        if isinstance(periods, list) and periods:
            for p in periods:
                try:
                    o = p.get("open") or {}
                    c = p.get("close") or {}
                    g_open_day = o.get("day")
                    g_close_day = c.get("day")
                    if g_open_day is None or g_close_day is None:
                        continue
                    # Map Google 0=Sun..6=Sat -> our 0=Mon..6=Sun
                    open_day_num = (int(g_open_day) - 1) % 7
                    close_day_num = (int(g_close_day) - 1) % 7
                    start = str(o.get("time", "")).strip()
                    end = str(c.get("time", "")).strip()
                    is_overnight = close_day_num != open_day_num
                    day = WEEKDAYS[open_day_num]
                    if start and end:
                        schedule[day].append(_format_range(start, end, bool(is_overnight)))
                except Exception:
                    continue
        elif isinstance(weekday_text, list) and weekday_text:
            # Keep line as-is per weekday if present
            for line in weekday_text:
                try:
                    if ":" in line:
                        day, rest = line.split(":", 1)
                        day = day.strip()
                        rest = rest.strip()
                        if day in schedule:
                            schedule[day] = [rest] if rest else []
                except Exception:
                    continue

    return schedule


def formatBusinessHours(input_hours: Any) -> str:
    """
    Normalize various hours formats (Yelp or Google) into a single string:
    "Monday (..), Tuesday (..), ..., Sunday (..)"

    Precedence:
      - Prefer Yelp-style in businesses.hours / hours.open
      - Else Google opening_hours with periods or weekday_text

    Overnight handling:
      - If is_overnight is true or end < start, label as "5:00 PM - 1:00 AM" on the same weekday.

    If no hours present for a day, shows "Closed".
    """
    # Try Yelp-style first (handles both businesses.business_hours list and Yelp 'hours' dict)
    schedule = _normalize_yelp_hours(input_hours)
    if not any(schedule[day] for day in WEEKDAYS):
        # Try Google opening_hours next
        schedule = _normalize_google_opening_hours(input_hours)

    parts: List[str] = []
    for day in WEEKDAYS:
        entries = schedule.get(day) or []
        if entries:
            parts.append(f"{day} ({'; '.join(entries)})")
        else:
            parts.append(f"{day} (Closed)")
    return ", ".join(parts)