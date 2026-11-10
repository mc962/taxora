"""Pure date-window helpers for partitioned ingest (no Temporal imports, so unit-testable
and safe to import inside workflow code).

A window is an inclusive (start, end) date pair, matching GBIF's ``eventDate=start,end``.
"""

from datetime import date, timedelta

Window = tuple[date, date]


def window_to_range(window: Window) -> str:
    return f"{window[0].isoformat()},{window[1].isoformat()}"


def window_from_range(event_date_range: str) -> Window:
    start, _, end = event_date_range.partition(",")
    return date.fromisoformat(start), date.fromisoformat(end or start)


def split_window(window: Window) -> tuple[Window, Window] | None:
    """Split a window into two contiguous, non-overlapping halves.

    Returns None for a single-day window — it can't be split any further.
    """
    start, end = window
    days = (end - start).days
    if days <= 0:
        return None
    mid = start + timedelta(days=days // 2)
    return (start, mid), (mid + timedelta(days=1), end)
