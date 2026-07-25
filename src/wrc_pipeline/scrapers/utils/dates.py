"""Date-range partitioning.

A partition is ``(partition_date, query_start, query_end)`` where
``partition_date`` is the canonical bucket key (first day of the month /
Monday of the ISO week / the day itself) and ``query_start`` / ``query_end``
are the inclusive slice clipped to the caller's requested range. The bucket
key stays stable across runs so downstream ``partition_date`` values line
up regardless of where the requested range began.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from typing import Callable, Iterator

Partition = tuple[date, date, date]


def parse_iso(s: str) -> date:
    return date.fromisoformat(s)


def _monthly(start: date, end: date) -> Iterator[Partition]:
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        month_end = date(cursor.year, cursor.month, monthrange(cursor.year, cursor.month)[1])
        yield (cursor, max(cursor, start), min(month_end, end))
        cursor = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )


def _weekly(start: date, end: date) -> Iterator[Partition]:
    # Monday-anchored ISO weeks — same bucket key regardless of where in the
    # week the requested range starts.
    cursor = start - timedelta(days=start.weekday())
    while cursor <= end:
        week_end = cursor + timedelta(days=6)
        yield (cursor, max(cursor, start), min(week_end, end))
        cursor += timedelta(days=7)


def _daily(start: date, end: date) -> Iterator[Partition]:
    cursor = start
    while cursor <= end:
        yield (cursor, cursor, cursor)
        cursor += timedelta(days=1)


_DISPATCH: dict[str, Callable[[date, date], Iterator[Partition]]] = {
    "monthly": _monthly,
    "weekly": _weekly,
    "daily": _daily,
}

PARTITION_SIZES: tuple[str, ...] = tuple(_DISPATCH)


def iter_partitions(start: date, end: date, size: str) -> Iterator[Partition]:
    try:
        gen = _DISPATCH[size]
    except KeyError:
        raise ValueError(
            f"unknown partition_size={size!r} (expected: {list(_DISPATCH)})"
        ) from None
    if end < start:
        return iter(())
    return gen(start, end)


def to_site_date(d: date) -> str:
    """Site accepts DD/MM/YYYY, DD-MM-YYYY, or YYYY-MM-DD (recon §7.3). Use ISO."""
    return d.isoformat()
