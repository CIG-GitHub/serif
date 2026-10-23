"""
Int-tick helpers for native date and datetime columns.

DateStorage and DatetimeStorage hold integer ticks (days / microseconds
since 1970-01-01). Where an operation means the same thing on those ints as
on the dates, callers swap in storage.tick_view(), an ArrayStorage over the
same buffer with no copy, and run their ordinary int code path unchanged.

These helpers only accept operands whose Python semantics the ticks
reproduce exactly: same-kind native columns, plain dates, naive datetimes.
They return None for anything else (timezone-aware datetimes, subclasses
such as pandas Timestamp, mixed kinds), and the caller keeps its boxed
path, whose behavior is the specification.
"""

from array import array
from datetime import date
from datetime import datetime
from itertools import repeat

from .nullable import _BitMaskBuilder
from .storage import ArrayStorage
from .storage import DateStorage
from .storage import DatetimeStorage
from .storage import _MICROS_PER_DAY
from .storage import _TemporalStorage


def _naive_micros(value):
    """µs tick of an exact naive datetime, or None. fold is ignored, as
    Python's own comparisons ignore it between naive datetimes."""
    if type(value) is not datetime or value.tzinfo is not None:
        return None
    if value.fold:
        value = value.replace(fold=0)
    return DatetimeStorage._unbox(value)


def _scalar_tick(storage, value):
    if isinstance(storage, DateStorage):
        if type(value) is date:
            return DateStorage._unbox(value)
        return None
    return _naive_micros(value)


def tick_pair(left, right):
    """Tick views for two same-kind native columns, or None."""
    if (isinstance(left, _TemporalStorage)
            and type(left) is type(right)):
        return left.tick_view(), right.tick_view()
    return None


def tick_scalar(storage, value):
    """(tick view, int tick) for a native column and a matching scalar."""
    if not isinstance(storage, _TemporalStorage):
        return None
    tick = _scalar_tick(storage, value)
    if tick is None:
        return None
    return storage.tick_view(), tick


def member_ticks(storage, members):
    """The set of member ticks for is_in on a native column, or None.

    Members Python never finds equal here (a datetime in a date column, a
    date or aware datetime in a datetime column) drop out. Any member that
    isn't an exact date/datetime returns None, keeping subclass and
    custom-__eq__ semantics on the boxed path.
    """
    if not isinstance(storage, _TemporalStorage):
        return None
    ticks = set()
    for member in members:
        if type(member) not in (date, datetime):
            return None
        tick = _scalar_tick(storage, member)
        if tick is not None:
            ticks.add(tick)
    return ticks


def shift_days(storage, offsets):
    """DateStorage moved by whole-day `offsets`, or None.

    `offsets` yields one int (or None, giving null) per lane. Returns None
    for non-native storage, or as soon as a result leaves Python's date
    range, so the caller's boxed path runs and raises the error it always
    has. Nullability follows the actual nulls, as the boxed path's does.
    """
    if not isinstance(storage, DateStorage):
        return None
    low, high = DateStorage._MIN_TICK, DateStorage._MAX_TICK
    mask = storage._mask
    ticks = array(DateStorage._TYPECODE)
    validity = _BitMaskBuilder()
    for i, (tick, offset) in enumerate(zip(storage._ticks, offsets)):
        if offset is None or (mask is not None and mask.is_null(i)):
            ticks.append(0)
            validity.append(True)
            continue
        moved = tick + offset
        if not low <= moved <= high:
            return None
        ticks.append(moved)
        validity.append(False)
    return DateStorage(ticks, validity.finish())


def days_between(storage, other, reverse=False):
    """Whole days `storage - other` as int ArrayStorage, or None.

    `other` is a DateStorage of the same length or a plain date scalar;
    reverse=True gives `other - storage`. None for anything else, leaving
    subclasses and fallback columns to the boxed path.
    """
    if not isinstance(storage, DateStorage):
        return None
    if isinstance(other, DateStorage):
        others, other_mask = other._ticks, other._mask
    else:
        tick = _scalar_tick(storage, other)
        if tick is None:
            return None
        others, other_mask = repeat(tick), None
    mask = storage._mask
    days = array('q')
    validity = _BitMaskBuilder()
    for i, (tick, other_tick) in enumerate(zip(storage._ticks, others)):
        if ((mask is not None and mask.is_null(i))
                or (other_mask is not None and other_mask.is_null(i))):
            days.append(0)
            validity.append(True)
            continue
        days.append(other_tick - tick if reverse else tick - other_tick)
        validity.append(False)
    return ArrayStorage(days, validity.finish())


def month_ends(storage):
    """DateStorage of each date's last day of its month, or None.

    Each date boxes once to read its year and month; the month-end tick is
    computed once per distinct month. December 9999 has no next month to
    back up from: None, so the boxed path raises as it always has.
    """
    if not isinstance(storage, DateStorage):
        return None
    ends = {}
    mask = storage._mask
    ticks = array(DateStorage._TYPECODE)
    validity = _BitMaskBuilder()
    for i, tick in enumerate(storage._ticks):
        if mask is not None and mask.is_null(i):
            ticks.append(0)
            validity.append(True)
            continue
        day = DateStorage._box(tick)
        month = (day.year, day.month)
        end = ends.get(month)
        if end is None:
            year, number = month
            if year == date.max.year and number == 12:
                return None
            first_of_next = date(year + number // 12, number % 12 + 1, 1)
            end = DateStorage._unbox(first_of_next) - 1
            ends[month] = end
        ticks.append(end)
        validity.append(False)
    return DateStorage(ticks, validity.finish())


def at_midnight(storage):
    """DateStorage days scaled to DatetimeStorage µs, or None.

    Null lanes become 0: a file's null lanes may hold any int32, which times
    a day of µs could overflow int64.
    """
    if not isinstance(storage, DateStorage):
        return None
    ticks = storage._ticks
    mask = storage._mask
    if mask is None:
        micros = array('q', (tick * _MICROS_PER_DAY for tick in ticks))
    else:
        micros = array('q', (
            0 if mask.is_null(i) else tick * _MICROS_PER_DAY
            for i, tick in enumerate(ticks)
        ))
    return DatetimeStorage(micros, mask)
