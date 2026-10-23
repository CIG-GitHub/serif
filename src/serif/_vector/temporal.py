"""
Int-tick operands for native date and datetime columns.

DateStorage and DatetimeStorage hold integer ticks (days / microseconds
since 1970-01-01). Where an operation means the same thing on those ints as
on the dates, callers swap in a tick view, an ArrayStorage over the same
buffer with no copy, and run their ordinary int code path unchanged.

These helpers only accept operands whose Python semantics the ticks
reproduce exactly: same-kind native columns, plain dates, naive datetimes.
They return None for anything else (timezone-aware datetimes, subclasses
such as pandas Timestamp, mixed kinds), and the caller keeps its boxed
path, whose behavior is the specification.
"""

from array import array
from datetime import date
from datetime import datetime

from .storage import ArrayStorage
from .storage import DateStorage
from .storage import DatetimeStorage
from .storage import _MICROS_PER_DAY
from .storage import _TemporalStorage


def tick_view(storage):
    """The column's ticks as int ArrayStorage, sharing buffer and mask."""
    return ArrayStorage(storage._ticks, storage._mask)


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
        return tick_view(left), tick_view(right)
    return None


def tick_scalar(storage, value):
    """(tick view, int tick) for a native column and a matching scalar."""
    if not isinstance(storage, _TemporalStorage):
        return None
    tick = _scalar_tick(storage, value)
    if tick is None:
        return None
    return tick_view(storage), tick


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
