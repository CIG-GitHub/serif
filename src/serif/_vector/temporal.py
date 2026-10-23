"""
Tick-space fast paths for native date and datetime columns.

DateStorage and DatetimeStorage hold integer ticks (days / microseconds
since 1970-01-01). Wherever an operation's meaning on dates is exactly its
meaning on those ints, the column is viewed as a plain int column, an
ArrayStorage over the same tick buffer, no copy, and the operation runs
through the existing int machinery: optional accelerators, the pure loop,
null handling, result types. Nothing new to keep conformant.

The fast paths take only operands whose Python semantics the ticks
reproduce exactly: plain naive date/datetime values (and ISO strings, which
_Date compares as dates). Everything else (timezone-aware datetimes,
subclasses such as pandas Timestamp, mixed kinds Python refuses to order)
DECLINES to the boxed Python path, whose behavior is the specification.
"""

from array import array
from datetime import date
from datetime import datetime

from .._execution import DECLINED
from .dtype import Schema
from .storage import ArrayStorage
from .storage import DateStorage
from .storage import DatetimeStorage
from .storage import _MICROS_PER_DAY
from .storage import _TemporalStorage


def _vector_class():
    # Local import avoids a cycle while Vector delegates here.
    from ..vector import Vector
    return Vector


def _tick_view(storage, widen=False):
    """The column's ticks as int ArrayStorage; widen=True scales days to µs.

    Widening rebuilds the buffer. Null lanes become 0 there: a file's null
    lanes may hold any int32, which times a day of µs could overflow int64.
    """
    ticks = storage._ticks
    mask = storage._mask
    if not widen:
        return ArrayStorage(ticks, mask)
    if mask is None:
        widened = array('q', (tick * _MICROS_PER_DAY for tick in ticks))
    else:
        widened = array('q', (
            0 if mask.is_null(i) else tick * _MICROS_PER_DAY
            for i, tick in enumerate(ticks)
        ))
    return ArrayStorage(widened, mask)


def _int_vector(vector, view):
    Vector = _vector_class()
    nullable = vector._dtype.nullable if vector._dtype is not None else True
    return Vector._from_storage(view, Schema(int, nullable))


def _naive_micros(value):
    """µs tick of an exact naive datetime, or None. fold is ignored, as
    Python's own comparisons ignore it between naive datetimes."""
    if type(value) is not datetime or value.tzinfo is not None:
        return None
    if value.fold:
        value = value.replace(fold=0)
    return DatetimeStorage._unbox(value)


def _scalar_ticks(storage, value):
    """Return (tick, widen) for a scalar comparable in tick space, or None.

    widen=True means the tick is in µs and a date column must scale up to
    meet it: a date compares as midnight against a datetime, as _Date does.
    """
    if isinstance(storage, DateStorage):
        vtype = type(value)
        if vtype is date:
            return DateStorage._unbox(value), False
        if vtype is str:
            return DateStorage._unbox(date.fromisoformat(value)), False
        micros = _naive_micros(value)
        if micros is not None:
            return micros, True
        return None
    micros = _naive_micros(value)
    if micros is not None:
        return micros, False
    return None


def compare(vector, other, op):
    """Compare a native temporal column in tick space, or DECLINE."""
    storage = vector._storage
    if not isinstance(storage, _TemporalStorage):
        return DECLINED
    Vector = _vector_class()

    if isinstance(other, Vector):
        other_storage = other._storage
        if (other.ndims() > 1
                or len(other) != len(vector)
                or not isinstance(other_storage, _TemporalStorage)):
            return DECLINED
        if type(storage) is type(other_storage):
            left = _tick_view(storage)
            right = _tick_view(other_storage)
        elif isinstance(storage, DateStorage):
            # date vs datetime column: the date side is midnight.
            left = _tick_view(storage, widen=True)
            right = _tick_view(other_storage)
        else:
            # datetime vs date: Python returns False for == and refuses
            # to order them; keep the boxed path's exact behavior.
            return DECLINED
        return _int_vector(vector, left)._elementwise_compare(
            _int_vector(other, right), op)

    scalar = _scalar_ticks(storage, other)
    if scalar is None:
        return DECLINED
    tick, widen = scalar
    left = _tick_view(storage, widen=widen)
    return _int_vector(vector, left)._elementwise_compare(tick, op)


def is_in(vector, members, match_null):
    """Membership in tick space, or DECLINE.

    `members` are the non-None members, already screened for the dead-member
    warning by the caller. Members Python can never find equal here (a
    datetime in a date column, a date or aware datetime in a datetime
    column) drop out. Any member that isn't an exact date/datetime declines,
    keeping subclass and custom-__eq__ semantics on the boxed path.
    """
    storage = vector._storage
    if not isinstance(storage, _TemporalStorage):
        return DECLINED

    is_date_column = isinstance(storage, DateStorage)
    ticks = set()
    for member in members:
        mtype = type(member)
        if mtype is date:
            if is_date_column:
                ticks.add(DateStorage._unbox(member))
        elif mtype is datetime:
            if not is_date_column:
                micros = _naive_micros(member)
                if micros is not None:
                    ticks.add(micros)
        else:
            return DECLINED

    tick_members = sorted(ticks)
    if match_null:
        tick_members.append(None)
    return _int_vector(vector, _tick_view(storage)).is_in(tick_members)
