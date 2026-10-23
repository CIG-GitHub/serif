from ..vector import Vector
from . import temporal as _temporal
from .element_api import _elementwise_method
from .element_api import _elementwise_property
from datetime import date
from datetime import datetime
from datetime import timedelta
from itertools import repeat
from .dtype import Schema
from ..errors import SerifValueError


def _day_offsets(other, sign, n):
    """Per-lane whole-day offsets for `date ± other`, or None.

    `other` is an int or timedelta, scalar or typed column of length n.
    Python's date arithmetic counts only a timedelta's .days, both ways:
    `date - td` is `date + timedelta(-td.days)`. So a negative sub-day
    timedelta still moves the date (its .days is -1), while
    `date - timedelta(hours=1)` leaves it where it is.
    """
    def days(value):
        if isinstance(value, timedelta):
            return sign * value.days
        return sign * value

    if isinstance(other, Vector):
        schema = other.schema()
        if (schema is None or schema.kind not in (int, timedelta)
                or len(other) != n):
            return None
        return (None if value is None else days(value)
                for value in other._storage)
    if isinstance(other, (int, timedelta)):
        return repeat(days(other), n)
    return None


def _wrap(storage, kind):
    """A tick-path result, nullable only where nulls actually landed."""
    return Vector._from_storage(storage, Schema(kind, storage._mask is not None))


class _Date(Vector):
    def _elementwise_compare(self, other, op):
        # A date compares as the date an ISO string names, and as midnight
        # against a datetime. Rewrite whichever side needs it, then compare
        # like kinds through the base path (docs/null-semantics.md), where
        # native storage compares as int ticks.
        other = self._check_duplicate(other)

        if isinstance(other, Vector) and other.ndims() == 1:
            schema = other.schema()
            kind = schema.kind if schema is not None else None
            if kind is str:
                other = Vector._from_iterable_known_dtype(
                    (None if y is None else date.fromisoformat(y)
                     for y in other),
                    Schema(date, schema.nullable),
                )
            elif kind is datetime:
                return self._at_midnight()._elementwise_compare(other, op)
        elif isinstance(other, str):
            other = date.fromisoformat(other)
        elif isinstance(other, datetime):
            return self._at_midnight()._elementwise_compare(other, op)
        return super()._elementwise_compare(other, op)

    def _at_midnight(self):
        """This column as datetimes at midnight, same name and nullability."""
        dtype = Schema(datetime, self._dtype.nullable)
        storage = _temporal.at_midnight(self._storage)
        if storage is not None:
            return Vector._from_storage(storage, dtype, name=self._name)
        return Vector._from_iterable_known_dtype(
            (None if x is None else datetime.combine(x, datetime.min.time())
             for x in self),
            dtype,
            name=self._name,
        )

    def __add__(self, other):
        """ adding integers is adding days """
        # Native storage moves its day ticks; anything the tick path
        # declines (or that leaves the date range) takes the boxed path.
        offsets = _day_offsets(other, 1, len(self))
        if offsets is not None:
            storage = _temporal.shift_days(self._storage, offsets)
            if storage is not None:
                return _wrap(storage, date)

        if isinstance(other, Vector) and other.schema() is not None:
            other_kind = other.schema().kind
            if other_kind in (int, timedelta):
                if len(self) != len(other):
                    raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
                return Vector._from_iterable_known_kind(
                    (
                        (
                            date.fromordinal(s.toordinal() + y)
                            if other_kind is int
                            else s + y
                        )
                        if s is not None and y is not None
                        else None
                        for s, y in zip(self._storage, other, strict=True)
                    ),
                    date,
                )

        if isinstance(other, int):
            return Vector._from_iterable_known_kind(
                (
                    date.fromordinal(s.toordinal() + other)
                    if s is not None
                    else None
                    for s in self._storage
                ),
                date,
            )
        if isinstance(other, timedelta):
            return Vector._from_iterable_known_kind(
                (
                    s + other if s is not None else None
                    for s in self._storage
                ),
                date,
            )
        # Everything else goes through the base elementwise machinery.
        return super().__add__(other)

    def __radd__(self, other):
        if isinstance(other, timedelta):
            storage = _temporal.shift_days(
                self._storage, _day_offsets(other, 1, len(self)))
            if storage is not None:
                return _wrap(storage, date)
            return Vector._from_iterable_known_kind(
                (
                    other + s if s is not None else None
                    for s in self._storage
                ),
                date,
            )
        return super().__radd__(other)

    def __sub__(self, other):
        """
        Date algebra in whole days (Excel-style, closing the int-days ring):

            datevec + int  → date        datevec - int  → date
            datevec - date → int (days)

        datetime vectors are NOT _Date and keep Python's timedelta
        semantics — subsecond precision matters there.
        """
        # Native storage works on day ticks; anything the tick path
        # declines (or that leaves the date range) takes the boxed path.
        if isinstance(other, Vector):
            schema = other.schema()
            if (schema is not None and schema.kind is date
                    and len(other) == len(self)):
                storage = _temporal.days_between(self._storage, other._storage)
                if storage is not None:
                    return _wrap(storage, int)
        else:
            storage = _temporal.days_between(self._storage, other)
            if storage is not None:
                return _wrap(storage, int)
        offsets = _day_offsets(other, -1, len(self))
        if offsets is not None:
            storage = _temporal.shift_days(self._storage, offsets)
            if storage is not None:
                return _wrap(storage, date)

        if isinstance(other, Vector) and other.schema() is not None:
            other_kind = other.schema().kind
            if other_kind in (int, timedelta, date):
                if len(self) != len(other):
                    raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
                if other_kind is date:
                    return Vector._from_iterable_known_kind(
                        (
                            (s - y).days
                            if s is not None and y is not None
                            else None
                            for s, y in zip(self._storage, other, strict=True)
                        ),
                        int,
                    )
                return Vector._from_iterable_known_kind(
                    (
                        (
                            date.fromordinal(s.toordinal() - y)
                            if other_kind is int
                            else s - y
                        )
                        if s is not None and y is not None
                        else None
                        for s, y in zip(self._storage, other, strict=True)
                    ),
                    date,
                )

        if isinstance(other, int):
            return Vector._from_iterable_known_kind(
                (
                    date.fromordinal(s.toordinal() - other)
                    if s is not None
                    else None
                    for s in self._storage
                ),
                date,
            )

        if isinstance(other, date) and not isinstance(other, datetime):
            return Vector._from_iterable_known_kind(
                (
                    (s - other).days if s is not None else None
                    for s in self._storage
                ),
                int,
            )

        if isinstance(other, timedelta):
            return Vector._from_iterable_known_kind(
                (
                    s - other if s is not None else None
                    for s in self._storage
                ),
                date,
            )

        return super().__sub__(other)

    def __rsub__(self, other):
        """date_scalar - datevec → int days (same whole-day algebra)."""
        if isinstance(other, date) and not isinstance(other, datetime):
            storage = _temporal.days_between(self._storage, other, reverse=True)
            if storage is not None:
                return _wrap(storage, int)
            return Vector._from_iterable_known_kind(
                (
                    (other - s).days if s is not None else None
                    for s in self._storage
                ),
                int,
            )
        return super().__rsub__(other)

    def eomonth(self):
        storage = _temporal.month_ends(self._storage)
        if storage is not None:
            return _wrap(storage, date)

        def values():
            for d in self._storage:
                if d is None:
                    yield None
                    continue

                # move to first of next month
                first_next = (
                    d.replace(day=28) + timedelta(days=4)
                ).replace(day=1)

                # back up one day -> last day of original month
                yield first_next - timedelta(days=1)

        return Vector._from_iterable_known_kind(values(), date)


# Plain per-element date methods, stamped onto the class at definition time
# (see string.py for the rationale). date's class/static constructors
# (today, fromordinal, fromtimestamp, fromisoformat, fromisocalendar) are
# deliberately not stamped — mapping a constructor across elements ignores
# the element and is meaningless as a column operation.
_DATE_PROXY_METHODS = (
    'ctime', 'isocalendar', 'isoformat', 'isoweekday', 'replace',
    'strftime', 'timetuple', 'toordinal', 'weekday',
)

_DATE_PROXY_KINDS = {
    'ctime': str,
    'isocalendar': tuple,
    'isoformat': str,
    'isoweekday': int,
    'replace': date,
    'strftime': str,
    'timetuple': tuple,
    'toordinal': int,
    'weekday': int,
}

for _m in _DATE_PROXY_METHODS:
    setattr(
        _Date,
        _m,
        _elementwise_method(date, _m, _DATE_PROXY_KINDS[_m]),
    )
del _m

for _attribute in ('year', 'month', 'day'):
    setattr(
        _Date,
        _attribute,
        _elementwise_property(date, _attribute, int),
    )
del _attribute
