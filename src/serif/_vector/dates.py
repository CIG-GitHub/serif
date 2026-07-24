from ..vector import Vector
from .element_api import _elementwise_attribute
from .element_api import _elementwise_proxy
from datetime import date
from datetime import datetime
from datetime import timedelta
from collections.abc import Iterable
from ..errors import SerifValueError


class _Date(Vector):
    def _elementwise_compare(self, other, op):
        # Unknown in, unknown out (docs/null-semantics.md). Also note:
        # dates promote to midnight via datetime.min.time() when compared
        # against datetimes.
        other = self._check_duplicate(other)

        def _wrap(vals):
            return Vector._from_iterable_known_kind(vals, bool)

        if isinstance(other, Vector):
            if len(self) != len(other):
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            if other.schema() is not None and other.schema().kind is str:
                return _wrap(
                    None if (x is None or y is None) else bool(op(x, date.fromisoformat(y)))
                    for x, y in zip(self, other, strict=True)
                )
            if other.schema() is not None and other.schema().kind is datetime:
                return _wrap(
                    None if (x is None or y is None) else bool(op(datetime.combine(x, datetime.min.time()), y))
                    for x, y in zip(self, other, strict=True)
                )
        elif isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            if len(self) != len(other):
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            return _wrap(
                None if (x is None or y is None) else bool(op(x, y))
                for x, y in zip(self, other, strict=True)
            )
        elif isinstance(other, str):
            rhs = date.fromisoformat(other)
            return _wrap(
                None if x is None else bool(op(x, rhs))
                for x in self
            )
        elif isinstance(other, datetime):
            return _wrap(
                None if x is None else bool(op(datetime.combine(x, datetime.min.time()), other))
                for x in self
            )
        # finally,
        return super()._elementwise_compare(other, op)

    def __add__(self, other):
        """ adding integers is adding days """
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
            return Vector._from_iterable_known_kind(
                (
                    (other - s).days if s is not None else None
                    for s in self._storage
                ),
                int,
            )
        return super().__rsub__(other)

    def eomonth(self):
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
    setattr(_Date, _m, _elementwise_proxy(_m, _DATE_PROXY_KINDS.get(_m)))
del _m

for _attribute in ('year', 'month', 'day'):
    setattr(_Date, _attribute, _elementwise_attribute(_attribute, int))
del _attribute
