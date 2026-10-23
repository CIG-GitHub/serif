from ..vector import Vector
from . import temporal as _temporal
from .element_api import _elementwise_method
from .element_api import _elementwise_property
from datetime import date
from datetime import datetime
from datetime import timedelta
from .dtype import Schema
from ..errors import SerifValueError


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
