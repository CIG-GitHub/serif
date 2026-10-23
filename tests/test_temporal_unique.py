"""
unique() on native date/datetime columns dedupes their int ticks.

The spec is the generic path: first-seen order, None kept once, nullable
only when a null survives. The tick path must match it exactly without
boxing a single value.
"""

from contextlib import contextmanager
from datetime import date
from datetime import datetime

import pytest

from serif import Schema
from serif import Vector
from serif._vector.storage import DateStorage
from serif._vector.storage import DatetimeStorage
from serif._vector.storage import TupleStorage


D = [date(2024, 1, 2), None, date(1969, 12, 31), date(2024, 1, 2), None,
     date(1, 1, 1)]
T = [datetime(2024, 1, 2, 3), datetime(2024, 1, 2, 3), None,
     datetime(1969, 12, 31, 23, 59, 59, 999999)]


def _first_seen(values):
    out = []
    for value in values:
        if value not in out:
            out.append(value)
    return out


@pytest.fixture
def no_boxing(monkeypatch):
    def fail(tick):
        raise AssertionError('unique boxed a tick')

    @contextmanager
    def guard():
        with monkeypatch.context() as patcher:
            patcher.setattr(DateStorage, '_box', staticmethod(fail))
            patcher.setattr(DatetimeStorage, '_box', staticmethod(fail))
            yield

    return guard


@pytest.mark.parametrize("values,storage_type", [
    (D, DateStorage),
    ([x for x in D if x is not None], DateStorage),
    (T, DatetimeStorage),
    ([None, None], None),
], ids=['date', 'date_dense', 'datetime', 'all_null'])
def test_unique_matches_first_seen(values, storage_type, no_boxing):
    if storage_type is None:
        v = Vector(values, dtype=Schema(date, True))
        storage_type = DateStorage
    else:
        v = Vector(values)

    with no_boxing():
        result = v.unique()
        storage = result._storage

    expected = _first_seen(values)
    assert type(storage) is storage_type
    assert list(result) == expected
    assert result.schema() == Schema(v.schema().kind, None in expected)


def test_fallback_column_keeps_generic_path():
    class SubDate(date):
        pass

    with pytest.warns(UserWarning, match='not a plain date'):
        v = Vector([date(2024, 1, 1), SubDate(2024, 1, 2), date(2024, 1, 1)])

    assert type(v._storage) is TupleStorage
    # Rebuilding a column that still holds the subclass warns again.
    with pytest.warns(UserWarning, match='not a plain date'):
        result = v.unique()
    assert list(result) == [date(2024, 1, 1), date(2024, 1, 2)]
