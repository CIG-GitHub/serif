"""
Native temporal storage: date and datetime columns as epoch ticks.

DateStorage holds int32 days since 1970-01-01 and DatetimeStorage int64
microseconds since 1970-01-01T00:00 — Arrow's and Parquet's own encodings.
Values with no exact tick (timezone-aware or fold=1 datetimes, subclasses)
degrade the column to TupleStorage, with a warning. Protocol conformance
(slice/take/sort/setitem/...) lives in test_storage_protocol.py.
"""

from datetime import date
from datetime import datetime
from datetime import timezone

import pytest

from serif import Schema
from serif import Table
from serif import Vector
from serif._vector.storage import DateStorage
from serif._vector.storage import DatetimeStorage
from serif._vector.storage import TupleStorage


class _SubDate(date):
    pass


class _SubDatetime(datetime):
    pass


def test_date_ticks_are_days_since_epoch():
    leap_day = date(2024, 2, 29)
    v = Vector([date(1970, 1, 1), None, date(1969, 12, 31), leap_day])
    storage = v._storage

    assert type(storage) is DateStorage
    assert storage._ticks.typecode == 'i'
    assert storage._ticks.itemsize == 4
    assert storage._ticks.tolist() == [
        0, 0, -1, (leap_day - date(1970, 1, 1)).days,
    ]
    assert bytes(storage._mask._buf) == b'\x0d'


def test_datetime_ticks_are_microseconds_since_epoch():
    values = [
        datetime(1970, 1, 1),
        datetime(1970, 1, 1, 0, 0, 0, 1),
        datetime(1969, 12, 31, 23, 59, 59, 999999),
        None,
    ]
    storage = Vector(values)._storage

    assert type(storage) is DatetimeStorage
    assert storage._ticks.typecode == 'q'
    assert storage._ticks.tolist() == [0, 1, -1, 0]
    assert list(storage) == values


@pytest.mark.parametrize(
    "values,storage_type",
    [
        ([date.min, date.max, date(1970, 1, 1)], DateStorage),
        (
            [
                datetime.min,
                datetime.max,
                datetime(1, 1, 1, 0, 0, 0, 1),
                datetime(1970, 1, 1),
            ],
            DatetimeStorage,
        ),
    ],
    ids=['date', 'datetime'],
)
def test_full_python_range_round_trips_exactly(values, storage_type):
    v = Vector(values)

    assert type(v._storage) is storage_type
    assert list(v) == values


def test_empty_typed_columns_use_native_storage():
    assert type(Vector([], dtype=date)._storage) is DateStorage
    assert type(Vector([], dtype=datetime)._storage) is DatetimeStorage


def test_plain_date_in_datetime_column_is_midnight():
    v = Vector([date(2020, 1, 1), datetime(2020, 1, 2, 6)])

    assert v.schema() == Schema(datetime, False)
    assert type(v._storage) is DatetimeStorage
    assert list(v) == [datetime(2020, 1, 1), datetime(2020, 1, 2, 6)]
    assert all(type(value) is datetime for value in v)


def test_ticks_never_leak_as_numbers():
    dates = Vector([date(1970, 1, 2), None])
    stamps = Vector([datetime(1970, 1, 1, 0, 0, 0, 1)])

    assert list(dates == 1) == [False, None]
    assert list(stamps == 1) == [False]


@pytest.mark.parametrize(
    "bad,reason",
    [
        (datetime(2024, 1, 2, tzinfo=timezone.utc), 'timezone-aware'),
        (datetime(2024, 1, 2, fold=1), 'fold=1'),
        (_SubDatetime(2024, 1, 2), 'not a plain datetime'),
    ],
    ids=['aware', 'fold', 'subclass'],
)
def test_datetime_without_exact_tick_falls_back_with_warning(bad, reason):
    values = [datetime(2024, 1, 1), None, bad, datetime(2024, 1, 3)]

    with pytest.warns(UserWarning, match=reason):
        v = Vector(values)

    assert type(v._storage) is TupleStorage
    assert v.schema() == Schema(datetime, True)
    assert list(v) == values
    assert v[2] is bad


def test_date_subclass_falls_back_with_warning():
    sub = _SubDate(2024, 1, 2)

    with pytest.warns(UserWarning, match='not a plain date'):
        v = Vector([date(2024, 1, 1), sub])

    assert type(v._storage) is TupleStorage
    assert v.schema().kind is date
    assert v[1] is sub


def test_batch_writes_land_in_the_tick_buffer_without_leaking():
    source = Vector([date(2024, 1, 1), date(2024, 1, 2)], name='d')
    t = Table({'d': source})

    with t.batch() as m:
        private = m.d._storage
        m[0, 'd'] = date(1969, 12, 31)
        m[1, 'd'] = None
        assert m.d._storage is private

    assert type(t.d._storage) is DateStorage
    assert list(t.d) == [date(1969, 12, 31), None]
    assert list(source) == [date(2024, 1, 1), date(2024, 1, 2)]


def test_batch_write_without_exact_tick_rebuilds_into_fallback():
    t = Table({'ts': [datetime(2024, 1, 1), datetime(2024, 1, 2)]})
    aware = datetime(2024, 1, 3, tzinfo=timezone.utc)

    with t.batch() as m:
        with pytest.warns(UserWarning, match='timezone-aware'):
            m[1, 'ts'] = aware

    assert type(t.ts._storage) is TupleStorage
    assert list(t.ts) == [datetime(2024, 1, 1), aware]
