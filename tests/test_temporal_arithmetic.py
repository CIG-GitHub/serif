"""
Whole-day date arithmetic on native date columns, done on int ticks.

Ground truth is Python's own date arithmetic on the boxed values: the tick
path must reproduce it lane for lane, including how a timedelta's .days
counts both ways (date + timedelta(hours=-1) is the day before, while
date - timedelta(hours=1) is the same day), null propagation, and result
nullability following the actual nulls. Results
that leave Python's date range fall back to the boxed path, which raises
exactly what it always has.
"""

from contextlib import contextmanager
from datetime import date
from datetime import timedelta

import pytest

from serif import Schema
from serif import Vector
from serif._vector.storage import ArrayStorage
from serif._vector.storage import DateStorage


DATES = [date(2024, 1, 31), None, date(1969, 12, 31), date(2024, 2, 29),
         date(1, 1, 2)]
DAY_COUNTS = [1, 5, None, -365, 0]
SPANS = [timedelta(days=1), timedelta(hours=30), None,
         timedelta(hours=-1), timedelta(days=-1, hours=23)]


@pytest.fixture
def no_boxing(monkeypatch):
    """A context manager inside which boxing any date tick fails."""
    def fail(tick):
        raise AssertionError('tick path boxed a date')

    @contextmanager
    def guard():
        with monkeypatch.context() as patcher:
            patcher.setattr(DateStorage, '_box', staticmethod(fail))
            yield

    return guard


def _lanes(left, right, op):
    if not isinstance(right, list):
        right = [right] * len(left)
    return [None if x is None or y is None else op(x, y)
            for x, y in zip(left, right)]


def _add_days(x, n):
    return date.fromordinal(x.toordinal() + n)


def _sub_days(x, n):
    return date.fromordinal(x.toordinal() - n)


# ---------------------------------------------------------------------------
# date ± int / timedelta -> date
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "other,op,python_op",
    [
        (3, '+', _add_days),
        (-40, '-', _sub_days),
        (True, '+', _add_days),
        (timedelta(days=2), '+', lambda x, y: x + y),
        (timedelta(hours=30), '+', lambda x, y: x + y),
        (timedelta(hours=1), '-', lambda x, y: x - y),
        (timedelta(hours=-1), '+', lambda x, y: x + y),
    ],
    ids=['+int', '-int', '+bool', '+td', '+td-30h', '-td-1h', '+td-neg-1h'],
)
def test_scalar_shift_matches_python(no_boxing, other, op, python_op):
    v = Vector(DATES)
    expected = _lanes(DATES, other, python_op)

    with no_boxing():
        result = v + other if op == '+' else v - other
        storage = result._storage

    assert type(storage) is DateStorage
    assert list(result) == expected
    assert result.schema() == Schema(date, True)


@pytest.mark.parametrize(
    "others,op,python_op",
    [
        (DAY_COUNTS, '+', _add_days),
        (DAY_COUNTS, '-', _sub_days),
        (SPANS, '+', lambda x, y: x + y),
        (SPANS, '-', lambda x, y: x - y),
    ],
    ids=['+ints', '-ints', '+tds', '-tds'],
)
def test_column_shift_matches_python(no_boxing, others, op, python_op):
    v, w = Vector(DATES), Vector(others)
    expected = _lanes(DATES, others, python_op)

    with no_boxing():
        result = v + w if op == '+' else v - w

    assert list(result) == expected


def test_timedelta_plus_dates_matches_python(no_boxing):
    v = Vector(DATES)
    span = timedelta(days=10, hours=5)
    expected = _lanes(DATES, span, lambda x, y: y + x)

    with no_boxing():
        result = span + v

    assert list(result) == expected


def test_shift_nullability_follows_actual_nulls():
    declared = Vector([date(2024, 1, 1)], dtype=Schema(date, True))

    assert (declared + 1).schema() == Schema(date, False)
    assert (Vector(DATES) + 1).schema() == Schema(date, True)


# ---------------------------------------------------------------------------
# date - date -> int days
# ---------------------------------------------------------------------------

def test_date_minus_date_matches_python(no_boxing):
    v, w = Vector(DATES), Vector([date(2024, 1, 1)] * 4 + [None])
    anchor = date(2000, 1, 1)

    with no_boxing():
        columns = v - w
        scalar = v - anchor
        reverse = anchor - v

    assert type(columns._storage) is ArrayStorage
    assert list(columns) == _lanes(DATES, list(w), lambda x, y: (x - y).days)
    assert list(scalar) == _lanes(DATES, anchor, lambda x, y: (x - y).days)
    assert list(reverse) == _lanes(DATES, anchor, lambda x, y: (y - x).days)
    assert scalar.schema() == Schema(int, True)


# ---------------------------------------------------------------------------
# eomonth
# ---------------------------------------------------------------------------

def test_eomonth_matches_python():
    days = [date(2024, 2, 10), None, date(2023, 2, 28), date(2024, 12, 31),
            date(1969, 12, 1), date(1, 1, 1), date(2024, 2, 1)]
    expected = [
        None if d is None
        else (d.replace(day=28) + timedelta(days=4)).replace(day=1)
        - timedelta(days=1)
        for d in days
    ]

    result = Vector(days).eomonth()

    assert type(result._storage) is DateStorage
    assert list(result) == expected


# ---------------------------------------------------------------------------
# Out of range: the boxed path raises what it always raised
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "run,error",
    [
        (lambda v: v + 1, ValueError),                   # fromordinal
        (lambda v: v + timedelta(days=1), OverflowError),
        (lambda v: v.eomonth(), OverflowError),
    ],
    ids=['+int', '+td', 'eomonth'],
)
def test_past_year_9999_raises_as_before(run, error):
    with pytest.raises(error):
        run(Vector([date(2024, 1, 1), date(9999, 12, 31)]))


def test_before_year_1_raises_as_before():
    with pytest.raises(ValueError):
        Vector([date(1, 1, 1)]) - 1


# ---------------------------------------------------------------------------
# Fallback columns keep the boxed path
# ---------------------------------------------------------------------------

def test_fallback_date_column_still_shifts():
    class SubDate(date):
        pass

    with pytest.warns(UserWarning, match='not a plain date'):
        v = Vector([date(2024, 1, 1), SubDate(2024, 1, 2), None])

    assert list(v + 1) == [date(2024, 1, 2), date(2024, 1, 3), None]
    assert list(v - date(2024, 1, 1)) == [0, 1, None]
