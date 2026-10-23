"""
Tick-space comparisons and is_in for native date/datetime columns.

Ground truth is Python's own comparison of the boxed values: the tick path
must reproduce it lane for lane (null in, None out), with numpy and
without. Fast cases also run with boxing disabled, which proves they never
build a date object. Declining cases (aware or subclass operands, datetime
vs date) take the boxed path and must give the same answers as before.
"""

import operator
from array import array
from contextlib import contextmanager
from datetime import date
from datetime import datetime
from datetime import timezone

import pytest

from serif import Schema
from serif import Vector
from serif._vector._numpy import operators as numpy_ops
from serif._vector.nullable import BitMask
from serif._vector.storage import DateStorage
from serif._vector.storage import DatetimeStorage


OPS = [operator.eq, operator.ne, operator.lt,
       operator.le, operator.gt, operator.ge]
OP_IDS = ['eq', 'ne', 'lt', 'le', 'gt', 'ge']

DATES = [date(2024, 1, 2), None, date(1969, 12, 31), date(1970, 1, 1),
         date(2024, 1, 2)]
OTHER_DATES = [date(2024, 1, 2), date(2000, 1, 1), None, date(1970, 1, 1),
               date(2025, 1, 1)]
STAMPS = [datetime(2024, 1, 2), None,
          datetime(1969, 12, 31, 23, 59, 59, 999999),
          datetime(2024, 1, 1, 12), datetime(2024, 1, 2, 0, 0, 0, 1)]


class _SubDate(date):
    pass


def _midnight(value):
    return datetime.combine(value, datetime.min.time())


@pytest.fixture(params=['accelerated', 'pure'])
def backend(request, monkeypatch):
    if request.param == 'pure':
        monkeypatch.setattr(numpy_ops, '_USE_NUMPY', False)
    return request.param


@pytest.fixture
def no_boxing(monkeypatch):
    """A context manager inside which boxing any tick fails the test."""
    def fail(tick):
        raise AssertionError('tick path boxed a value')

    @contextmanager
    def guard():
        with monkeypatch.context() as patcher:
            patcher.setattr(DateStorage, '_box', staticmethod(fail))
            patcher.setattr(DatetimeStorage, '_box', staticmethod(fail))
            yield

    return guard


def _expected(left, right, op, promote=lambda value: value):
    if isinstance(right, list):
        return [
            None if x is None or y is None else op(promote(x), y)
            for x, y in zip(left, right)
        ]
    return [None if x is None else op(promote(x), right) for x in left]


# ---------------------------------------------------------------------------
# Fast path: results equal Python's, and no date object is ever built
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op", OPS, ids=OP_IDS)
@pytest.mark.parametrize(
    "values,other,promote",
    [
        (DATES, date(2024, 1, 2), None),
        (DATES, date(1969, 12, 31), None),
        (DATES, datetime(2024, 1, 1, 12), _midnight),
        (STAMPS, datetime(2024, 1, 2), None),
        (STAMPS, datetime(2024, 1, 2, fold=1), None),
    ],
    ids=['date', 'date-negative', 'date-vs-datetime', 'datetime', 'fold'],
)
def test_scalar_compare_matches_python(backend, no_boxing, values, other,
                                       promote, op):
    v = Vector(values)
    expected = _expected(values, other, op, promote or (lambda x: x))

    with no_boxing():
        result = v._elementwise_compare(other, op)

    assert list(result) == expected
    assert result.schema() == Schema(bool, True)


@pytest.mark.parametrize("op", OPS, ids=OP_IDS)
def test_iso_string_compares_as_its_date(backend, no_boxing, op):
    v = Vector(DATES)
    expected = _expected(DATES, date(2024, 1, 2), op)

    with no_boxing():
        result = v._elementwise_compare('2024-01-02', op)

    assert list(result) == expected


@pytest.mark.parametrize("op", OPS, ids=OP_IDS)
@pytest.mark.parametrize(
    "left,right,promote",
    [
        (DATES, OTHER_DATES, None),
        (DATES, STAMPS, _midnight),
        (STAMPS, STAMPS[::-1], None),
    ],
    ids=['date-date', 'date-datetime', 'datetime-datetime'],
)
def test_vector_compare_matches_python(backend, no_boxing, left, right,
                                       promote, op):
    lv, rv = Vector(left), Vector(right)
    expected = _expected(left, right, op, promote or (lambda x: x))

    with no_boxing():
        result = lv._elementwise_compare(rv, op)

    assert list(result) == expected
    assert result.schema() == Schema(bool, True)


def test_operators_route_through_ticks(backend, no_boxing):
    v = Vector(DATES)
    stamps = Vector(STAMPS)

    with no_boxing():
        assert list(v == date(2024, 1, 2)) == [True, None, False, False, True]
        assert list(v >= '1970-01-01') == [True, None, False, True, True]
        assert list(stamps < datetime(2024, 1, 2)) == [
            False, None, True, True, False]
        assert list(v == v) == [True, None, True, True, True]


@pytest.mark.parametrize("op", OPS, ids=OP_IDS)
def test_iso_string_vector_compares_as_dates(backend, no_boxing, op):
    strings = ['2024-01-02', '2000-01-01', None, '1970-01-01', '2025-01-01']
    v, s = Vector(DATES), Vector(strings)
    expected = _expected(DATES, OTHER_DATES, op)

    with no_boxing():
        result = v._elementwise_compare(s, op)

    assert list(result) == expected
    assert result.schema() == Schema(bool, True)


@pytest.mark.parametrize(
    "other",
    ['2024-01-02', datetime(2024, 1, 2), date(2024, 1, 2)],
    ids=['str', 'datetime', 'date'],
)
def test_result_nullability_follows_the_schemas(backend, other):
    # Declared nullable but holding no nulls: the result is nullable too,
    # for every operand kind, as with every other comparison.
    v = Vector([date(2024, 1, 2)], dtype=Schema(date, True))

    assert (v == other).schema() == Schema(bool, True)
    assert (Vector([date(2024, 1, 2)]) == other).schema() == Schema(bool, False)


def test_fallback_date_column_still_compares_at_midnight(backend):
    with pytest.warns(UserWarning, match='not a plain date'):
        v = Vector([date(2024, 1, 1), _SubDate(2024, 1, 2)])

    assert list(v == datetime(2024, 1, 2)) == [False, True]
    assert list(v < datetime(2024, 1, 1, 12)) == [True, False]


def test_widening_ignores_garbage_in_null_lanes(backend):
    # A file's null lanes may hold any int32; scaled to µs that overflows
    # int64. Widening must zero them, not multiply them.
    storage = DateStorage(
        array('i', [2**31 - 1, 0]),
        BitMask.from_iterable([True, False]),
    )
    v = Vector._from_storage(storage, Schema(date, True))

    assert list(v == datetime(1970, 1, 1)) == [None, True]


# ---------------------------------------------------------------------------
# Declines: the boxed path keeps Python's exact answers
# ---------------------------------------------------------------------------

def test_datetime_column_vs_date_stays_python_false(backend):
    v = Vector(STAMPS)

    assert list(v == date(2024, 1, 2)) == [False, None, False, False, False]


def test_aware_scalar_never_equals_naive(backend):
    v = Vector(STAMPS)
    aware = datetime(2024, 1, 2, tzinfo=timezone.utc)

    assert list(v == aware) == [False, None, False, False, False]


def test_date_subclass_scalar_compares_by_value(backend):
    v = Vector(DATES)

    assert list(v == _SubDate(2024, 1, 2)) == [True, None, False, False, True]


# ---------------------------------------------------------------------------
# is_in
# ---------------------------------------------------------------------------

def _expected_in(values, members):
    match_null = any(m is None for m in members)
    return [
        (True if match_null else None) if x is None
        else any(m is not None and x == m for m in members)
        for x in values
    ]


@pytest.mark.parametrize(
    "values,members",
    [
        (DATES, [date(2024, 1, 2), date(1969, 12, 31)]),
        (DATES, [date(1970, 1, 1), None]),
        (DATES, []),
        (STAMPS, [datetime(2024, 1, 2), datetime(2024, 1, 1, 12, fold=1)]),
        (STAMPS, [datetime(2024, 1, 2, tzinfo=timezone.utc),
                  datetime(1969, 12, 31, 23, 59, 59, 999999), None]),
    ],
    ids=['dates', 'dates-null', 'empty', 'datetimes-fold', 'aware-dropped'],
)
def test_is_in_matches_python(backend, no_boxing, values, members):
    v = Vector(values)
    expected = _expected_in(values, members)

    with no_boxing():
        result = v.is_in(members)

    assert list(result) == expected


def test_is_in_dead_member_still_warns(backend):
    v = Vector(DATES)

    with pytest.warns(UserWarning, match='can never match'):
        result = v.is_in([datetime(2024, 1, 2)])

    assert list(result) == [False, None, False, False, False]


def test_is_in_subclass_member_declines_to_python(backend):
    v = Vector(DATES)
    members = [_SubDate(2024, 1, 2)]

    assert list(v.is_in(members)) == _expected_in(DATES, members)
