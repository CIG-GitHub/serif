"""
Conformance tests for the OPTIONAL Rust stable argsort
(serif._vector._rust.sort.sort_indices) behind Table.sort_by and
Vector.sort_by.

The guarantee under test: sorting returns IDENTICAL results whether the
Rust radix sort runs or Python's own stable sort does — same row order for
ties, per-key directions, nulls first/last under both directions, and
float comparisons (-0.0 ties 0.0). NaN makes Python's order depend on the
algorithm, so the kernel must DECLINE and Python's result must surface.
Date and datetime columns sort as their int32 / int64 ticks.

Skipped entirely when the extension isn't built.
"""

import math
import random
from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest

pytest.importorskip("serif._rust")

from serif import Table
from serif import Vector
from serif._execution import DECLINED
from serif._vector._rust import sort as rust_sort
from serif._vector.storage import DateStorage


I64_MAX = 2**63 - 1
I64_MIN = -2**63


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pure(fn, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(rust_sort, '_USE_RUST', False)
        return fn()


def _same(a, b):
    if isinstance(a, float) and math.isnan(a):
        return isinstance(b, float) and math.isnan(b)
    return a == b and type(a) is type(b)


def _assert_tables_identical(expected, actual):
    assert actual.column_names() == expected.column_names()
    for expected_col, actual_col in zip(expected.cols(), actual.cols()):
        assert type(actual_col._storage) is type(expected_col._storage)
        assert actual_col.schema() == expected_col.schema()
        expected_values = list(expected_col)
        actual_values = list(actual_col)
        assert len(actual_values) == len(expected_values)
        for i, (e, a) in enumerate(zip(expected_values, actual_values)):
            assert _same(e, a), f"{actual_col._name}[{i}]: {a!r} != {e!r}"


def _conform(make, monkeypatch):
    _assert_tables_identical(_pure(make, monkeypatch), make())


def _conform_vector(make, monkeypatch):
    expected = _pure(make, monkeypatch)
    actual = make()
    assert actual.schema() == expected.schema()
    assert all(_same(e, a) for e, a in zip(list(expected), list(actual)))
    assert len(actual) == len(expected)


@pytest.fixture
def engaged(monkeypatch):
    """Record whether each Rust sort accelerated (True) or declined."""
    calls = []
    orig = rust_sort.sort_indices

    def spy(*args):
        result = orig(*args)
        if rust_sort._USE_RUST:  # skip the pure-Python comparison runs
            calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(rust_sort, 'sort_indices', spy)
    return calls


def _table(**columns):
    n = len(next(iter(columns.values())))
    return Table({**columns, 'row': list(range(n))})


INTS = [3, None, -1, 3, None, I64_MIN, I64_MAX, -1, 0, 3, None, 2**40]
FLOATS = [0.0, -0.0, 1.5, None, -math.inf, math.inf, 1.5, -2.25, 0.0, None, -0.0]
BOOLS = [True, None, False, True, False, None, True]
DATES = [date(2024, 1, 2), None, date(1969, 12, 31), date(2024, 1, 2),
         date(1, 1, 1), date(9999, 12, 31), None, date(1970, 1, 1)]
DATETIMES = [datetime(2024, 1, 2, 9, 30), None,
             datetime(1969, 12, 31, 23, 59, 59, 999999),
             datetime(2024, 1, 2, 9, 30), datetime(1, 1, 1),
             datetime(9999, 12, 31, 23, 59, 59, 999999), None]


# ---------------------------------------------------------------------------
# Conformance
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("na_last", [True, False])
@pytest.mark.parametrize(
    "values",
    [INTS, FLOATS, BOOLS, DATES, DATETIMES],
    ids=['int', 'float', 'bool', 'date', 'datetime'],
)
def test_single_key(values, reverse, na_last, monkeypatch, engaged):
    t = _table(k=values)
    _conform(lambda: t.sort_by('k', reverse=reverse, na_last=na_last), monkeypatch)
    _conform_vector(lambda: t.k.sort_by(reverse=reverse, na_last=na_last), monkeypatch)
    assert engaged.count(True) == 2


@pytest.mark.parametrize("reverse", [
    False, True, [False, True], [True, False],
], ids=str)
@pytest.mark.parametrize("na_last", [True, False])
def test_multi_key(reverse, na_last, monkeypatch, engaged):
    rng = random.Random(7)
    n = 200
    t = _table(
        g=[None if rng.random() < 0.1 else rng.randrange(5) for _ in range(n)],
        v=[None if rng.random() < 0.1 else rng.choice([-0.0, 0.0, 1.0, -3.5]) for _ in range(n)],
    )
    _conform(lambda: t.sort_by(['g', 'v'], reverse=reverse, na_last=na_last), monkeypatch)
    assert engaged[-1] is True


def test_three_mixed_kind_keys(monkeypatch, engaged):
    rng = random.Random(11)
    n = 300
    t = _table(
        b=[rng.choice([True, False, None]) for _ in range(n)],
        i=[rng.randrange(-3, 3) for _ in range(n)],
        f=[rng.choice([0.5, -0.5, None]) for _ in range(n)],
    )
    _conform(
        lambda: t.sort_by(['b', 'i', 'f'], reverse=[True, False, True], na_last=False),
        monkeypatch,
    )
    assert engaged[-1] is True


def test_date_key_combines_with_other_kinds(monkeypatch, engaged):
    # Date ticks travel as an int32 key next to int64 and float keys.
    rng = random.Random(13)
    n = 300
    t = _table(
        d=[None if rng.random() < 0.1
           else date(2024, 1, 1) + timedelta(days=rng.randrange(-3, 3))
           for _ in range(n)],
        i=[rng.randrange(-3, 3) for _ in range(n)],
        ts=[None if rng.random() < 0.1
            else datetime(2024, 1, 1) + timedelta(microseconds=rng.randrange(5))
            for _ in range(n)],
    )
    _conform(
        lambda: t.sort_by(['d', 'i', 'ts'], reverse=[True, False, True], na_last=False),
        monkeypatch,
    )
    assert engaged[-1] is True


def test_python_fallback_sorts_dates_without_boxing(monkeypatch):
    # With Rust off, both pure sorts order date columns by their ticks.
    t = _table(d=DATES)
    expected_column = sorted(
        (x for x in DATES if x is not None)) + [None] * DATES.count(None)

    def fail(tick):
        raise AssertionError('sort boxed a tick')

    with monkeypatch.context() as m:
        m.setattr(rust_sort, '_USE_RUST', False)
        m.setattr(DateStorage, '_box', staticmethod(fail))
        by_table = t.sort_by('d')
        by_vector = t.d.sort_by()

    assert list(by_table.d) == expected_column
    assert list(by_vector) == expected_column


def test_category_keys_follow_category_order(monkeypatch, engaged):
    sizes = Vector(['m', 'z', None, 'a', 'z', 'm']).categorize(['z', 'm', 'a'])
    t = Table({'c': sizes, 'row': list(range(6))})
    _conform(lambda: t.sort_by('c'), monkeypatch)
    _conform(lambda: t.sort_by(['c', 'row'], reverse=[True, True]), monkeypatch)
    assert list(t.sort_by('c').c) == ['z', 'z', 'm', 'm', 'a', None]
    assert engaged == [True, True, True]


@pytest.mark.parametrize("n", [1, 2, 255, 256, 257, 70_000])
def test_randomized_against_python(n, monkeypatch):
    rng = random.Random(n)
    spread = rng.choice([3, 1000, 2**62])
    t = _table(
        i=[None if rng.random() < 0.05 else rng.randint(-spread, spread) for _ in range(n)],
        f=[None if rng.random() < 0.05 else rng.uniform(-1e6, 1e6) for _ in range(n)],
    )
    for by, reverse in [('i', False), ('i', True), (['f', 'i'], [True, False])]:
        _conform(lambda: t.sort_by(by, reverse=reverse), monkeypatch)


# ---------------------------------------------------------------------------
# Declines
# ---------------------------------------------------------------------------

def test_nan_declines_and_python_decides(monkeypatch, engaged):
    t = _table(f=[2.0, math.nan, 1.0, None, math.nan, 0.5])
    _conform(lambda: t.sort_by('f'), monkeypatch)
    assert engaged == [False]


def test_nan_under_null_lane_still_engages(monkeypatch, engaged):
    t = _table(f=[2.0, None, 1.0])
    t.f._storage._data[1] = math.nan
    _conform(lambda: t.sort_by('f'), monkeypatch)
    assert engaged == [True]


def test_string_keys_decline_whole_sort(monkeypatch, engaged):
    t = _table(
        s=['b', 'a', None, 'a'],
        d=[date(2024, 1, 2), date(2023, 1, 1), None, date(2023, 1, 1)],
        i=[1, 2, 3, 4],
    )
    _conform(lambda: t.sort_by(['i', 's']), monkeypatch)
    _conform(lambda: t.sort_by(['d', 's']), monkeypatch)
    assert engaged == [False, False]


def test_bigint_tuple_storage_declines(monkeypatch, engaged):
    t = _table(i=[2**70, 1, -(2**70)])
    _conform(lambda: t.sort_by('i'), monkeypatch)
    assert engaged == [False]


def test_empty_declines(monkeypatch):
    t = Table({'i': Vector([1]).schema() and Vector([], dtype=Vector([1]).schema())})
    _conform(lambda: t.sort_by('i'), monkeypatch)


def test_off_switch_declines(monkeypatch):
    storage = Vector([2, 1])._storage
    monkeypatch.setattr(rust_sort, '_USE_RUST', False)
    assert rust_sort.sort_indices([storage], [False], True) is DECLINED
