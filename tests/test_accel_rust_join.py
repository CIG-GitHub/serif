"""
Conformance tests for the OPTIONAL Rust single-key int64 join probe
(serif._table._rust.joins.probe_int64).

The guarantee under test: every join flavor and cardinality setting returns
an IDENTICAL table (row order, values, schemas, storage types) and raises
the IDENTICAL diagnostic whether the Rust probe runs or every accelerator
is off. Unlike the NumPy probes, the Rust probe also takes nullable keys
(nulls never match) and many-to-many right sides, so those are pinned here.

Skipped entirely when the extension isn't built.
"""

import random

import pytest

pytest.importorskip("serif._rust")

from serif import Table
from serif import Vector
from serif._execution import DECLINED
from serif._table._numpy import joins as numpy_joins
from serif._table._rust import joins as rust_joins
from serif._vector._numpy import selection as numpy_selection
from serif._vector.dtype import Schema
from serif.errors import SerifValueError


I64_MAX = 2**63 - 1
I64_MIN = -2**63

FLAVORS = ['inner_join', 'left_join', 'full_join']
CARDINALITY = [
    (False, False),
    (False, True),
    (True, False),
    (True, True),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_pure(fn, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(rust_joins, '_USE_RUST', False)
        m.setattr(numpy_joins, '_USE_NUMPY', False)
        m.setattr(numpy_selection, '_USE_NUMPY', False)
        return fn()


def _assert_tables_identical(expected, actual):
    assert actual.column_names() == expected.column_names()
    assert len(actual) == len(expected)
    for expected_col, actual_col in zip(expected.cols(), actual.cols()):
        assert type(actual_col._storage) is type(expected_col._storage)
        expected_schema = expected_col.schema()
        actual_schema = actual_col.schema()
        if expected_schema is None:
            assert actual_schema is None
        else:
            assert actual_schema.kind is expected_schema.kind
            assert actual_schema.nullable is expected_schema.nullable
        assert list(actual_col) == list(expected_col)


def _outcome(fn):
    try:
        return 'ok', fn()
    except SerifValueError as error:
        return 'error', str(error)


def _conform(make, monkeypatch):
    expected = _outcome(lambda: _all_pure(make, monkeypatch))
    actual = _outcome(make)
    assert actual[0] == expected[0], (expected, actual)
    if expected[0] == 'error':
        assert actual[1] == expected[1]
    else:
        _assert_tables_identical(expected[1], actual[1])


def _join(flavor, left_keys, right_keys, expect_left_unique, expect_right_unique):
    # Explicit schema: empty or all-null keys would otherwise infer object.
    int_keys = lambda keys: Vector(keys, dtype=Schema(int, None in keys))

    def make():
        left = Table({
            'key': int_keys(left_keys),
            'lv': [f'l{i}' for i in range(len(left_keys))],
        })
        right = Table({
            'key': int_keys(right_keys),
            'rv': list(range(len(right_keys))),
        })
        return getattr(left, flavor)(
            right,
            'key',
            'key',
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
        )
    return make


@pytest.fixture
def engaged(monkeypatch):
    """Record whether each Rust probe accelerated (True) or declined."""
    calls = []
    orig = rust_joins.probe_int64

    def spy(*args):
        result = orig(*args)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(rust_joins, 'probe_int64', spy)
    return calls


# ---------------------------------------------------------------------------
# Conformance
# ---------------------------------------------------------------------------

CASES = {
    'unique': ([3, 1, 2, 4], [2, 3, 5]),
    'many_to_many': ([2, 1, 2, 7], [2, 9, 2, 1, 2]),
    'left_dups': ([5, 6, 5, 6, 8], [6, 5]),
    'nullable_both': ([None, 1, 2, None, 3], [3, None, 1, None]),
    'all_null_left': ([None, None], [1, 2]),
    'sparse_extremes': ([I64_MIN, 0, I64_MAX, -1], [I64_MAX, 5, I64_MIN]),
    'empty_left': ([], [1, 2]),
    'empty_right': ([1, 2], []),
    'no_overlap': ([1, 2, 3], [4, 5, 6]),
}


@pytest.mark.parametrize("flavor", FLAVORS)
@pytest.mark.parametrize("cardinality", CARDINALITY, ids=lambda c: f"lu={c[0]},ru={c[1]}")
@pytest.mark.parametrize("case", list(CASES))
def test_conforms(flavor, cardinality, case, monkeypatch):
    left_keys, right_keys = CASES[case]
    make = _join(flavor, left_keys, right_keys, *cardinality)
    _conform(make, monkeypatch)


@pytest.mark.parametrize("flavor", FLAVORS)
def test_randomized_against_python(flavor, monkeypatch):
    rng = random.Random(20261015)
    for _ in range(30):
        def column(n, spread):
            return [
                None if rng.random() < 0.1 else rng.randint(-spread, spread)
                for _ in range(n)
            ]
        spread = rng.choice([5, 50, 2**40])
        left_keys = column(rng.randint(0, 60), spread)
        right_keys = column(rng.randint(0, 60), spread)
        for cardinality in CARDINALITY:
            _conform(_join(flavor, left_keys, right_keys, *cardinality), monkeypatch)


# ---------------------------------------------------------------------------
# Engagement and diagnostics
# ---------------------------------------------------------------------------

def test_engages_where_numpy_declines(engaged):
    # Nullable keys, many-to-many right, sparse keys: NumPy declines all.
    _join('left_join', [None, 1, 2], [1, None], False, True)()
    _join('inner_join', [1, 2], [1, 1, 2], False, False)()
    _join('inner_join', [2**60, -2**60], [2**60], False, True)()
    assert engaged == [True, True, True]


def test_right_duplicate_diagnostic(engaged):
    make = _join('inner_join', [1], [4, 9, 9, 4, 4], False, True)
    with pytest.raises(SerifValueError, match=r"\(9,\).*appears 2 times"):
        make()
    assert engaged == [True]


def test_left_duplicate_diagnostic(engaged):
    make = _join('inner_join', [3, 8, None, None, 8, 3], [3], True, True)
    with pytest.raises(SerifValueError, match=r"duplicate key \(8,\)"):
        make()
    assert engaged == [True]


def test_null_keys_are_not_left_duplicates(monkeypatch, engaged):
    make = _join('left_join', [None, None, 1], [1], True, True)
    _conform(make, monkeypatch)
    assert list(make().rv) == [None, None, 0]


def test_rust_take_arrays_drive_python_gather(monkeypatch, engaged):
    make = _join('full_join', [1, None, 3], [3, 4], False, True)
    expected = _all_pure(make, monkeypatch)
    monkeypatch.setattr(numpy_selection, '_USE_NUMPY', False)
    _assert_tables_identical(expected, make())
    assert engaged[-1] is True


def test_declines_non_int_keys():
    strings = Table({'k': ['a', 'b']}).k._storage
    ints = Table({'k': [1, 2]}).k._storage
    args = (False, True, False, False)
    assert rust_joins.probe_int64(strings, strings, *args) is DECLINED
    assert rust_joins.probe_int64(ints, strings, *args) is DECLINED


def test_off_switch_declines(monkeypatch):
    ints = Table({'k': [1, 2]}).k._storage
    monkeypatch.setattr(rust_joins, '_USE_RUST', False)
    assert rust_joins.probe_int64(ints, ints, False, True, False, False) is DECLINED
