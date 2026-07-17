"""
Row-as-Vector conformance.

Row's docstring promises it "behaves like a Vector (math, logic,
isinstance)" while staying a zero-copy view into the Table's columns.
These tests hold it to that promise: every base Vector behavior must work
on a Row exactly as it would on the materialized tuple of row values.
"""

from datetime import date

import pytest

from serif import Table, Vector
from serif.errors import SerifTypeError
from serif.table import Row


@pytest.fixture
def num_t():
    return Table({'a': [1, 2, 3], 'b': [10, 20, 30], 'c': [100, 200, 300]})


@pytest.fixture
def null_t():
    # Nulls placed after a leading value so column inference stays int.
    return Table({'a': [1, 2], 'b': [5, None]})


@pytest.fixture
def mixed_t():
    return Table({'n': [1, 2], 's': ['x', 'y']})


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def test_row_sum(num_t):
    assert num_t[0].sum() == 111


def test_row_mean(num_t):
    assert num_t[0].mean() == 37


def test_row_min_max(num_t):
    assert num_t[1].min() == 2
    assert num_t[1].max() == 200


def test_row_count_skips_nulls(null_t):
    assert null_t[1].count() == 1


def test_row_sum_skips_nulls(null_t):
    assert null_t[1].sum() == 2


def test_row_aggregation_per_iteration(num_t):
    # The mutable-iterator pattern reuses one Row object; aggregation must
    # reflect the CURRENT index each time.
    assert [r.sum() for r in num_t] == [111, 222, 333]


def test_row_sum_on_mixed_types_raises(mixed_t):
    # Python semantics: 1 + 'x' is a TypeError, not a silent result.
    with pytest.raises(TypeError):
        mixed_t[0].sum()


# ---------------------------------------------------------------------------
# Arithmetic
# ---------------------------------------------------------------------------

def test_row_add_scalar(num_t):
    result = num_t[1] + 1
    assert list(result) == [3, 21, 201]
    assert isinstance(result, Vector)
    assert not isinstance(result, Row)


def test_row_mul_scalar(num_t):
    assert list(num_t[0] * 2) == [2, 20, 200]


def test_row_add_row(num_t):
    assert list(num_t[0] + num_t[1]) == [3, 30, 300]


def test_row_sub_row(num_t):
    assert list(num_t[1] - num_t[0]) == [1, 10, 100]


def test_row_neg(num_t):
    assert list(-num_t[0]) == [-1, -10, -100]


def test_row_arithmetic_skips_nulls(null_t):
    assert list(null_t[1] + 1) == [3, None]


def test_row_dot_product(num_t):
    assert num_t[0] @ num_t[0] == 1 + 100 + 10000


# ---------------------------------------------------------------------------
# Comparisons / logic
# ---------------------------------------------------------------------------

def test_row_compare_scalar(num_t):
    assert list(num_t[0] > 5) == [False, True, True]


def test_row_eq_row(num_t):
    assert list(num_t[0] == num_t[0]) == [True, True, True]


def test_row_eq_list(num_t):
    assert list(num_t[0] == [1, 10, 100]) == [True, True, True]


def test_row_compare_nulls_are_null(null_t):
    # Unknown in, unknown out (docs/null-semantics.md).
    assert list(null_t[1] == null_t[1]) == [True, None]


def test_row_isna(null_t):
    assert list(null_t[1].isna()) == [False, True]


def test_row_isinstance(num_t):
    assert list(num_t[0].is_type(int)) == [True, True, True]


# ---------------------------------------------------------------------------
# Access / iteration / slicing
# ---------------------------------------------------------------------------

def test_row_unpacking(num_t):
    a, b, c = num_t[0]
    assert (a, b, c) == (1, 10, 100)


def test_row_getitem_int_str_attr(num_t):
    row = num_t[1]
    assert row[0] == 2
    assert row['b'] == 20
    assert row.c == 200


def test_row_slice_returns_vector(num_t):
    result = num_t[0][1:]
    assert list(result) == [10, 100]
    assert isinstance(result, Vector)


def test_row_boolean_mask(num_t):
    assert list(num_t[0][[True, False, True]]) == [1, 100]


def test_row_sort(num_t):
    assert list(num_t[0].sort_by(reverse=True)) == [100, 10, 1]


def test_row_repr(num_t):
    assert repr(num_t[0]).startswith('Row(')


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_row_schema_homogeneous(num_t):
    schema = num_t[0].schema()
    assert schema.kind is int
    assert schema.nullable is False


def test_row_schema_nullable_if_any_column_nullable(null_t):
    assert null_t[0].schema().nullable is True


def test_row_schema_mixed_is_object(mixed_t):
    assert mixed_t[0].schema().kind is object


# ---------------------------------------------------------------------------
# Dtype-proxied methods
# ---------------------------------------------------------------------------

def test_row_str_method_proxy():
    t = Table({'a': ['hello'], 'b': ['world']})
    assert list(t[0].upper()) == ['HELLO', 'WORLD']


def test_row_date_property_proxy():
    t = Table({'a': [date(2024, 3, 1)], 'b': [date(2025, 7, 2)]})
    assert list(t[0].year) == [2024, 2025]


# ---------------------------------------------------------------------------
# Read-only contract
# ---------------------------------------------------------------------------

def test_row_setitem_raises(num_t):
    with pytest.raises(SerifTypeError):
        num_t[0][0] = 99


def test_row_setitem_error_points_to_table_assignment(num_t):
    with pytest.raises(SerifTypeError, match=r't\[row_index, col\]'):
        num_t[0]['a'] = 99
