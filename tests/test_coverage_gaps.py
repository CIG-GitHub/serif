"""
Coverage for public API that previously had zero tests: Vector.new,
bit_lshift/bit_rshift, eomonth, the before/after string helpers, to_dict,
plain-Vector matmul, full_join depth, and the deprecated rename().
"""

from datetime import date

import pytest

from serif import Vector, Table, Schema, SerifValueError


# ---------------------------------------------------------------------------
# Vector.new
# ---------------------------------------------------------------------------

def test_vector_new_repeats_default_element():
    v = Vector.new(0, 3)
    assert list(v) == [0, 0, 0]
    assert v.schema() == Schema(int, False)


def test_vector_new_str():
    v = Vector.new('x', 2)
    assert list(v) == ['x', 'x']
    assert v.schema().kind is str


def test_vector_new_zero_length_keeps_dtype():
    v = Vector.new(0, 0)
    assert len(v) == 0
    assert v.schema() == Schema(int, False)


# ---------------------------------------------------------------------------
# bit_lshift / bit_rshift (the actual bit ops; << and >> are overridden)
# ---------------------------------------------------------------------------

def test_bit_lshift():
    assert list(Vector([1, 2]).bit_lshift(2)) == [4, 8]


def test_bit_rshift():
    assert list(Vector([8, 4]).bit_rshift(1)) == [4, 2]


def test_bit_shift_propagates_null():
    assert list(Vector([1, None]).bit_lshift(1)) == [2, None]


# ---------------------------------------------------------------------------
# _Date.eomonth
# ---------------------------------------------------------------------------

def test_eomonth_leap_year_and_null():
    v = Vector([date(2024, 2, 10), date(2023, 2, 10), None])
    assert list(v.eomonth()) == [date(2024, 2, 29), date(2023, 2, 28), None]


def test_eomonth_december():
    assert list(Vector([date(2024, 12, 1)]).eomonth()) == [date(2024, 12, 31)]


# ---------------------------------------------------------------------------
# _String before/after helpers
# ---------------------------------------------------------------------------

def test_before_after_first_separator():
    v = Vector(['a-b-c', 'x-y', None])
    assert list(v.before('-')) == ['a', 'x', None]
    assert list(v.after('-')) == ['b-c', 'y', None]


def test_before_after_last_separator():
    v = Vector(['a-b-c'])
    assert list(v.before_last('-')) == ['a-b']
    assert list(v.after_last('-')) == ['c']


def test_before_missing_separator_returns_whole_string():
    # str.partition puts the whole string in [0] when sep is absent
    assert list(Vector(['abc']).before('-')) == ['abc']
    assert list(Vector(['abc']).after('-')) == ['']


# ---------------------------------------------------------------------------
# Table.to_dict
# ---------------------------------------------------------------------------

def test_to_dict_named_columns():
    t = Table({'x': [1, 2], 'y': [3, 4]})
    assert t.to_dict() == {'x': [1, 2], 'y': [3, 4]}


def test_to_dict_unnamed_column_positional_key():
    # Same col{i}_ spelling as attribute access (t.col0_)
    t = Table([Vector([1, 2])])
    assert t.to_dict() == {'col0_': [1, 2]}


# ---------------------------------------------------------------------------
# Vector @ Vector (dot product)
# ---------------------------------------------------------------------------

def test_vector_matmul_dot_product():
    assert Vector([1, 2, 3]) @ Vector([4, 5, 6]) == 32


def test_vector_matmul_length_mismatch_raises():
    with pytest.raises(ValueError, match="Length mismatch"):
        Vector([1, 2]) @ Vector([1, 2, 3])


# ---------------------------------------------------------------------------
# full_join depth: multi-key and cardinality flags
# ---------------------------------------------------------------------------

def _fj_tables():
    t1 = Table({'k1': [1, 1, 2], 'k2': ['a', 'b', 'a'], 'v': [10, 20, 30]})
    t2 = Table({'k1': [1, 2, 3], 'k2': ['a', 'a', 'z'], 'w': [100, 200, 300]})
    return t1, t2


def test_full_join_multi_key():
    t1, t2 = _fj_tables()
    j = t1.full_join(t2, ['k1', 'k2'], ['k1', 'k2'])

    assert list(j['k1']) == [1, 1, 2, None]
    assert list(j['k2']) == ['a', 'b', 'a', None]
    assert list(j['v']) == [10, 20, 30, None]
    assert list(j['w']) == [100, None, 200, 300]
    # Same-named right key columns are dropped
    assert j.column_names() == ['k1', 'k2', 'v', 'w']


def test_full_join_expect_right_unique_violation():
    t1 = Table({'k': [1]})
    t2 = Table({'k': [1, 1], 'w': [5, 6]})
    with pytest.raises(SerifValueError, match="expect_right_unique"):
        t1.full_join(t2, 'k', 'k', expect_right_unique=True)


def test_full_join_expect_left_unique_violation():
    t1 = Table({'k': [1, 1]})
    t2 = Table({'k': [1], 'w': [5]})
    with pytest.raises(SerifValueError, match="expect_left_unique"):
        t1.full_join(t2, 'k', 'k', expect_left_unique=True)


# ---------------------------------------------------------------------------
# Deprecated rename()
# ---------------------------------------------------------------------------

def test_rename_warns_deprecation_and_still_renames():
    v = Vector([1, 2], name='old')
    with pytest.warns(DeprecationWarning, match="rename"):
        result = v.rename('new')
    assert result is v
    assert v.name == 'new'
