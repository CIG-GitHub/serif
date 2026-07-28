"""
v.is_in(group): element-wise membership (docs/null-semantics.md).

Membership is Python ==. None among the members names absence itself:
null positions match and the mask is total. Without it: unknown in,
unknown out. Members whose type can never match the vector's dtype warn —
they match nothing, and a dead member is usually a typo'd group.
"""

import warnings
from datetime import date
from datetime import datetime

import pytest

from serif import Table
from serif import Vector
from serif import SerifTypeError


# ---------------------------------------------------------------------------
# Membership is Python ==
# ---------------------------------------------------------------------------

def test_basic_membership():
    v = Vector(['a', 'b', 'c'])
    assert list(v.is_in(['a', 'f'])) == [True, False, False]


def test_membership_is_python_equality():
    # The numeric tower: 2 == 2.0 and 1 == True, exactly as in Python.
    assert list(Vector([1, 2, 3]).is_in([2.0])) == [False, True, False]
    assert list(Vector([1, 2, 3]).is_in([True])) == [True, False, False]


def test_empty_group_matches_nothing():
    assert list(Vector([1, 2]).is_in([])) == [False, False]


def test_result_is_bool_vector():
    result = Vector([1, 2]).is_in([1])
    assert result.schema().kind is bool


def test_vector_as_group():
    v = Vector([1, 2, 3])
    keys = Vector([2, 3, 4])
    assert list(v.is_in(keys)) == [False, True, True]


def test_generator_group():
    v = Vector([1, 2, 3])
    assert list(v.is_in(x for x in [2])) == [False, True, False]


# ---------------------------------------------------------------------------
# Nulls: unknown out, unless None in the group names absence
# ---------------------------------------------------------------------------

def test_null_positions_are_unknown_without_none():
    v = Vector([1, None, 3])
    result = v.is_in([1, 3])
    assert list(result) == [True, None, True]
    assert result.schema().nullable is True


def test_none_in_group_claims_the_nulls():
    v = Vector([1, None, 3])
    result = v.is_in([1, None])
    assert list(result) == [True, True, False]
    schema = result.schema()
    assert schema.kind is bool
    assert schema.nullable is False  # explicit absence collapses unknowns


def test_none_in_group_is_sugar_for_or_is_na():
    v = Vector([1, None, 3, None, 5])
    sugar = v.is_in([1, None])
    spelled = v.is_in([1]) | v.is_na()
    assert list(sugar) == list(spelled)


def test_vector_group_with_nulls_claims_nulls():
    # Uniform rule: a null member names absence wherever it came from;
    # dropna() the group if that is not what you meant.
    v = Vector([1, None])
    keys = Vector([1, None])
    assert list(v.is_in(keys)) == [True, True]


def test_filtering_with_is_in():
    v = Vector([1, None, 3, 4])
    assert list(v[v.is_in([1, 4])]) == [1, 4]        # null row excluded
    assert list(v[v.is_in([1, None])]) == [1, None]  # null row claimed


# ---------------------------------------------------------------------------
# Dead-typed members warn: correct answer, likely surprise
# ---------------------------------------------------------------------------

def test_dead_typed_member_warns_and_matches_nothing():
    v = Vector(['a', 'b'])
    with pytest.warns(UserWarning, match='never match'):
        result = v.is_in(['a', 7])
    assert list(result) == [True, False]


def test_live_members_do_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert list(Vector([1, 2]).is_in([2.0, 5])) == [False, True]


def test_none_member_does_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Vector([1, None]).is_in([1, None])


def test_datetime_member_never_matches_date_vector():
    # datetime subclasses date but never equals a pure date.
    v = Vector([date(2025, 1, 31)])
    with pytest.warns(UserWarning, match='never match'):
        result = v.is_in([datetime(2025, 1, 31)])
    assert list(result) == [False]


# ---------------------------------------------------------------------------
# Bad groups puke
# ---------------------------------------------------------------------------

def test_single_string_group_raises():
    with pytest.raises(SerifTypeError, match='iterable'):
        Vector(['a']).is_in('abc')


def test_scalar_group_raises():
    with pytest.raises(SerifTypeError, match='iterable'):
        Vector([1]).is_in(7)


# ---------------------------------------------------------------------------
# Categorical and Table surfaces
# ---------------------------------------------------------------------------

def test_categorical_is_in():
    c = Vector(['b', None, 'a']).categorize(['a', 'b'])
    assert list(c.is_in(['b'])) == [True, None, False]
    assert list(c.is_in(['b', None])) == [True, True, False]


def test_table_is_in_lifts_per_column():
    t = Table({'a': [1, 2], 'b': [2, 3]})
    result = t.is_in([2])
    assert list(result.a) == [False, True]
    assert list(result.b) == [True, False]


def test_table_is_in_survives_a_generator_group():
    t = Table({'a': [1, 2], 'b': [2, 3]})
    result = t.is_in(x for x in [2])
    assert list(result.a) == [False, True]
    assert list(result.b) == [True, False]
