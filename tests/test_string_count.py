"""
String-vector count(): arity-dispatched dual role.

count() with no arguments is the Vector aggregate (non-null count) — the
same meaning as on every other dtype, and required for Table.count() and
count-per-group aggregations to work on tables containing string columns.
count(sub[, start[, end]]) is per-element str.count. Zero-arg str.count has
never existed in Python, so the aggregate meaning collides with nothing.
"""

import pytest

from serif import Table, Vector


def test_zero_arg_count_is_nonnull_aggregate():
    assert Vector(['a', 'b', None]).count() == 2


def test_zero_arg_count_all_present():
    assert Vector(['a', 'b']).count() == 2


def test_substring_count_is_elementwise():
    assert list(Vector(['banana', 'cabana']).count('an')) == [2, 1]


def test_substring_count_propagates_none():
    assert list(Vector(['aa', None]).count('a')) == [2, None]


def test_substring_count_with_start_arg():
    assert list(Vector(['banana']).count('a', 2)) == [2]


def test_table_count_with_string_column():
    t = Table({'g': ['a', 'b', 'c'], 's': ['x', None, 'z']})
    assert list(t.count()) == [3, 2]


def test_aggregate_count_per_group_on_string_column():
    t = Table({'g': ['a', 'a', 'b'], 's': ['x', None, 'z']})
    result = t.aggregate('g', {'n': t.s.count})
    assert list(result.g) == ['a', 'b']
    assert list(result.n) == [1, 1]
