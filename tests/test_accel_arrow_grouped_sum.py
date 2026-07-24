"""Conformance for Arrow's fused hash-grouped bound-sum fast path."""

import math
import warnings

import pytest

pytest.importorskip("pyarrow")

from serif import Table
from serif import Vector
from serif._execution import DECLINED
from serif._table._arrow import aggregation as arrow_aggregation
from serif._vector import Schema
from serif._vector.storage import ArrayStorage
from serif._vector.storage import StringStorage


def _without_arrow(fn):
    saved = arrow_aggregation._USE_ARROW
    arrow_aggregation._USE_ARROW = False
    try:
        return fn()
    finally:
        arrow_aggregation._USE_ARROW = saved


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
        for left, right in zip(expected_col, actual_col):
            if isinstance(left, float) and math.isnan(left):
                assert isinstance(right, float) and math.isnan(right)
            else:
                assert right == left
                assert type(right) is type(left)


def _source(vector):
    return vector.schema(), vector._storage


def test_nullable_int_sum_and_first_appearance_conform():
    def run():
        table = Table({
            'group': [3, 1, 3, 2, 1],
            'value': [1, 2, None, 4, 5],
        })
        return table.aggregate('group', {'total': table.value.sum})

    expected = _without_arrow(run)
    actual = run()
    _assert_tables_identical(expected, actual)
    assert list(actual.group) == [3, 1, 2]
    assert list(actual.total) == [1, 7, 4]


def test_fast_and_fallback_warning_behavior_matches():
    def run():
        table = Table({'group': [1, 2, 1], 'value': [3, 4, 5]})
        return table.aggregate('group', {'total': table.value.sum})

    with warnings.catch_warnings(record=True) as fast_warnings:
        warnings.simplefilter('always')
        run()
    with warnings.catch_warnings(record=True) as fallback_warnings:
        warnings.simplefilter('always')
        _without_arrow(run)

    assert [str(item.message) for item in fast_warnings] == []
    assert [str(item.message) for item in fallback_warnings] == []


def test_multiple_bound_sums_share_one_grouping():
    def run():
        table = Table({
            'group': ['b', 'a', 'b', 'c'],
            'x': [1, 2, 3, 4],
            'y': [10.0, None, 30.0, 40.0],
        })
        return table.aggregate(
            'group', {'sx': table.x.sum, 'sy': table.y.sum})

    _assert_tables_identical(_without_arrow(run), run())


def test_physical_result_returns_string_key_storage():
    table = Table({
        'group': ['b', 'a', 'b'],
        'x': [1, 2, 3],
        'y': [1.5, None, 2.5],
    })
    result = arrow_aggregation.grouped_sums(
        _source(table.group),
        [_source(table.x), _source(table.y)],
    )

    assert result is not DECLINED
    (key_schema, keys), columns = result
    assert key_schema.kind is str
    assert key_schema.nullable is False
    assert isinstance(keys, StringStorage)
    assert list(keys) == ['b', 'a']
    assert [values for _, values in columns] == [[4, 2], [4.0, 0]]
    assert all(type(key) is str for key in keys)
    assert all(
        type(value) in (int, float)
        for _, values in columns
        for value in values
    )


def test_physical_result_returns_int64_key_storage():
    table = Table({
        'group': [3, 1, 3, 2],
        'value': [1, 2, 3, 4],
    })
    result = arrow_aggregation.grouped_sums(
        _source(table.group),
        [_source(table.value)],
    )

    assert result is not DECLINED
    (key_schema, keys), _ = result
    assert key_schema.kind is int
    assert key_schema.nullable is False
    assert isinstance(keys, ArrayStorage)
    assert keys._data.typecode == 'q'
    assert list(keys) == [3, 1, 2]
    assert all(type(key) is int for key in keys)


def test_key_schema_name_and_collision_survive_storage_return():
    table = Table([
        Vector(
            [2, 1, 2],
            dtype=Schema(int, True),
            name='key',
        ),
        Vector([3, 4, 5], name='value'),
    ])

    result = table.aggregate('key', {'key': table.value.sum})

    assert result.column_names() == ['key', 'key2']
    assert isinstance(result.key._storage, ArrayStorage)
    assert result.key.schema().kind is int
    assert result.key.schema().nullable is True
    assert list(result.key) == [2, 1]
    assert all(type(key) is int for key in result.key)


def test_all_null_group_retains_sum_identity():
    def run():
        table = Table({
            'group': [1, 1, 2],
            'value': [None, None, 3],
        })
        return table.aggregate('group', {'total': table.value.sum})

    actual = run()
    _assert_tables_identical(_without_arrow(run), actual)
    assert list(actual.total) == [0, 3]


def test_narrow_spread_sum_reconstructs_bigint():
    def run():
        table = Table({
            'group': [1, 1, 2],
            'value': [2**62, 2**62, 7],
        })
        return table.aggregate('group', {'total': table.value.sum})

    actual = run()
    _assert_tables_identical(_without_arrow(run), actual)
    assert actual.total[0] == 2**63


def test_ambiguous_int_group_declines():
    table = Table({
        'group': [1, 1],
        'value': [-2**63, 2**63 - 1],
    })
    assert arrow_aggregation.grouped_sums(
        _source(table.group),
        [_source(table.value)],
    ) is DECLINED
    _assert_tables_identical(
        _without_arrow(lambda: table.aggregate(
            'group', {'total': table.value.sum})),
        table.aggregate('group', {'total': table.value.sum}),
    )


def test_unsupported_key_storages_decline():
    nullable = Table({'group': [1, None], 'value': [2, 3]})
    floating = Table({'group': [1.0, 2.0], 'value': [2, 3]})

    assert arrow_aggregation.grouped_sums(
        _source(nullable.group),
        [_source(nullable.value)],
    ) is DECLINED
    assert arrow_aggregation.grouped_sums(
        _source(floating.group),
        [_source(floating.value)],
    ) is DECLINED


@pytest.mark.parametrize('aggregation', ['callable', 'other_method'])
def test_unrecognized_aggregation_declines(aggregation, monkeypatch):
    calls = []
    original = arrow_aggregation.grouped_sums

    def spy(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(arrow_aggregation, 'grouped_sums', spy)
    table = Table({'group': [1, 1, 2], 'value': [1, 2, 3]})
    if aggregation == 'callable':
        table.aggregate('group', {'total': lambda group: group.value.sum()})
    else:
        table.aggregate('group', {'peak': table.value.max})
    assert calls == []


def test_fast_path_engages(monkeypatch):
    calls = []
    original = arrow_aggregation.grouped_sums

    def spy(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(arrow_aggregation, 'grouped_sums', spy)
    table = Table({'group': [1, 2, 1], 'value': [1, None, 3]})
    table.aggregate('group', {'total': table.value.sum})
    assert calls == [True]
