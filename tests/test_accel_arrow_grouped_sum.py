"""Conformance for Arrow's fused hash-grouped bound-sum fast path."""

import math

import pytest

pytest.importorskip("pyarrow")

from serif import Table
from serif._accel import arrow as bridge


def _without_arrow(fn):
    saved = bridge._USE_ARROW
    bridge._USE_ARROW = False
    try:
        return fn()
    finally:
        bridge._USE_ARROW = saved


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
    assert bridge.grouped_sums(
        table.group._storage, [table.value._storage]) is None
    _assert_tables_identical(
        _without_arrow(lambda: table.aggregate(
            'group', {'total': table.value.sum})),
        table.aggregate('group', {'total': table.value.sum}),
    )


@pytest.mark.parametrize('aggregation', ['callable', 'other_method'])
def test_unrecognized_aggregation_declines(aggregation, monkeypatch):
    calls = []
    original = bridge.grouped_sums

    def spy(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(bridge, 'grouped_sums', spy)
    table = Table({'group': [1, 1, 2], 'value': [1, 2, 3]})
    if aggregation == 'callable':
        table.aggregate('group', {'total': lambda group: group.value.sum()})
    else:
        table.aggregate('group', {'peak': table.value.max})
    assert calls == []


def test_fast_path_engages(monkeypatch):
    calls = []
    original = bridge.grouped_sums

    def spy(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(result is not None)
        return result

    monkeypatch.setattr(bridge, 'grouped_sums', spy)
    table = Table({'group': [1, 2, 1], 'value': [1, None, 3]})
    table.aggregate('group', {'total': table.value.sum})
    assert calls == [True]
