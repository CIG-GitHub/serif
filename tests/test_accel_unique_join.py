"""Conformance for right-unique direct-address and string-hash probes."""

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("pyarrow")

from serif import Table
from serif._accel import arrow as bridge
from serif._accel import join as join_mod
from serif.errors import SerifValueError


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


def _without_dense_probe(fn):
    saved = join_mod.probe_int64_dense
    join_mod.probe_int64_dense = lambda *args, **kwargs: None
    try:
        return fn()
    finally:
        join_mod.probe_int64_dense = saved


def _without_string_hash_probe(fn):
    saved = bridge.join_probe_strings_hash
    bridge.join_probe_strings_hash = lambda *args, **kwargs: None
    try:
        return fn()
    finally:
        bridge.join_probe_strings_hash = saved


@pytest.mark.parametrize('flavor', ['inner_join', 'left_join', 'full_join'])
def test_dense_int_probe_conforms(flavor):
    def run():
        left = Table({'key': [3, 1, 2, 4], 'value': [30, 10, 20, 40]})
        right = Table({'key': [2, 3, 5], 'other': [200, 300, 500]})
        return getattr(left, flavor)(right, 'key', 'key')

    _assert_tables_identical(_without_dense_probe(run), run())


@pytest.mark.parametrize('flavor', ['inner_join', 'left_join', 'full_join'])
def test_string_hash_probe_conforms(flavor):
    def run():
        left = Table({
            'key': ['c', 'a', 'b', 'd'],
            'value': [30, 10, 20, 40],
        })
        right = Table({
            'key': ['b', 'c', 'e'],
            'other': [200, 300, 500],
        })
        return getattr(left, flavor)(right, 'key', 'key')

    _assert_tables_identical(_without_string_hash_probe(run), run())


def test_dense_take_arrays_preserve_join_order():
    left = Table({'key': [3, 1, 2, 4]})
    right = Table({'key': [2, 3, 5]})

    inner = join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False)
    assert inner[0] == 'ok'
    assert inner[1].tolist() == [0, 2]
    assert inner[2].tolist() == [1, 0]

    full = join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, True, True)
    assert full[1].tolist() == [0, 1, 2, 3, -1]
    assert full[2].tolist() == [1, -1, 0, -1, 2]


def test_signed_extreme_compact_range_uses_dense_probe():
    low = -2**63
    left = Table({'key': [low + 2, low, low + 1]})
    right = Table({'key': [low + 1, low + 2]})
    result = join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False)
    assert result[1].tolist() == [0, 2]
    assert result[2].tolist() == [1, 0]


def test_sparse_int_range_declines():
    left = Table({'key': [0, 10**9]})
    right = Table({'key': [0, 10**9]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False) is None


def test_duplicate_right_declines_to_diagnostic_path():
    left = Table({'key': [1, 2]})
    right = Table({'key': [1, 1, 2]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False) is None

    def run():
        return left.inner_join(right, 'key', 'key')

    with pytest.raises(SerifValueError) as expected:
        _without_dense_probe(run)
    with pytest.raises(SerifValueError) as actual:
        run()
    assert str(actual.value) == str(expected.value)


def test_left_uniqueness_violation_declines():
    left = Table({'key': [1, 2, 1]})
    right = Table({'key': [1, 2]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        True, True, False, False) is None


def test_nullable_and_nonunique_right_modes_decline():
    left = Table({'key': [1, None, 2]})
    right = Table({'key': [1, 2]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False) is None

    dense = Table({'key': [1, 2]})
    assert join_mod.probe_int64_dense(
        dense.key._storage, right.key._storage,
        False, False, False, False) is None


def test_dense_and_string_fast_paths_engage(monkeypatch):
    dense_calls = []
    string_calls = []
    dense_original = join_mod.probe_int64_dense
    string_original = bridge.join_probe_strings_hash

    def dense_spy(*args, **kwargs):
        result = dense_original(*args, **kwargs)
        dense_calls.append(result is not None)
        return result

    def string_spy(*args, **kwargs):
        result = string_original(*args, **kwargs)
        string_calls.append(result is not None)
        return result

    monkeypatch.setattr(join_mod, 'probe_int64_dense', dense_spy)
    monkeypatch.setattr(bridge, 'join_probe_strings_hash', string_spy)

    Table({'key': [1, 2]}).inner_join(
        Table({'key': [2]}), 'key', 'key')
    Table({'key': ['a', 'b']}).inner_join(
        Table({'key': ['b']}), 'key', 'key')

    assert dense_calls == [True, False]
    assert string_calls == [True]
