"""Conformance for right-unique direct-address and string-hash probes."""

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("pyarrow")

from serif import Table, Vector
from serif._execution import DECLINED
from serif._table._arrow import joins as arrow_join_mod
from serif._table._numpy import joins as join_mod
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
    join_mod.probe_int64_dense = lambda *args, **kwargs: DECLINED
    try:
        return fn()
    finally:
        join_mod.probe_int64_dense = saved


def _without_string_hash_probe(fn):
    saved = arrow_join_mod.probe_strings_hash
    arrow_join_mod.probe_strings_hash = lambda *args, **kwargs: DECLINED
    try:
        return fn()
    finally:
        arrow_join_mod.probe_strings_hash = saved


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
    assert all(
        isinstance(indexer, np.ndarray) and indexer.dtype == np.intp
        for indexer in inner[1:]
    )
    assert inner[1].tolist() == [0, 2]
    assert inner[2].tolist() == [1, 0]

    full = join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, True, True)
    assert all(
        isinstance(indexer, np.ndarray) and indexer.dtype == np.intp
        for indexer in full[1:]
    )
    assert full[1].tolist() == [0, 1, 2, 3, -1]
    assert full[2].tolist() == [1, -1, 0, -1, 2]


def test_sorted_int_probe_returns_numpy_indexers():
    left = Table({'key': [2, 1, 2]})
    right = Table({'key': [2, 2, 1]})

    result = join_mod.probe_int64(
        left.key._storage,
        right.key._storage,
        False,
        False,
        False,
        False,
    )
    assert result[0] == 'ok'
    assert all(
        isinstance(indexer, np.ndarray) and indexer.dtype == np.intp
        for indexer in result[1:]
    )
    assert result[1].tolist() == [0, 0, 1, 2, 2]
    assert result[2].tolist() == [0, 1, 2, 0, 1]


def test_numpy_indexers_drive_object_payload_fallback():
    left = Table([
        Vector([2, 1], name='key'),
        Vector([[2], [1]], name='left_object').to_object(),
    ])
    right = Table([
        Vector([1, 3], name='key'),
        Vector([{'value': 1}, {'value': 3}], name='right_object').to_object(),
    ])

    result = left.full_join(right, 'key', 'key')

    assert list(result.key) == [2, 1, None]
    assert list(result.left_object) == [[2], [1], None]
    assert list(result.right_object) == [None, {'value': 1}, {'value': 3}]


def test_signed_extreme_compact_range_uses_dense_probe():
    low = -2**63
    left = Table({'key': [low + 2, low, low + 1]})
    right = Table({'key': [low + 1, low + 2]})
    result = join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False)
    assert all(
        isinstance(indexer, np.ndarray) and indexer.dtype == np.intp
        for indexer in result[1:]
    )
    assert result[1].tolist() == [0, 2]
    assert result[2].tolist() == [1, 0]


def test_sparse_int_range_declines():
    left = Table({'key': [0, 10**9]})
    right = Table({'key': [0, 10**9]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False) is DECLINED


def test_duplicate_right_declines_to_diagnostic_path():
    left = Table({'key': [1, 2]})
    right = Table({'key': [1, 1, 2]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False) is DECLINED

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
        True, True, False, False) is DECLINED


def test_nullable_and_nonunique_right_modes_decline():
    left = Table({'key': [1, None, 2]})
    right = Table({'key': [1, 2]})
    assert join_mod.probe_int64_dense(
        left.key._storage, right.key._storage,
        False, True, False, False) is DECLINED

    dense = Table({'key': [1, 2]})
    assert join_mod.probe_int64_dense(
        dense.key._storage, right.key._storage,
        False, False, False, False) is DECLINED


def test_dense_and_string_fast_paths_engage(monkeypatch):
    dense_calls = []
    string_calls = []
    dense_original = join_mod.probe_int64_dense
    string_original = arrow_join_mod.probe_strings_hash

    def dense_spy(*args, **kwargs):
        result = dense_original(*args, **kwargs)
        dense_calls.append(result is not DECLINED)
        return result

    def string_spy(*args, **kwargs):
        result = string_original(*args, **kwargs)
        string_calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(join_mod, 'probe_int64_dense', dense_spy)
    monkeypatch.setattr(arrow_join_mod, 'probe_strings_hash', string_spy)

    Table({'key': [1, 2]}).inner_join(
        Table({'key': [2]}), 'key', 'key')
    Table({'key': ['a', 'b']}).inner_join(
        Table({'key': ['b']}), 'key', 'key')

    assert dense_calls == [True, False]
    assert string_calls == [True]
