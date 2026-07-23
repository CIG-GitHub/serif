"""
Conformance tests for the OPTIONAL numpy implementations behind joins and
partitions: _table._numpy.grouping.group_indices (single-key bucketing) and
_vector._numpy.selection.take_pad_storage (gather with -1 → null pad lanes).

The guarantee under test — accelerators widen transport, never semantics:
every join and aggregate must return IDENTICAL results with numpy on or
off — same values, same nulls, same schemas, same column names, same row
order, and the same error text when a cardinality expectation is
violated. Declines (float/nullable/multi keys, unsupported backends)
must be invisible.

Skipped entirely when numpy isn't installed.
"""

import math
from datetime import date

import pytest

np = pytest.importorskip("numpy")

from serif import Table, Vector
from serif._execution import DECLINED
from serif._table._arrow import joins as arrow_join_mod
from serif._table._numpy import grouping as group_mod
from serif._table._numpy import joins as join_mod
from serif._vector._numpy import selection as mask_mod
from serif.errors import SerifValueError


# ---------------------------------------------------------------------------
# Helpers (same harness as the other accel suites)
# ---------------------------------------------------------------------------

def _pure(fn):
    saved_grouping = group_mod._USE_NUMPY
    saved_join = join_mod._USE_NUMPY
    saved_selection = mask_mod._USE_NUMPY
    group_mod._USE_NUMPY = False
    join_mod._USE_NUMPY = False
    mask_mod._USE_NUMPY = False
    try:
        return fn()
    finally:
        group_mod._USE_NUMPY = saved_grouping
        join_mod._USE_NUMPY = saved_join
        mask_mod._USE_NUMPY = saved_selection


def _assert_same_value(p, f, where):
    if p is None:
        assert f is None, f"{where}: expected None, got {f!r}"
    elif isinstance(p, float) and math.isnan(p):
        assert isinstance(f, float) and math.isnan(f), f"{where}: {f!r} is not nan"
    else:
        assert f == p, f"{where}: {f!r} != {p!r}"
        assert type(f) is type(p), f"{where}: {type(f)} vs {type(p)}"


def _assert_identical(pure_v, fast_v):
    assert len(fast_v) == len(pure_v)
    assert fast_v.vector_name == pure_v.vector_name
    assert type(fast_v._storage) is type(pure_v._storage)
    if pure_v.schema() is None:
        assert fast_v.schema() is None
    else:
        assert fast_v.schema().kind is pure_v.schema().kind
        assert fast_v.schema().nullable is pure_v.schema().nullable
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        _assert_same_value(p, f, f"[{i}]")


def _assert_tables_identical(pure_t, fast_t):
    assert fast_t.column_names() == pure_t.column_names()
    assert len(fast_t) == len(pure_t)
    for i in range(len(pure_t.cols())):
        _assert_identical(pure_t.cols(i), fast_t.cols(i))


# ---------------------------------------------------------------------------
# group_indices — storage-level conformance vs the pure dict loop
# ---------------------------------------------------------------------------

def _pure_partition(vals):
    index = {}
    for i, v in enumerate(vals):
        index.setdefault((v,), []).append(i)
    return index


@pytest.mark.parametrize("vals", [
    [3, 1, 3, 2, 1, 3],
    [5, 5, 5, 5],
    [-2, -1, -2, 0],
    list(range(6, 0, -1)),
    [7],
], ids=["dupes", "single_key", "negatives", "all_unique_desc", "one_row"])
def test_group_indices_matches_pure_dict(vals):
    fast = group_mod.group_indices(Vector(vals)._storage)
    pure = _pure_partition(vals)
    assert fast is not DECLINED
    assert list(fast.keys()) == list(pure.keys())    # first-appearance order
    assert all(
        isinstance(bucket, np.ndarray) and bucket.dtype == np.intp
        for bucket in fast.values()
    )
    assert {
        key: bucket.tolist()
        for key, bucket in fast.items()
    } == pure


def test_group_indices_declines_unsupported():
    assert group_mod.group_indices(
        Vector([1.5, 2.5])._storage
    ) is DECLINED  # float: NaN semantics
    assert group_mod.group_indices(
        Vector([1, None, 2])._storage
    ) is DECLINED  # nullable
    assert group_mod.group_indices(
        Vector(['a', 'b'])._storage
    ) is DECLINED  # strings


# ---------------------------------------------------------------------------
# take_pad_storage — storage-level conformance
# ---------------------------------------------------------------------------

VECTORS = [
    ("int_dense",   lambda: Vector([10, 20, 30, 40], name='v')),
    ("int_null",    lambda: Vector([10, None, 30, None], name='v')),
    ("float_dense", lambda: Vector([1.5, -0.0, float('inf'), 4.25], name='v')),
    ("float_null",  lambda: Vector([1.5, None, float('nan'), None], name='v')),
    ("bool_dense",  lambda: Vector([True, False, True, False], name='v')),
    ("bool_null",   lambda: Vector([True, None, False, None], name='v')),
    ("str_dense",   lambda: Vector(['alpha', '', 'γδ 🎉', 'd' * 200], name='v')),
    ("str_null",    lambda: Vector(['alpha', None, '', None], name='v')),
]


@pytest.mark.parametrize("vf", [v[1] for v in VECTORS], ids=[v[0] for v in VECTORS])
@pytest.mark.parametrize("indices", [
    [2, -1, 0, -1, 2],
    [-1, -1, -1],
    [3, 2, 1, 0],        # no pads: plain take passthrough
], ids=["mixed_pads", "all_pads", "no_pads"])
def test_take_pad_conformance(vf, indices):
    storage = vf()._storage
    fast = mask_mod.take_pad_storage(storage, indices)
    assert fast is not DECLINED
    assert len(fast) == len(indices)
    for i, src in enumerate(indices):
        expected = None if src < 0 else storage[src]
        _assert_same_value(expected, fast[i], f"[{i}]")
        assert fast.is_null(i) == (expected is None)


def test_take_pad_on_empty_storage_declines():
    empty = Vector([1])[0:0]._storage
    assert mask_mod.take_pad_storage(
        empty,
        [-1, -1],
    ) is DECLINED


# ---------------------------------------------------------------------------
# Joins — end-to-end conformance, all three flavors
# ---------------------------------------------------------------------------

def _join_tables():
    left = Table({
        'id': [1, 2, 3, 4, 2],
        'x':  [1.5, None, 2.5, 4.0, 0.5],
        's':  ['a', None, 'c', 'd', 'e'],
        'b':  [True, False, None, True, False],
        'd':  [date(2026, 1, i) for i in range(1, 6)],  # TupleStorage: declines
    })
    right = Table({
        'id':    [2, 3, 5, 2],   # duplicate key 2: fan-out; 5: unmatched
        'other': [20, 30, None, 21],
        'tag':   ['p', 'q', None, 'r'],
    })
    return left, right


@pytest.mark.parametrize("flavor", ["inner_join", "left_join", "full_join"])
def test_join_conforms(flavor):
    def run():
        left, right = _join_tables()
        return getattr(left, flavor)(right, 'id', 'id',
                                     expect_right_unique=False)
    _assert_tables_identical(_pure(run), run())


def test_join_on_string_keys_conforms():
    # NumPy join probes decline str keys; Arrow owns them when installed.
    # Padded gather remains independent and still engages.
    def run():
        left = Table({'k': ['a', 'b', 'c'], 'x': [1, 2, 3]})
        right = Table({'k': ['b', 'c', 'd'], 'y': [2.5, 3.5, 4.5]})
        return left.left_join(right, 'k', 'k')
    _assert_tables_identical(_pure(run), run())


def test_join_no_matches_conforms():
    def run():
        left = Table({'id': [1, 2], 'x': [1.0, 2.0]})
        right = Table({'id': [3, 4], 'y': [3.0, 4.0]})
        return left.inner_join(right, 'id', 'id')
    _assert_tables_identical(_pure(run), run())


def test_expect_right_unique_error_matches_pure():
    def run():
        left = Table({'id': [5, 3]})
        right = Table({'id': [5, 3, 5, 3, 3], 'v': [1, 2, 3, 4, 5]})
        return left.inner_join(right, 'id', 'id')
    with pytest.raises(SerifValueError) as fast_err:
        run()
    with pytest.raises(SerifValueError) as pure_err:
        _pure(run)
    assert str(fast_err.value) == str(pure_err.value)


def test_expect_left_unique_error_matches_pure():
    def run():
        left = Table({'id': [7, 8, 7], 'x': [1, 2, 3]})
        right = Table({'id': [7, 8], 'y': [10, 20]})
        return left.inner_join(right, 'id', 'id', expect_left_unique=True)
    with pytest.raises(SerifValueError) as fast_err:
        run()
    with pytest.raises(SerifValueError) as pure_err:
        _pure(run)
    assert str(fast_err.value) == str(pure_err.value)


def test_multi_key_join_declines_conforms():
    def run():
        left = Table({'a': [1, 1, 2], 'b': [1, 2, 1], 'x': [10, 20, 30]})
        right = Table({'a': [1, 2], 'b': [2, 1], 'y': [200, 300]})
        return left.inner_join(right, ['a', 'b'], ['a', 'b'])
    _assert_tables_identical(_pure(run), run())


def test_left_join_empty_right_conforms():
    def run():
        left = Table({'id': [1, 2], 'x': [1.5, 2.5]})
        right = Table({'id': Vector([], dtype=int),
                       'y': Vector([], dtype=float)})
        return left.left_join(right, 'id', 'id')
    fast, pure = run(), _pure(run)
    _assert_tables_identical(pure, fast)
    assert list(fast['y']) == [None, None]


# ---------------------------------------------------------------------------
# Partitions — aggregate conformance incl. first-appearance group order
# ---------------------------------------------------------------------------

def test_aggregate_int_groupby_conforms():
    def run():
        t = Table({'g': [3, 1, 3, 2, 1], 'x': [1.5, 2.5, None, 4.0, 0.5]})
        return t.aggregate(groupby=t.g, aggregations={'total': t.x.sum})
    fast, pure = run(), _pure(run)
    _assert_tables_identical(pure, fast)
    assert list(fast.cols(0)) == [3, 1, 2]  # first-appearance order pinned


def test_group_index_arrays_drive_object_slicing():
    table = Table([
        Vector([2, 1, 2], name='g'),
        Vector([['a'], ['b'], ['c']], name='payload').to_object(),
    ])

    result = table.aggregate(
        groupby='g',
        aggregations={'size': lambda group: len(group.payload)},
    )

    assert list(result.g) == [2, 1]
    assert list(result['size']) == [2, 1]


def test_window_int_groupby_conforms():
    # track_row_keys declines the bucket accelerator; results unchanged.
    def run():
        t = Table({'g': [1, 2, 1, 2], 'x': [1, 2, 3, None]})
        return t.window(groupby=t.g, aggregations={'gsum': t.x.sum})
    _assert_tables_identical(_pure(run), run())


def test_multi_key_groupby_declines_conforms():
    def run():
        t = Table({'g1': [1, 1, 2], 'g2': [1, 2, 1], 'x': [1.0, 2.0, 3.0]})
        return t.aggregate(groupby=['g1', 'g2'], aggregations={'m': t.x.sum})
    _assert_tables_identical(_pure(run), run())


def test_nan_group_keys_retain_python_dict_semantics():
    nan = float('nan')
    table = Table({'g': [nan, nan], 'x': [1, 2]})
    result = table.aggregate(groupby='g')

    assert len(result) == 2
    assert all(math.isnan(value) for value in result.g)


# ---------------------------------------------------------------------------
# The fast paths actually engage (guards against silent decline rot)
# ---------------------------------------------------------------------------

def _spy(monkeypatch, module, fn_name, calls):
    orig = getattr(module, fn_name)

    def wrapper(*args, **kwargs):
        result = orig(*args, **kwargs)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(module, fn_name, wrapper)


def test_group_fallback_engages_when_fused_sum_declines(monkeypatch):
    from serif._table._arrow import aggregation as arrow_mod
    calls = []
    _spy(monkeypatch, group_mod, 'group_indices', calls)
    # Exercise the numpy bucket fallback, not the earlier fused Arrow
    # grouped-sum path (covered by test_accel_arrow_grouped_sum.py).
    monkeypatch.setattr(arrow_mod, '_USE_ARROW', False)

    t = Table({'g': [1, 2, 1], 'x': [1.0, 2.0, 3.0]})
    t.aggregate(groupby=t.g, aggregations={'m': t.x.sum})
    assert calls == [True]

    calls.clear()
    t2 = Table({'g': ['a', 'b', 'a'], 'x': [1.0, 2.0, 3.0]})
    t2.aggregate(groupby=t2.g, aggregations={'m': t2.x.sum})
    assert calls == [False]  # str key declines HERE; Table's Arrow grouping
    #                          picks it up when installed (its own suite)


def test_join_sort_fallback_engages_when_hash_probe_declines(monkeypatch):
    probe_calls, pad_calls = [], []
    _spy(monkeypatch, join_mod, 'probe_int64', probe_calls)
    _spy(monkeypatch, mask_mod, 'take_pad_storage', pad_calls)
    monkeypatch.setattr(join_mod, 'probe_int64_dense',
                        lambda *args, **kwargs: DECLINED)
    monkeypatch.setattr(arrow_join_mod, 'probe_strings_hash',
                        lambda *args, **kwargs: DECLINED)

    left = Table({'id': [1, 2, 3], 'a': [1.0, 2.0, 3.0]})
    right = Table({'id': [2, 3], 'b': ['x', 'y']})
    left.left_join(right, 'id', 'id')

    assert probe_calls == [True]         # int64 keys probe in numpy
    assert pad_calls and all(pad_calls)  # every typed column gathered fast

    probe_calls.clear()
    left2 = Table({'k': ['a', 'b'], 'x': [1, 2]})
    right2 = Table({'k': ['b'], 'y': [2.5]})
    left2.left_join(right2, 'k', 'k')
    assert probe_calls == [False]        # str keys decline HERE; arrow's
    #                                      probe picks them up when installed
