"""
Conformance tests for the OPTIONAL numpy-accelerated positional take.

The guarantee under test — python in → python out, backend-independent:
for any (storage, indices) pair, take_storage must be indistinguishable
from the pure storage.take(): same values, same nulls in the same slots,
same concrete storage type — and every surfaced value a concrete Python
type, never a numpy scalar. Public paths that gather (sort_by, dropna,
Table.sort_by, aggregate/window group slicing) must return identical
results with the accelerator on or off.

Skipped entirely when numpy isn't installed.
"""

import math
from datetime import date

import pytest

np = pytest.importorskip("numpy")

from serif import Table, Vector
from serif._execution import DECLINED
from serif._vector._numpy import selection as mask_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pure(fn):
    saved = mask_mod._USE_NUMPY
    mask_mod._USE_NUMPY = False
    try:
        return fn()
    finally:
        mask_mod._USE_NUMPY = saved


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


# ---------------------------------------------------------------------------
# Storage-level conformance: take_storage vs the pure storage.take()
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

INDEX_SETS = [
    ("identity", [0, 1, 2, 3]),
    ("reverse",  [3, 2, 1, 0]),
    ("subset",   [2, 0]),
    ("dupes",    [1, 1, 3, 1, 0]),   # duplicates GROW the output (join fan-out)
    ("empty",    []),
]


@pytest.mark.parametrize("vf", [v[1] for v in VECTORS], ids=[v[0] for v in VECTORS])
@pytest.mark.parametrize("indices", [i[1] for i in INDEX_SETS], ids=[i[0] for i in INDEX_SETS])
def test_storage_conformance(vf, indices):
    storage = vf()._storage
    fast = mask_mod.take_storage(storage, indices)
    pure = storage.take(indices)
    assert fast is not DECLINED, "supported backend must not decline"
    assert type(fast) is type(pure)
    assert len(fast) == len(pure)
    for i in range(len(pure)):
        _assert_same_value(pure[i], fast[i], f"[{i}]")
        assert fast.is_null(i) == pure.is_null(i)


def test_dupes_of_null_slots_stay_null():
    storage = Vector([10, None, 30])._storage
    out = mask_mod.take_storage(storage, [1, 1, 1, 0])
    assert [out[i] for i in range(4)] == [None, None, None, 10]


# ---------------------------------------------------------------------------
# Public-path conformance: sort / dropna / table sort / aggregate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("vf", [v[1] for v in VECTORS], ids=[v[0] for v in VECTORS])
def test_sort_conforms(vf):
    _assert_identical(_pure(lambda: vf().sort_by()), vf().sort_by())
    _assert_identical(_pure(lambda: vf().sort_by(reverse=True, na_last=False)),
                      vf().sort_by(reverse=True, na_last=False))


@pytest.mark.parametrize("vf", [v[1] for v in VECTORS], ids=[v[0] for v in VECTORS])
def test_dropna_conforms(vf):
    _assert_identical(_pure(lambda: vf().dropna()), vf().dropna())


def _sortable_table():
    return Table({
        'k': [3, 1, None, 2, 1],
        'x': [1.5, None, 2.5, 4.0, 0.5],
        's': ['c', 'a', None, 'b', 'a'],
        'b': [True, None, False, True, False],
        'd': [date(2026, 1, i) for i in (5, 3, 4, 1, 2)],  # TupleStorage: declines
    })


def test_table_sort_conforms():
    t = _sortable_table()
    fast = t.sort_by('k')
    pure = _pure(lambda: t.sort_by('k'))
    assert fast.column_names() == pure.column_names()
    for name in pure.column_names():
        _assert_identical(pure[name], fast[name])


def test_aggregate_conforms():
    # Exact binary floats: the take accelerator must not perturb values,
    # and sums of exact halves are exact on both reduce paths.
    t = Table({
        'g': [1, 2, 1, 2, 1],
        'x': [1.5, None, 2.5, 4.0, None],
        's': ['a', 'b', 'c', 'd', 'e'],
    })
    agg = {'total': t.x.sum, 'last_s': t.s.last}
    fast = t.aggregate(groupby=t.g, aggregations=agg)
    pure = _pure(lambda: t.aggregate(groupby=t.g, aggregations=agg))
    assert fast.to_dict() == pure.to_dict()


def test_window_conforms():
    t = Table({'g': ['a', 'b', 'a', 'b'], 'x': [1, 2, 3, None]})
    agg = {'gsum': t.x.sum}
    fast = t.window(groupby=t.g, aggregations=agg)
    pure = _pure(lambda: t.window(groupby=t.g, aggregations=agg))
    assert fast.to_dict() == pure.to_dict()


def test_callable_aggregation_conforms():
    t = Table({'g': [1, 1, 2], 'x': [1.5, 2.5, 4.0]})
    agg = {'spread': lambda grp: grp.x.max() - grp.x.min()}
    fast = t.aggregate(groupby=t.g, aggregations=agg)
    pure = _pure(lambda: t.aggregate(groupby=t.g, aggregations=agg))
    assert fast.to_dict() == pure.to_dict()


def test_categorical_group_key_declines_identically():
    # Categorical columns decline the take accelerator everywhere (their
    # codes live in _CategoryStorage); results must be unchanged.
    t = Table({'g': ['x', 'y', 'x'], 'v': [1, 2, 3]})
    t2 = Table([t.g.categorize(['x', 'y']).alias('g'), t.v])
    agg = {'total': t2.v.sum}
    fast = t2.aggregate(groupby='g', aggregations=agg)
    pure = _pure(lambda: t2.aggregate(groupby='g', aggregations=agg))
    assert fast.to_dict() == pure.to_dict()


# ---------------------------------------------------------------------------
# The fast path actually engages (guards against silent decline rot)
# ---------------------------------------------------------------------------

def test_fast_path_engages_for_supported_storage(monkeypatch):
    calls = []
    orig = mask_mod.take_storage

    def spy(storage, indices):
        result = orig(storage, indices)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(mask_mod, 'take_storage', spy)
    Vector([3, 1, 2]).sort_by()
    Vector(['b', 'a']).sort_by()
    Vector([True, None, False]).dropna()
    assert calls == [True, True, True]

    calls.clear()
    Vector([date(2026, 1, 2), date(2026, 1, 1)]).sort_by()  # TupleStorage → declines
    assert calls == [False]


def test_group_slicer_fallback_engages_when_fused_sum_declines(monkeypatch):
    from serif._accel import arrow as arrow_mod
    calls = []
    orig = mask_mod.take_storage

    def spy(storage, indices):
        result = orig(storage, indices)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(mask_mod, 'take_storage', spy)
    # Keep this as a fallback slicer guard. Eligible bound sums normally
    # bypass per-group slicing through the fused Arrow path.
    monkeypatch.setattr(arrow_mod, 'grouped_sums', lambda *args, **kwargs: None)
    t = Table({'g': [1, 2, 1], 'x': [1.0, 2.0, 3.0]})
    t.aggregate(groupby=t.g, aggregations={'total': t.x.sum})
    assert calls and all(calls)
