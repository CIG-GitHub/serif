"""
Conformance tests for the OPTIONAL numpy-accelerated boolean-mask filter.

The guarantee under test — python in → python out, backend-independent:
for any (vector, mask) pair, v[mask] must return an IDENTICAL result
whether numpy is installed or not. Same values, same nulls in the same
slots, same schema, same storage type, same name — and every surfaced
value a concrete Python type, never a numpy scalar.

Declines (date/object columns, non-BoolStorage masks) must be
invisible: the pure path runs and results agree by construction.

Skipped entirely when numpy isn't installed.
"""

from datetime import date

import pytest

np = pytest.importorskip("numpy")

from serif import Table, Vector
from serif._execution import DECLINED
from serif._vector._numpy import selection as mask_mod
from serif._vector.storage import ArrayStorage, BoolStorage


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


def _assert_identical(pure_v, fast_v):
    assert len(fast_v) == len(pure_v)
    assert fast_v.vector_name == pure_v.vector_name
    assert type(fast_v._storage) is type(pure_v._storage)
    if pure_v.schema() is None:
        assert fast_v.schema() is None
    else:
        assert fast_v.schema().kind is pure_v.schema().kind
        assert fast_v.schema().nullable is pure_v.schema().nullable
    import math
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        if p is None:
            assert f is None, f"[{i}]: expected None, got {f!r}"
        elif isinstance(p, float) and math.isnan(p):
            assert isinstance(f, float) and math.isnan(f), f"[{i}]: {f!r} is not nan"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"


def _conform(v, mask):
    _assert_identical(_pure(lambda: v[mask]), v[mask])


# ---------------------------------------------------------------------------
# Vectors × masks
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

MASKS = [
    ("mixed",     lambda: Vector([True, False, True, False])),
    ("nullable",  lambda: Vector([True, None, False, True])),   # None excludes
    ("all_true",  lambda: Vector([True] * 4)),
    ("all_false", lambda: Vector([False] * 4)),
    ("list",      lambda: [False, True, True, False]),
]


@pytest.mark.parametrize("vf", [v[1] for v in VECTORS], ids=[v[0] for v in VECTORS])
@pytest.mark.parametrize("mf", [m[1] for m in MASKS], ids=[m[0] for m in MASKS])
def test_conformance(vf, mf):
    _conform(vf(), mf())


def test_nan_survives_filter_identically():
    import math
    v = Vector([1.5, float('nan'), 3.0])
    out = v[Vector([False, True, True])]
    assert math.isnan(out[0]) and out[1] == 3.0
    pure = _pure(lambda: v[Vector([False, True, True])])
    assert math.isnan(pure[0])


def test_empty_vector_empty_mask():
    v = Vector([1]).dropna()[0:0]
    _conform(v, Vector([True])[0:0])


# ---------------------------------------------------------------------------
# Declines are invisible
# ---------------------------------------------------------------------------

def test_date_columns_take_pure_path_identically():
    _conform(Vector([date(2026, 1, 1), date(2026, 1, 2)], name='d'),
             Vector([False, True]))


def test_string_filter_never_decodes():
    # The fast path gathers raw UTF-8 spans; a filtered vector must decode
    # each survivor identically to the pure path, byte for byte.
    v = Vector(['aé', '🎉' * 50, '', 'plain', None], name='s')
    mask = Vector([True, True, False, True, True])
    _conform(v, mask)
    out = v[mask]
    assert list(out) == ['aé', '🎉' * 50, 'plain', None]


def test_string_filter_both_copy_strategies_conform():
    # The copy strategy is picked by average span length (_JOIN_SPAN_BYTES):
    # short spans take the per-byte ragged gather, long spans the
    # preallocated span copy. Both must be invisible.
    short = Vector([f's{i}' for i in range(64)], name='v')        # ~3 B/span
    long_ = Vector(['x' * 100 + str(i) for i in range(64)], name='v')
    mask = Vector([i % 3 == 0 for i in range(64)])
    _conform(short, mask)
    _conform(long_, mask)


def test_table_mask_conforms():
    t = Table({'n': [1, None, 3, 4], 's': ['a', 'b', None, 'd']})
    mask = t['n'] > 1                      # nullable comparison: None excludes
    fast = t[mask]
    pure = _pure(lambda: t[mask])
    assert fast.column_names() == pure.column_names()
    for name in pure.column_names():
        _assert_identical(pure[name], fast[name])


# ---------------------------------------------------------------------------
# The fast path actually engages (guards against silent decline rot)
# ---------------------------------------------------------------------------

def test_fast_path_engages_for_supported_storage(monkeypatch):
    calls = []
    orig = mask_mod.filter_storage

    def spy(storage, mask):
        result = orig(storage, mask)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(mask_mod, 'filter_storage', spy)
    Vector([1, 2, 3])[Vector([True, False, True])]
    Vector([True, False])[Vector([True, True])]
    Vector(['a', 'b'])[Vector([True, False])]
    assert calls == [True, True, True]

    calls.clear()
    Vector([date(2026, 1, 1)])[Vector([True])]  # TupleStorage → declines
    assert calls == [False]
