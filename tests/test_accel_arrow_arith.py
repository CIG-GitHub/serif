"""
Conformance tests for the OPTIONAL arrow-accelerated checked integer
arithmetic (serif._vector._arrow.operators.binop_ints).

The guarantee under test — python in → python out, backend-independent:
`v <op> other` on int columns must return IDENTICAL vectors whether the
arrow backend runs or not — including when the result leaves int64 and
the pure path promotes (degrading storage to TupleStorage), which the
arrow path must reproduce by declining on ACTUAL overflow.

What makes this tier worth having is pinned here explicitly: numpy's
bounds pass must PREDICT overflow from operand extremes, so it declines
vector-vector cases where the extremes come from DIFFERENT lanes and no
actual pair overflows. Arrow's *_checked kernels compute-and-detect, so
those cases stay accelerated. (Scalar bounds are exact — a scalar's min
IS its max — so there is no scalar rescue; scalar coverage here is
conformance plus the numpy-absent mode.)

_pure() toggles only the arrow switch; _all_pure() toggles both.

Skipped entirely when pyarrow isn't installed (numpy too: the rescue
premises assert numpy's decline).
"""

import operator

import pytest

np = pytest.importorskip("numpy")
pa = pytest.importorskip("pyarrow")

from serif import Vector
from serif._execution import DECLINED
from serif._vector._arrow import operators as bridge
from serif._vector._numpy import operators as numpy_ops
from serif._vector._numpy.operators import binop_storage
from serif._vector.storage import ArrayStorage, TupleStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pure(fn):
    saved = bridge._USE_ARROW
    bridge._USE_ARROW = False
    try:
        return fn()
    finally:
        bridge._USE_ARROW = saved


def _all_pure(fn):
    saved_np = numpy_ops._USE_NUMPY
    numpy_ops._USE_NUMPY = False
    try:
        return _pure(fn)
    finally:
        numpy_ops._USE_NUMPY = saved_np


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
        if p is None:
            assert f is None, f"[{i}]: expected None, got {f!r}"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"


def _conform(fn):
    pure = _pure(fn)
    fast = fn()
    _assert_identical(pure, fast)
    return fast


# ---------------------------------------------------------------------------
# The rescues: numpy's bounds pass declines, arrow computes
# ---------------------------------------------------------------------------

def test_add_rescues_bounds_decline():
    # Bounds combine extremes across lanes: hi_a + hi_b = 2**63 predicts
    # overflow, but the actual PAIRS sum to 0. numpy declines; arrow runs.
    a = Vector([2**62, -(2**62), None])
    b = Vector([-(2**62), 2**62, 5])
    assert binop_storage(
        a._storage,
        b._storage,
        operator.add,
        int,
    ) is DECLINED
    fast = _conform(lambda: a + b)
    assert list(fast) == [0, 0, None]
    assert type(fast._storage) is ArrayStorage   # stayed in the buffer world


def test_sub_rescues_bounds_decline():
    # lo_a - hi_b crosses the floor, but the actual pairs never do.
    a = Vector([-(2**62) - 10, 0])
    b = Vector([-5, 2**62])
    assert binop_storage(
        a._storage,
        b._storage,
        operator.sub,
        int,
    ) is DECLINED
    fast = _conform(lambda: a - b)
    assert list(fast) == [-(2**62) - 5, -(2**62)]


def test_mul_rescues_bounds_decline():
    # Peak-magnitude product 2**35 * 2**35 predicts overflow; the actual
    # pairs are 2**36 each.
    a = Vector([2**35, 2])
    b = Vector([2, 2**35])
    assert binop_storage(
        a._storage,
        b._storage,
        operator.mul,
        int,
    ) is DECLINED
    fast = _conform(lambda: a * b)
    assert list(fast) == [2**36, 2**36]


def test_huge_value_under_null_never_computes():
    # The bounds pass sees 2**63 - 1 and declines the whole column; the
    # lane holding it is null on the other side, so neither the pure path
    # nor arrow ever computes it. Exact declining beats predicted.
    a = Vector([None, 5])
    b = Vector([2**63 - 1, 3])
    assert binop_storage(
        a._storage,
        b._storage,
        operator.add,
        int,
    ) is DECLINED
    fast = _conform(lambda: a + b)
    assert list(fast) == [None, 8]


# ---------------------------------------------------------------------------
# Actual overflow: arrow declines, pure promotes — identically
# ---------------------------------------------------------------------------

def test_actual_overflow_degrades_identically():
    v = Vector([2**63 - 1, 1])
    fast = _conform(lambda: v + v)
    assert list(fast) == [2**64 - 2, 2]          # promoted past int64
    assert type(fast._storage) is TupleStorage   # the pure degradation
    assert type(fast[0]) is int


def test_scalar_overflow_degrades_identically():
    v = Vector([2**63 - 1, 0])
    fast = _conform(lambda: v + 10)
    assert list(fast) == [2**63 + 9, 10]
    assert type(fast._storage) is TupleStorage


def test_out_of_range_scalar_declines_conform():
    v = Vector([1, 2, None])
    fast = _conform(lambda: v + 2**64)
    assert list(fast) == [2**64 + 1, 2**64 + 2, None]


# ---------------------------------------------------------------------------
# Ordinary lanes and other kinds: conformance, no regression
# ---------------------------------------------------------------------------

def test_plain_arithmetic_conforms():
    a = Vector([1, -2, None, 4])
    b = Vector([10, 20, 30, None])
    for op in (operator.add, operator.sub, operator.mul):
        _conform(lambda op=op: op(a, b))
        _conform(lambda op=op: op(a, 7))


def test_bool_scalar_stays_pure_and_conforms():
    v = Vector([1, 2])
    assert bridge.binop_ints(
        v._storage,
        True,
        operator.add,
        int,
    ) is DECLINED
    _conform(lambda: v + True)   # Python semantics: 1 + True == 2


def test_float_lanes_are_not_arrows_business():
    v = Vector([1, 2, None])
    assert bridge.binop_ints(
        v._storage,
        2.5,
        operator.add,
        float,
    ) is DECLINED
    _conform(lambda: v + 2.5)    # numpy's tier, unchanged


# ---------------------------------------------------------------------------
# numpy-absent mode: arrow alone carries int arithmetic
# ---------------------------------------------------------------------------

def test_numpy_off_arrow_still_accelerates(monkeypatch):
    calls = []
    orig = bridge.binop_ints

    def spy(*args, **kwargs):
        result = orig(*args, **kwargs)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(bridge, 'binop_ints', spy)
    monkeypatch.setattr(numpy_ops, '_USE_NUMPY', False)

    a = Vector([1, -2, None, 4])
    b = Vector([10, 20, 30, None])

    def run():
        return a + b

    fast = run()
    assert calls == [True]
    _assert_identical(_all_pure(run), fast)

    calls.clear()
    fast = a * 7
    assert calls == [True]
    _assert_identical(_all_pure(lambda: a * 7), fast)
