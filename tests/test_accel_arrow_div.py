"""
Conformance tests for the OPTIONAL arrow-accelerated true division
(serif._accel.arrow.div_floats).

The guarantee under test — python in → python out, backend-independent:
`v / other` must return IDENTICAL vectors whether the arrow backend
runs or not, including raising the same ZeroDivisionError when a zero
divisor actually divides.

Unlike the int-arithmetic tier this one runs BEFORE numpy (base.py):
numpy's division must neutralize null-lane divisors (a copy) and scan
for zeros (a pass); arrow's divide_checked skips null lanes and raises
on real zeros, so the preparation work disappears. The load-bearing
assumption — that arrow ignores the 0-SENTINELS serif parks under null
divisor lanes — is pinned by an engagement assert in
test_zero_sentinel_under_null_stays_accelerated: if a pyarrow version
ever checks null lanes, that test fails loudly and the tier needs a
fill_null(1) before the kernel (the arrow-native neutralize).

Skipped entirely when pyarrow isn't installed.
"""

import math
import operator

import pytest

np = pytest.importorskip("numpy")
pa = pytest.importorskip("pyarrow")

from serif import Vector
import serif._accel as accel
from serif._accel import arrow as bridge


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
    saved_np = accel._USE_NUMPY
    accel._USE_NUMPY = False
    try:
        return _pure(fn)
    finally:
        accel._USE_NUMPY = saved_np


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
        elif isinstance(p, float) and math.isnan(p):
            assert isinstance(f, float) and math.isnan(f), f"[{i}]: {f!r} is not nan"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"
            if isinstance(p, float):
                # -0.0 == 0.0 passes ==; the sign bit must match too.
                assert math.copysign(1, f) == math.copysign(1, p), \
                    f"[{i}]: sign of {f!r} != sign of {p!r}"


def _conform(fn):
    pure = _pure(fn)
    fast = fn()
    _assert_identical(pure, fast)
    return fast


# ---------------------------------------------------------------------------
# Bit-identical lanes, every operand shape
# ---------------------------------------------------------------------------

def test_float_float_conforms():
    a = Vector([1.5, -0.0, float('inf'), float('nan'), None, 10.0])
    b = Vector([0.5, 4.0, 2.0, 2.0, 3.0, float('inf')])
    fast = _conform(lambda: a / b)
    assert fast[5] == 0.0


def test_float_scalar_conforms():
    v = Vector([1.5, None, -7.25, float('nan')])
    _conform(lambda: v / 0.25)
    _conform(lambda: v / -3.0)
    _conform(lambda: v / float('inf'))


def test_mixed_int_float_conforms():
    ints = Vector([3, -7, None, 2**53])          # 2**53 exact as float64
    floats = Vector([2.0, 0.5, 1.0, 4.0])
    _conform(lambda: ints / floats)              # int64 ÷ float64
    _conform(lambda: floats / ints)              # float64 ÷ int64 (and the
    #                                              divisor's 0-sentinel sits
    #                                              under its null lane)
    _conform(lambda: ints / 2.5)                 # int64 ÷ float scalar
    _conform(lambda: floats / 4)                 # float64 ÷ int scalar


# ---------------------------------------------------------------------------
# Zero divisors: the same raise, either mode
# ---------------------------------------------------------------------------

def test_real_zero_divisor_raises_identically():
    a = Vector([1.0, 2.0])
    b = Vector([4.0, 0.0])
    with pytest.raises(ZeroDivisionError) as fast_err:
        a / b
    with pytest.raises(ZeroDivisionError) as pure_err:
        _pure(lambda: a / b)
    assert str(fast_err.value) == str(pure_err.value)


def test_negative_zero_divisor_raises_identically():
    a = Vector([1.0])
    b = Vector([-0.0])
    with pytest.raises(ZeroDivisionError):
        a / b
    with pytest.raises(ZeroDivisionError):
        _pure(lambda: a / b)


def test_zero_scalar_raises_identically():
    v = Vector([1.0, 2.0])
    for zero in (0.0, -0.0, 0):
        with pytest.raises(ZeroDivisionError) as fast_err:
            v / zero
        with pytest.raises(ZeroDivisionError) as pure_err:
            _pure(lambda: v / zero)
        assert str(fast_err.value) == str(pure_err.value)


def test_zero_sentinel_under_null_stays_accelerated():
    # THE load-bearing pin: b holds a 0.0 sentinel under its null lane —
    # every nullable column does. That zero never DIVIDES in the pure
    # path, and arrow must skip it too: not just conform (decline would
    # conform), ACCELERATE. If this assert ever fails on a pyarrow
    # upgrade, divide_checked started checking null lanes — neutralize
    # with fill_null(1) before the kernel.
    a = Vector([1.0, 8.0, None])
    b = Vector([2.0, None, 4.0])
    fast_st = bridge.div_floats(a._storage, b._storage,
                                operator.truediv, float)
    assert fast_st is not None
    assert list(fast_st) == [0.5, None, None]

    fast = _conform(lambda: a / b)
    assert list(fast) == [0.5, None, None]


# ---------------------------------------------------------------------------
# Declines that must stay invisible
# ---------------------------------------------------------------------------

def test_int_int_declines_everywhere():
    # Python true-divides integers EXACTLY at any magnitude; float64
    # transport is only exact through 2**53. Arrow declines all of it —
    # the ≤2**53 case is numpy's (guarded), beyond it pure is the spec.
    a = Vector([1, 2, None])
    b = Vector([4, 8, 2])
    assert bridge.div_floats(a._storage, b._storage,
                             operator.truediv, float) is None
    _conform(lambda: a / b)
    _conform(lambda: a / 4)

    big = Vector([2**53 + 1])
    fast = _conform(lambda: big / (2**53 + 1))
    assert fast[0] == 1.0                        # exact in pure, both modes


def test_floordiv_and_mod_are_not_divisions_here():
    a = Vector([7.5, -7.5, None])
    b = Vector([2.0, 2.0, 2.0])
    for op in (operator.floordiv, operator.mod):
        assert bridge.div_floats(a._storage, b._storage, op, float) is None
        _conform(lambda op=op: op(a, b))         # numpy's tier, unchanged


def test_bool_scalar_declines_conform():
    v = Vector([1.0, 2.0])
    assert bridge.div_floats(v._storage, True, operator.truediv, float) is None
    _conform(lambda: v / True)


def test_huge_int_scalar_declines_conform():
    v = Vector([2.0, None, 8.0])
    fast = _conform(lambda: v / 2**64)
    assert fast[0] == 2.0 / 2**64


# ---------------------------------------------------------------------------
# numpy-absent mode: arrow alone carries division
# ---------------------------------------------------------------------------

def test_numpy_off_arrow_still_accelerates(monkeypatch):
    calls = []
    orig = bridge.div_floats

    def spy(*args, **kwargs):
        result = orig(*args, **kwargs)
        calls.append(result is not None)
        return result

    monkeypatch.setattr(bridge, 'div_floats', spy)
    monkeypatch.setattr(accel, '_USE_NUMPY', False)

    a = Vector([1.5, None, -6.0])
    b = Vector([0.5, 2.0, None])

    def run():
        return a / b

    fast = run()
    assert calls == [True]
    _assert_identical(_all_pure(run), fast)
