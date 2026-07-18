"""
Conformance tests for the OPTIONAL numpy-accelerated elementwise ops.

This is the BIT-IDENTICAL tier: each lane computes one IEEE operation on
both paths, so v ⊕ w must be cell-for-cell EQUAL (values, types, nulls,
schema, unnamed result) with numpy on or off. Everywhere numpy's
fixed-width model diverges from Python's numeric tower, the accelerator
must DECLINE and the pure path's behavior — including its exceptions —
must surface unchanged:

  * int overflow → pure promotes to bigint (TupleStorage degrade)
  * zero divisors in EXECUTING lanes → pure raises ZeroDivisionError
    (a zero under a null lane never divides, and must not raise)
  * int/float mixing past 2**53 → pure compares/divides exactly

Skipped entirely when numpy isn't installed.
"""

import math
import operator

import pytest

np = pytest.importorskip("numpy")

from serif import Vector
import serif._accel as accel
from serif._vector.storage import BoolStorage


def _pure(fn):
    saved = accel._USE_NUMPY
    accel._USE_NUMPY = False
    try:
        return fn()
    finally:
        accel._USE_NUMPY = saved


def _assert_identical(pure_v, fast_v):
    assert len(fast_v) == len(pure_v)
    assert fast_v.vector_name == pure_v.vector_name
    assert fast_v.schema().kind is pure_v.schema().kind
    assert fast_v.schema().nullable is pure_v.schema().nullable
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        if p is None:
            assert f is None, f"[{i}]: expected None, got {f!r}"
        elif isinstance(p, float) and math.isnan(p):
            assert isinstance(f, float) and math.isnan(f), f"[{i}]: {f!r} != nan"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"


def _conform(make):
    _assert_identical(_pure(make), make())


LHS = [
    ("int_dense",  lambda: Vector([7, -3, 0, 12], name='a')),
    ("int_null",   lambda: Vector([7, None, -3, None], name='a')),
    ("float_dense", lambda: Vector([1.5, -2.25, 0.5, 100.0], name='a')),
    ("float_null",  lambda: Vector([1.5, None, -0.0, 3.25], name='a')),
]

RHS = [
    ("int_vec",    lambda: Vector([2, 5, -1, 3])),
    ("int_null_vec", lambda: Vector([2, 5, None, 3])),
    ("float_vec",  lambda: Vector([0.5, -1.5, 2.0, 4.0])),
    ("int_scalar",  lambda: 3),
    ("float_scalar", lambda: -1.5),
]

ARITH = [operator.add, operator.sub, operator.mul, operator.truediv,
         operator.floordiv, operator.mod]
CMPS = [operator.eq, operator.ne, operator.lt, operator.le,
        operator.gt, operator.ge]


@pytest.mark.parametrize("lf", [x[1] for x in LHS], ids=[x[0] for x in LHS])
@pytest.mark.parametrize("rf", [x[1] for x in RHS], ids=[x[0] for x in RHS])
@pytest.mark.parametrize("op", ARITH, ids=lambda o: o.__name__)
def test_arithmetic_conformance(lf, rf, op):
    _conform(lambda: op(lf(), rf()))


@pytest.mark.parametrize("lf", [x[1] for x in LHS], ids=[x[0] for x in LHS])
@pytest.mark.parametrize("rf", [x[1] for x in RHS], ids=[x[0] for x in RHS])
@pytest.mark.parametrize("op", CMPS, ids=lambda o: o.__name__)
def test_comparison_conformance(lf, rf, op):
    _conform(lambda: op(lf(), rf()))
    assert isinstance(op(lf(), rf())._storage, BoolStorage)


def test_nan_and_signed_zero_comparisons():
    nan = float('nan')
    v = Vector([nan, 1.0, -0.0])
    _conform(lambda: v == Vector([nan, 1.0, 0.0]))   # nan!=nan, -0.0==0.0
    _conform(lambda: v < 5.0)                        # nan<x is False
    _conform(lambda: v != v)


# ---------------------------------------------------------------------------
# Divergence cases: the accelerator must step aside, behavior unchanged
# ---------------------------------------------------------------------------

def test_int_overflow_promotes_identically():
    big = Vector([2**62, 2**62])
    out = big + big                                  # > int64: pure promotes
    assert list(out) == [2**63, 2**63] == list(_pure(lambda: big + big))
    assert all(type(x) is int for x in out)


def test_mul_overflow_declines():
    v = Vector([2**32, -2**32])
    out = v * v
    assert list(out) == [2**64, 2**64] == list(_pure(lambda: v * v))


def test_zero_divisor_raises_identically():
    v = Vector([6, 8])
    for op in (operator.truediv, operator.floordiv, operator.mod):
        with pytest.raises(ZeroDivisionError):
            op(v, Vector([2, 0]))
        with pytest.raises(ZeroDivisionError):
            _pure(lambda: op(v, Vector([2, 0])))
    with pytest.raises(ZeroDivisionError):
        Vector([1.5]) / 0.0                          # floats raise too


def test_zero_under_null_lane_does_not_raise():
    # The zero divisor sits where the numerator is null: the pure path
    # never executes that division, so neither path may raise.
    a = Vector([6, None])
    b = Vector([3, 0])
    _conform(lambda: a / b)
    assert list(a / b) == [2.0, None]


def test_int_truediv_beyond_2_53_stays_exact():
    v = Vector([2**53 + 1])
    out = v / Vector([1])
    assert out[0] == _pure(lambda: (v / Vector([1]))[0])


def test_mixed_compare_beyond_2_53_stays_exact():
    # Python compares int-vs-float exactly; float64 conversion would say
    # equal. The accelerator must decline, not agree with float64.
    v = Vector([2**53 + 1])
    assert list(v == float(2**53)) == [False]
    assert list(_pure(lambda: v == float(2**53))) == [False]


def test_floordiv_int64_min_edge():
    v = Vector([-2**63 + 0, 10])                     # lo == int64 min
    out = v // Vector([-1, 3])                       # (-2**63)//-1 = 2**63
    assert list(out) == [2**63, 3] == list(_pure(lambda: v // Vector([-1, 3])))


# ---------------------------------------------------------------------------
# The fast path actually engages (guards against silent decline rot)
# ---------------------------------------------------------------------------

def test_fast_path_engages_and_declines_where_designed(monkeypatch):
    from serif._accel import ops as ops_mod
    engaged = []
    orig = ops_mod.binop_storage

    def spy(lhs, rhs, op_func, kind):
        result = orig(lhs, rhs, op_func, kind)
        engaged.append(result is not None)
        return result

    monkeypatch.setattr(ops_mod, 'binop_storage', spy)
    Vector([1, 2]) + Vector([3, 4])                  # engages
    Vector([1.5, 2.5]) * 2                           # engages
    Vector([1, None]) / Vector([2, 2])               # nullable: engages
    Vector([2**62, 2**62]) + Vector([2**62, 2**62])  # overflow: declines
    with pytest.raises(ZeroDivisionError):
        Vector([1, 2]) / Vector([1, 0])              # declines, pure raises
    assert engaged == [True, True, True, False, False]
