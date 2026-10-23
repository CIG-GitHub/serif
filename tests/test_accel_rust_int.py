"""
Conformance tests for the OPTIONAL Rust nullable int64 arithmetic
(serif._vector._rust.operators.binop_storage).

The guarantee under test: `v <op> other` on int columns returns IDENTICAL
vectors whether the Rust kernel runs or every accelerator is off. Where
int64 cannot hold Python's answer, the kernel must DECLINE and the pure
path's behavior must surface unchanged:

  * overflow in a valid lane → pure widens to exact ints (TupleStorage)
  * zero divisors in valid lanes → pure raises ZeroDivisionError
  * placeholders under null lanes never overflow, divide, or raise

Python's floor division and modulo round toward negative infinity; Rust
truncates toward zero. The kernel corrects for that, pinned here on every
sign combination.

Skipped entirely when the extension isn't built.
"""

import operator
import random

import pytest

pytest.importorskip("serif._rust")

from serif import Vector
from serif._execution import DECLINED
from serif._vector import operators as vector_ops
from serif._vector._arrow import operators as arrow_ops
from serif._vector._numpy import operators as numpy_ops
from serif._vector._rust import operators as rust_ops
from serif._vector.storage import ArrayStorage
from serif._vector.storage import TupleStorage


I64_MAX = 2**63 - 1
I64_MIN = -2**63

OPS = [
    operator.add,
    operator.sub,
    operator.mul,
    operator.floordiv,
    operator.mod,
]

REVERSE = {
    vector_ops._reverse_add: operator.add,
    vector_ops._reverse_sub: operator.sub,
    vector_ops._reverse_floordiv: operator.floordiv,
    vector_ops._reverse_mod: operator.mod,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_pure(fn, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(rust_ops, '_USE_RUST', False)
        m.setattr(numpy_ops, '_USE_NUMPY', False)
        m.setattr(arrow_ops, '_USE_ARROW', False)
        return fn()


def _assert_identical(pure_v, fast_v):
    assert len(fast_v) == len(pure_v)
    assert fast_v.vector_name == pure_v.vector_name
    assert fast_v.schema().kind is pure_v.schema().kind
    assert fast_v.schema().nullable is pure_v.schema().nullable
    assert type(fast_v._storage) is type(pure_v._storage)
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        assert f == p, f"[{i}]: {f!r} != {p!r}"
        assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"


def _conform(make, monkeypatch):
    _assert_identical(_all_pure(make, monkeypatch), make())


@pytest.fixture
def engaged(monkeypatch):
    """Record whether each Rust call accelerated (True) or declined."""
    calls = []
    orig = rust_ops.binop_storage

    def spy(*args):
        result = orig(*args)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(rust_ops, 'binop_storage', spy)
    return calls


SIGNED = [7, -7, 2, -2, 6, -6, 1, -1, 0, 13, -13, I64_MAX, I64_MIN]
DIVISORS = [2, -2, 3, -3, 1, -1, 7, -7, I64_MAX, I64_MIN]


# ---------------------------------------------------------------------------
# Conformance: every op, every operand shape
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op", OPS, ids=lambda f: f.__name__)
def test_vector_vector(op, monkeypatch):
    left = Vector([5, -9, None, 12, 0, -3, 8, None, 11])
    right = Vector([3, 4, 2, None, -5, -3, 1, None, -4])
    _conform(lambda: op(left, right), monkeypatch)


@pytest.mark.parametrize("op", OPS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("scalar", [3, -4, 1, -1, 2**40])
def test_vector_scalar(op, scalar, monkeypatch):
    left = Vector([5, -9, None, 12, 0, -3, 8, 2**40])
    _conform(lambda: op(left, scalar), monkeypatch)


@pytest.mark.parametrize("op", OPS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("scalar", [3, -4, 0, 2**40])
def test_scalar_vector(op, scalar, monkeypatch):
    right = Vector([5, -9, None, 12, -3, 8, 1])
    _conform(lambda: op(scalar, right), monkeypatch)


@pytest.mark.parametrize("op", OPS, ids=lambda f: f.__name__)
def test_floor_semantics_on_every_sign(op, monkeypatch, engaged):
    pairs = [(a, b) for a in SIGNED for b in DIVISORS]
    left = Vector([a for a, _ in pairs])
    right = Vector([b for _, b in pairs])
    _conform(lambda: op(left, right), monkeypatch)


def test_floor_division_matches_python_directly(engaged):
    left = Vector([-7, 7, -7, 7, -1, 1])
    right = Vector([2, -2, -2, 2, 3, -3])
    assert list(left // right) == [-4, -4, 3, 3, -1, -1]
    assert list(left % right) == [1, -1, -1, 1, 2, -2]
    assert engaged == [True, True]


def test_engages_on_plain_int_columns(engaged):
    a = Vector([1, 2, 3])
    b = Vector([10, None, 30])
    a + b
    a - 4
    10 - a
    a * b
    a // 2
    7 % b
    assert engaged == [True] * 6


@pytest.mark.parametrize("n", [0, 1, 7, 8, 9, 63, 64, 65, 1000])
def test_lengths_around_mask_bytes(n, monkeypatch, engaged):
    rng = random.Random(n)
    left = Vector([None if rng.random() < 0.2 else rng.randint(-50, 50) for _ in range(n)])
    right = Vector([None if rng.random() < 0.2 else rng.choice([-7, -2, 3, 9]) for _ in range(n)])
    for op in OPS:
        _conform(lambda: op(left, right), monkeypatch)


def test_randomized_against_python(monkeypatch):
    rng = random.Random(20261015)
    values = [I64_MIN, I64_MIN + 1, -1, 0, 1, I64_MAX - 1, I64_MAX]
    for _ in range(40):
        n = rng.randint(1, 40)
        left = Vector([
            None if rng.random() < 0.15
            else rng.choice(values) if rng.random() < 0.2
            else rng.randint(-2**40, 2**40)
            for _ in range(n)
        ])
        right = Vector([
            None if rng.random() < 0.15
            else rng.choice([v for v in values if v]) if rng.random() < 0.2
            else rng.choice([-1, 1]) * rng.randint(1, 2**20)
            for _ in range(n)
        ])
        for op in OPS:
            _conform(lambda: op(left, right), monkeypatch)


# ---------------------------------------------------------------------------
# Declines: Python's answer does not fit int64
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("make", [
    lambda: Vector([I64_MAX, 1]) + Vector([1, 1]),
    lambda: Vector([I64_MIN, 1]) - 1,
    lambda: Vector([2**62, 3]) * Vector([2, 3]),
    lambda: Vector([I64_MIN, 4]) // -1,
    lambda: 0 - Vector([I64_MIN]),
], ids=['add', 'sub', 'mul', 'floordiv', 'rsub'])
def test_overflow_widens_exactly_like_python(make, monkeypatch, engaged):
    fast = make()
    assert engaged == [False]
    assert isinstance(fast._storage, TupleStorage)
    _conform(make, monkeypatch)


def test_min_mod_minus_one_fits_and_engages(monkeypatch, engaged):
    _conform(lambda: Vector([I64_MIN, 5, -5]) % -1, monkeypatch)
    assert list(Vector([I64_MIN]) % -1) == [0]
    assert engaged[-1] is True


def test_overflow_under_null_lane_does_not_decline(monkeypatch, engaged):
    # The placeholder under a null is 0, but a garbage MAX must not count.
    left = Vector([None, 1, 2])
    left._storage._data[0] = I64_MAX
    fast = left + Vector([1, 1, 1])
    assert engaged == [True]
    assert list(fast) == [None, 2, 3]
    assert fast._storage._data[0] == 0


def test_cross_lane_extremes_still_engage(engaged):
    # NumPy's bounds pass would decline this; no actual pair overflows.
    fast = Vector([I64_MAX, -5]) + Vector([-5, I64_MAX])
    assert engaged == [True]
    assert list(fast) == [I64_MAX - 5, I64_MAX - 5]


# ---------------------------------------------------------------------------
# Division by zero
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op", [operator.floordiv, operator.mod], ids=lambda f: f.__name__)
def test_zero_divisor_in_valid_lane_raises(op):
    with pytest.raises(ZeroDivisionError):
        op(Vector([1, 2]), Vector([1, 0]))
    with pytest.raises(ZeroDivisionError):
        op(Vector([1, 2]), 0)


@pytest.mark.parametrize("op", [operator.floordiv, operator.mod], ids=lambda f: f.__name__)
def test_zero_divisor_under_null_never_divides(op, monkeypatch, engaged):
    left = Vector([None, 9, 8])
    right = Vector([0, 2, None])
    _conform(lambda: op(left, right), monkeypatch)
    engaged.clear()
    assert list(op(left, right)) == [None, op(9, 2), None]
    assert engaged == [True]


def test_zero_scalar_over_all_null_column(monkeypatch, engaged):
    left = Vector([None, None], dtype=Vector([1, None]).schema())
    _conform(lambda: left // 0, monkeypatch)


# ---------------------------------------------------------------------------
# Direct contract: what the kernel accepts and declines
# ---------------------------------------------------------------------------

def _storage(values):
    return Vector(values)._storage


def test_declines_non_int_work():
    ints = _storage([1, 2])
    floats = _storage([1.0, 2.0])
    bools = _storage([True, False])
    assert rust_ops.binop_storage(floats, 1, operator.add, float) is DECLINED
    assert rust_ops.binop_storage(ints, 1.5, operator.add, float) is DECLINED
    assert rust_ops.binop_storage(bools, 1, operator.add, int) is DECLINED
    assert rust_ops.binop_storage(ints, ints, operator.truediv, float) is DECLINED
    assert rust_ops.binop_storage(ints, 2, operator.pow, int) is DECLINED
    assert rust_ops.binop_storage(ints, 2**63, operator.add, int) is DECLINED
    assert rust_ops.binop_storage(ints, True, operator.add, int) is DECLINED


def test_off_switch_declines(monkeypatch):
    monkeypatch.setattr(rust_ops, '_USE_RUST', False)
    assert rust_ops.binop_storage(_storage([1]), 1, operator.add, int) is DECLINED


@pytest.mark.parametrize("reverse", list(REVERSE), ids=lambda f: f.__name__)
def test_reverse_ops_swap_operands(reverse):
    column = _storage([7, -7, None])
    result = rust_ops.binop_storage(column, 3, reverse, int)
    assert isinstance(result, ArrayStorage)
    forward = REVERSE[reverse]
    assert list(result) == [forward(3, 7), forward(3, -7), None]


def test_inputs_are_not_modified():
    left = _storage([1, None, 3])
    right = _storage([4, 5, None])
    before = (left._data.tobytes(), bytes(left._mask._buf),
              right._data.tobytes(), bytes(right._mask._buf))
    rust_ops.binop_storage(left, right, operator.mul, int)
    after = (left._data.tobytes(), bytes(left._mask._buf),
             right._data.tobytes(), bytes(right._mask._buf))
    assert before == after
