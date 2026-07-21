"""
Conformance tests for the OPTIONAL numpy-accelerated Kleene logical ops.

The guarantee under test — python in → python out, backend-independent:
for any pair of bool vectors (or a bool/None scalar rhs), `a & b`,
`a | b`, `a ^ b`, and `~a` must return an IDENTICAL result whether numpy
is installed or not. Same values, same nulls in the same slots, same
schema (including the post-hoc nullability rule for &/|/^ and the
schema-carried rule for ~), same storage type.

Two properties get adversarial attention:

* Garbage under null lanes: a BoolStorage born from an accelerated
  comparison carries the comparison of SENTINEL values in its masked
  bytes (e.g. null == null lanes hold a 1). Those bytes are unobservable
  through the mask and must never leak into a valid result lane.
* Settling: Kleene's known-False settles &, known-True settles | — so
  nulls in can mean non-nullable out, and the accel result must drop its
  mask exactly when the pure result has no Nones.

Declines (non-bool rhs vectors, list rhs) must be invisible: the pure
path runs and results agree by construction.

Skipped entirely when numpy isn't installed.
"""

import pytest

np = pytest.importorskip("numpy")

from serif import Table, Vector
from serif.errors import SerifValueError
from serif._vector._numpy import operators as numpy_ops
from serif._vector.storage import BoolStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pure(fn):
    saved = numpy_ops._USE_NUMPY
    numpy_ops._USE_NUMPY = False
    try:
        return fn()
    finally:
        numpy_ops._USE_NUMPY = saved


def _assert_identical(pure_v, fast_v):
    assert len(fast_v) == len(pure_v)
    assert fast_v.vector_name == pure_v.vector_name
    assert type(fast_v._storage) is type(pure_v._storage)
    assert fast_v.schema().kind is pure_v.schema().kind
    assert fast_v.schema().nullable is pure_v.schema().nullable
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        if p is None:
            assert f is None, f"[{i}]: expected None, got {f!r}"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"


def _conform(fn):
    _assert_identical(_pure(fn), fn())


# All nine (T, F, N) x (T, F, N) lane combinations in one vector pair.
T9A = [True, True, True, False, False, False, None, None, None]
T9B = [True, False, None, True, False, None, True, False, None]


# ---------------------------------------------------------------------------
# Truth tables — exhaustive lane coverage, accel vs pure
# ---------------------------------------------------------------------------

def test_and_truth_table():
    _conform(lambda: Vector(T9A) & Vector(T9B))


def test_or_truth_table():
    _conform(lambda: Vector(T9A) | Vector(T9B))


def test_xor_truth_table():
    _conform(lambda: Vector(T9A) ^ Vector(T9B))


def test_invert_truth_table():
    _conform(lambda: ~Vector([True, False, None]))


def test_kleene_values_are_the_spec():
    # Belt and braces: pin the table itself, not just pure/fast agreement.
    assert list(Vector(T9A) & Vector(T9B)) == [
        True, False, None, False, False, False, None, False, None]
    assert list(Vector(T9A) | Vector(T9B)) == [
        True, True, True, True, False, None, True, None, None]
    assert list(Vector(T9A) ^ Vector(T9B)) == [
        False, True, None, True, False, None, None, None, None]
    assert list(~Vector([True, False, None])) == [False, True, None]


# ---------------------------------------------------------------------------
# Nullability rules
# ---------------------------------------------------------------------------

def test_dense_inputs_stay_dense():
    a, b = Vector([True, True, False, False]), Vector([True, False, True, False])
    for expr in (lambda: a & b, lambda: a | b, lambda: a ^ b, lambda: ~a):
        _conform(expr)
        result = expr()
        assert result.schema().nullable is False
        assert result._storage._mask is None


def test_settled_results_drop_nullability():
    # Post-hoc rule: False settles &, True settles | — nulls in,
    # no nulls out, non-nullable schema and no storage mask.
    dense_false = Vector([False, False, False])
    dense_true = Vector([True, True, True])
    nullable = Vector([True, None, False])
    for expr in (lambda: dense_false & nullable,
                 lambda: dense_true | nullable):
        _conform(expr)
        result = expr()
        assert list(result) in ([False] * 3, [True] * 3)
        assert result.schema().nullable is False
        assert result._storage._mask is None


def test_invert_carries_schema_nullability_without_nulls():
    # ~ preserves the INPUT schema: a nullable-schema mask with no actual
    # nulls inverts to a nullable-schema result with no actual nulls.
    v = Vector([True, None, False])[0:1]      # nullable schema, no nulls
    assert v.schema().nullable is True
    _conform(lambda: ~v)
    assert (~v).schema().nullable is True
    assert list(~v) == [False]


# ---------------------------------------------------------------------------
# Garbage under null lanes must never leak
# ---------------------------------------------------------------------------

def test_garbage_sentinel_bytes_do_not_leak():
    # An accelerated comparison of nullable ints writes the comparison of
    # the 0-sentinels under its null lanes — here `null == null` writes a
    # raw 1 byte that the mask hides. Kleene ops on top must not surface it.
    left = Vector([0, None, 5, None])
    m = left == Vector([0, None, 5, 7])
    assert isinstance(m._storage, BoolStorage)
    assert m._storage.is_null(1)
    # Precondition for the test's power: the hidden byte really is set.
    # If a future change zeroes compare sentinels, revisit (don't just
    # delete) — find another garbage producer or drop the pin.
    assert m._storage._data[1] == 1
    _conform(lambda: m & Vector([True, True, True, True]))
    _conform(lambda: m | Vector([False, False, False, False]))
    _conform(lambda: m ^ Vector([False, False, False, False]))
    _conform(lambda: ~m)
    assert list(m & Vector([True] * 4)) == [True, None, True, None]


# ---------------------------------------------------------------------------
# Scalar rhs and reversed operands
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("scalar", [True, False, None])
def test_scalar_rhs(scalar):
    v = Vector([True, False, None])
    _conform(lambda: v & scalar)
    _conform(lambda: v | scalar)
    _conform(lambda: v ^ scalar)


def test_reversed_operands_route_the_same():
    v = Vector([True, False, None])
    _conform(lambda: True & v)
    _conform(lambda: False | v)


# ---------------------------------------------------------------------------
# Declines are invisible
# ---------------------------------------------------------------------------

def test_list_rhs_stays_pure_and_agrees():
    v = Vector([True, False, None])
    _conform(lambda: v & [True, None, False])
    _conform(lambda: v | [True, None, False])


def test_truthiness_rhs_vector_declines_invisibly():
    # bool & int-vector runs Kleene on TRUTHINESS — pure-path semantics
    # the accelerator must decline to (it only takes BoolStorage rhs).
    v = Vector([True, False, None])
    _conform(lambda: v & Vector([0, 7, 2]))


def test_int_bitwise_unaffected():
    # & on int vectors is bitwise arithmetic, not Kleene — untouched.
    _conform(lambda: Vector([1, 2, 3]) & Vector([3, 3, 3]))


def test_length_mismatch_raises_on_both_tiers():
    v = Vector([True, False])
    with pytest.raises(SerifValueError, match="Length mismatch"):
        v & Vector([True])
    with pytest.raises(SerifValueError, match="Length mismatch"):
        _pure(lambda: v & Vector([True]))


def test_empty_vectors():
    e = Vector([True])[0:0]
    _conform(lambda: e & e)
    _conform(lambda: e | e)
    _conform(lambda: e ^ e)
    _conform(lambda: ~e)


# ---------------------------------------------------------------------------
# End to end: the compound-predicate filter this tier exists for
# ---------------------------------------------------------------------------

def test_compound_predicate_filter_sum_end_to_end():
    t = Table({
        'a': list(range(100)),
        'b': [i % 7 for i in range(100)],
        'x': [float(i) for i in range(100)],
    })
    fast = t[(t.a > 10) & (t.b < 5)].x.sum()
    pure = _pure(lambda: t[(t.a > 10) & (t.b < 5)].x.sum())
    assert fast == pure

    fast = t[~(t.a <= 10) & (t.b < 5)].x.sum()
    pure = _pure(lambda: t[~(t.a <= 10) & (t.b < 5)].x.sum())
    assert fast == pure
