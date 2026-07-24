"""
Arithmetic edges: reverse operations with type promotion, and incompatible
operand handling.

Two doctrines pinned here:

1. Reverse ops promote like forward ops. 1.5 + Vector([1, 2]) is a FLOAT
   vector — historically it was stamped with the left vector's int dtype and
   crashed the int storage backend.

2. Incompatible element types raise (SerifTypeError, which is a TypeError —
   Python semantics). Historically Vector([1,2]) + Vector(['a','b'])
   silently returned a Vector of (1,'a') pairs.
"""

import operator

import pytest

from serif import Vector
from serif.errors import SerifTypeError
from serif._vector._python import operators as python_ops
from serif._vector.storage import ArrayStorage
from serif._vector.storage import TupleStorage


# ---------------------------------------------------------------------------
# Reverse ops — values
# ---------------------------------------------------------------------------

def test_radd_float_scalar_int_vector():
    assert list(1.5 + Vector([1, 2])) == [2.5, 3.5]


def test_radd_int_scalar_float_vector():
    assert list(1 + Vector([1.5])) == [2.5]


def test_radd_string_prefix():
    assert list('pre' + Vector(['a', 'b'])) == ['prea', 'preb']


def test_radd_list_is_elementwise():
    # Vector semantics: list + Vector is element-wise, not concatenation.
    assert list([1, 2] + Vector([3, 4])) == [4, 6]


def test_rsub():
    assert list(10 - Vector([1, 2])) == [9, 8]


def test_rmul():
    assert list(2 * Vector([3, 4])) == [6, 8]


def test_rtruediv():
    assert list(12 / Vector([4, 3])) == [3.0, 4.0]


def test_rfloordiv():
    assert list(7 // Vector([2])) == [3]


def test_rmod():
    assert list(7 % Vector([4])) == [3]


def test_rpow():
    assert list(2 ** Vector([3])) == [8]


def test_radd_preserves_none():
    assert list(0 + Vector([10, None, 20])) == [10, None, 20]


def test_radd_float_scalar_nullable_int_vector():
    assert list(1.5 + Vector([1, None])) == [2.5, None]


# ---------------------------------------------------------------------------
# Pure known-result storage construction
# ---------------------------------------------------------------------------

def test_pure_known_binary_kernels_return_array_storage():
    left = Vector([1, None, 3])._storage
    right = Vector([4, 5, 6])._storage

    vector_result = python_ops.binary_vector(
        left,
        right,
        operator.add,
        int,
    )
    scalar_result = python_ops.binary_scalar(
        left,
        0.5,
        operator.add,
        float,
    )

    assert isinstance(vector_result, ArrayStorage)
    assert list(vector_result) == [5, None, 9]
    assert isinstance(scalar_result, ArrayStorage)
    assert list(scalar_result) == [1.5, None, 3.5]


def test_pure_int_overflow_degrades_without_replaying_operations():
    left = Vector([1, None, 2**63 - 1, 3])._storage
    calls = []

    def tracked_add(x, y):
        calls.append((x, y))
        return x + y

    result = python_ops.binary_scalar(
        left,
        1,
        tracked_add,
        int,
    )

    assert isinstance(result, TupleStorage)
    assert list(result) == [2, None, 2**63, 4]
    assert calls == [(1, 1), (2**63 - 1, 1), (3, 1)]


def test_pure_unknown_binary_kernel_retains_materialized_result():
    result = python_ops.binary_scalar(
        Vector([1, None, 3])._storage,
        2,
        operator.add,
    )

    assert type(result) is tuple
    assert result == (3, None, 5)


# ---------------------------------------------------------------------------
# Reverse ops — dtype promotion (the H7 essence)
# ---------------------------------------------------------------------------

def test_radd_float_scalar_promotes_dtype():
    result = 1.5 + Vector([1, 2])
    assert result.schema().kind is float


def test_radd_int_scalar_keeps_int_dtype():
    result = 1 + Vector([2, 3])
    assert result.schema().kind is int


def test_rtruediv_promotes_int_to_float():
    result = 12 / Vector([4])
    assert result.schema().kind is float


def test_radd_string_keeps_str_dtype():
    result = 'x' + Vector(['a'])
    assert result.schema().kind is str


def test_radd_nullable_stays_nullable():
    result = 1.5 + Vector([1, None])
    assert result.schema().kind is float
    assert result.schema().nullable is True


# ---------------------------------------------------------------------------
# Incompatible operands raise — never tuple-pairs, never silence
# ---------------------------------------------------------------------------

def test_add_int_vector_to_str_vector_raises():
    with pytest.raises(TypeError):
        Vector([1, 2]) + Vector(['a', 'b'])


def test_add_str_vector_to_int_vector_raises():
    with pytest.raises(TypeError):
        Vector(['a', 'b']) + Vector([1, 2])


def test_add_incompatible_list_raises():
    with pytest.raises(TypeError):
        Vector([1, 2]) + ['a', 'b']


def test_add_incompatible_scalar_raises():
    with pytest.raises(TypeError):
        Vector([1, 2]) + 'a'


def test_add_none_scalar_raises():
    # Python semantics: 1 + None is a TypeError.
    with pytest.raises(TypeError):
        Vector([1, 2]) + None


def test_incompatible_error_is_serif_and_builtin():
    # Catchable both ways: as SerifTypeError and as plain TypeError.
    with pytest.raises(SerifTypeError):
        Vector([1]) + Vector(['a'])


def test_mul_incompatible_vectors_raises():
    with pytest.raises(TypeError):
        Vector([1.5]) * Vector(['a'])


def test_sub_incompatible_vectors_raises():
    with pytest.raises(TypeError):
        Vector(['a']) - Vector(['b'])
