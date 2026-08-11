"""Conformance tests for exact NumPy pointwise math."""

import math

import pytest

pytest.importorskip('numpy')

from serif import Vector
from serif._execution import DECLINED
from serif._vector._numpy import math as math_mod
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage


def _pure(function):
    enabled = math_mod._USE_NUMPY
    math_mod._USE_NUMPY = False
    try:
        return function()
    finally:
        math_mod._USE_NUMPY = enabled


def _assert_identical(fast, pure):
    assert len(fast) == len(pure)
    assert fast.vector_name == pure.vector_name
    assert fast.schema() == pure.schema()
    assert type(fast) is type(pure)
    for actual, expected in zip(fast, pure):
        if isinstance(expected, float) and math.isnan(expected):
            assert isinstance(actual, float) and math.isnan(actual)
            assert math.copysign(1.0, actual) == math.copysign(1.0, expected)
        else:
            assert actual == expected
            assert type(actual) is type(expected)
            if isinstance(expected, float) and expected == 0.0:
                assert math.copysign(1.0, actual) == math.copysign(
                    1.0,
                    expected,
                )


@pytest.mark.parametrize(
    ('function_name', 'values', 'storage_type'),
    [
        ('fabs', [-1.25, None, float('inf'), -0.0], ArrayStorage),
        ('isfinite', [1.25, None, float('inf'), float('nan')], BoolStorage),
        ('isinf', [1.25, None, float('-inf'), float('nan')], BoolStorage),
        ('isnan', [1.25, None, float('inf'), float('nan')], BoolStorage),
    ],
)
def test_exact_unary_math_matches_pure(
    function_name,
    values,
    storage_type,
):
    vector = Vector(values)
    fast = getattr(vector.math, function_name)()
    pure = _pure(lambda: getattr(vector.math, function_name)())

    _assert_identical(fast, pure)
    assert isinstance(fast._storage, storage_type)


def test_fabs_preserves_signed_zero_semantics():
    result = Vector([-0.0]).math.fabs()[0]
    assert math.copysign(1.0, result) == 1.0


def test_fixed_width_ints_are_supported():
    vector = Vector([-2, None, 3])
    _assert_identical(
        vector.math.fabs(),
        _pure(lambda: vector.math.fabs()),
    )


@pytest.mark.parametrize('function_name', ['ceil', 'floor', 'trunc'])
@pytest.mark.parametrize(
    'values',
    [
        [-2, None, 3],
        [-1.75, None, 2.25],
        [float(-2**63), float(2**63 - 1024)],
    ],
)
def test_rounding_to_int_matches_pure(function_name, values):
    vector = Vector(values)
    fast = getattr(vector.math, function_name)()
    pure = _pure(lambda: getattr(vector.math, function_name)())

    _assert_identical(fast, pure)
    assert isinstance(fast._storage, ArrayStorage)


@pytest.mark.parametrize('function_name', ['ceil', 'floor', 'trunc'])
def test_rounding_declines_for_python_exceptions(function_name):
    with pytest.raises(OverflowError):
        getattr(Vector([float('inf')]).math, function_name)()
    with pytest.raises(ValueError):
        getattr(Vector([float('nan')]).math, function_name)()


@pytest.mark.parametrize('function_name', ['ceil', 'floor', 'trunc'])
@pytest.mark.parametrize('value', [float(2**63), 1e20])
def test_rounding_declines_for_bigint_results(function_name, value):
    vector = Vector([value])
    _assert_identical(
        getattr(vector.math, function_name)(),
        _pure(lambda: getattr(vector.math, function_name)()),
    )


@pytest.mark.parametrize('function_name', ['ceil', 'floor', 'trunc'])
def test_rounding_does_not_execute_null_lanes(function_name):
    vector = Vector([float('inf')])
    vector[0] = None
    _assert_identical(
        getattr(vector.math, function_name)(),
        _pure(lambda: getattr(vector.math, function_name)()),
    )


@pytest.mark.parametrize(
    'values',
    [
        [0, 1, None, 2**62],
        [0.0, -0.0, None, 2.0, float('inf'), float('nan')],
    ],
)
def test_sqrt_matches_pure(values):
    vector = Vector(values)
    fast = vector.math.sqrt()
    pure = _pure(lambda: vector.math.sqrt())

    _assert_identical(fast, pure)
    assert isinstance(fast._storage, ArrayStorage)


def test_sqrt_declines_for_python_domain_errors():
    with pytest.raises(ValueError):
        Vector([4.0, -1.0]).math.sqrt()


def test_sqrt_does_not_execute_null_lanes():
    vector = Vector([-1.0])
    vector[0] = None
    _assert_identical(
        vector.math.sqrt(),
        _pure(lambda: vector.math.sqrt()),
    )


def test_sqrt_declines_for_bigint_storage():
    vector = Vector([2**80])
    _assert_identical(
        vector.math.sqrt(),
        _pure(lambda: vector.math.sqrt()),
    )


@pytest.mark.parametrize(
    ('values', 'sign'),
    [
        ([1.5, -0.0, None, float('nan')], -0.0),
        (
            [1.5, None, -0.0, float('nan')],
            Vector([-1.0, 1.0, None, -1.0]),
        ),
        ([1, -2, None, 3], -1),
    ],
)
def test_copysign_matches_pure(values, sign):
    vector = Vector(values)
    fast = vector.math.copysign(sign)
    pure = _pure(lambda: vector.math.copysign(sign))

    _assert_identical(fast, pure)
    assert isinstance(fast._storage, ArrayStorage)


@pytest.mark.parametrize(
    'sign',
    [
        -(2**80),
        Vector([2**80]),
        None,
    ],
)
def test_copysign_declines_for_unsupported_signs(sign):
    vector = Vector([1.0])
    _assert_identical(
        vector.math.copysign(sign),
        _pure(lambda: vector.math.copysign(sign)),
    )


def test_copysign_declines_for_bigint_storage():
    vector = Vector([2**80])
    _assert_identical(
        vector.math.copysign(-1.0),
        _pure(lambda: vector.math.copysign(-1.0)),
    )


def test_all_valid_mask_is_not_retained():
    vector = Vector([1.0, None])[:1]
    assert vector.schema().nullable
    _assert_identical(
        vector.math.fabs(),
        _pure(lambda: vector.math.fabs()),
    )


def test_unsupported_storage_and_functions_decline():
    fixed_width = Vector([1.0])._storage
    bigint = Vector([2**80])._storage

    assert math_mod.unary_storage(fixed_width, 'exp') is DECLINED
    assert math_mod.unary_storage(bigint, 'fabs') is DECLINED
    _assert_identical(
        Vector([2**80]).math.fabs(),
        _pure(lambda: Vector([2**80]).math.fabs()),
    )


def test_fast_path_engages_and_declines_where_designed(monkeypatch):
    engaged = []
    original = math_mod.unary_storage

    def spy(storage, function_name):
        result = original(storage, function_name)
        engaged.append(result is not DECLINED)
        return result

    monkeypatch.setattr(math_mod, 'unary_storage', spy)
    Vector([-1.0, None]).math.fabs()
    Vector([1, 2]).math.isfinite()
    Vector([1.5, None]).math.floor()
    Vector([4.0]).math.sqrt()
    with pytest.raises(ValueError):
        Vector([-1.0]).math.sqrt()
    Vector([1e20]).math.floor()
    Vector([2**80]).math.fabs()
    assert engaged == [True, True, True, True, False, False, False]


def test_binary_fast_path_engages_and_declines_where_designed(monkeypatch):
    engaged = []
    original = math_mod.binary_storage

    def spy(storage, other, function_name):
        result = original(storage, other, function_name)
        engaged.append(result is not DECLINED)
        return result

    monkeypatch.setattr(math_mod, 'binary_storage', spy)
    Vector([1.0, None]).math.copysign(-1.0)
    Vector([1, 2]).math.copysign(Vector([-1, 1]))
    Vector([1.0]).math.copysign(2**80)
    Vector([1.0]).math.fmod(2.0)
    assert engaged == [True, True, False, False]
