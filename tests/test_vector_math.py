"""Pure-Python pointwise ``Vector.math`` semantics."""

import math

import pytest

from serif import Schema
from serif import SerifValueError
from serif import Table
from serif import Vector


UNARY_CASES = (
    ('ceil', 1.25),
    ('factorial', 6),
    ('floor', -1.25),
    ('isqrt', 17),
    ('trunc', -1.25),
    ('fabs', -1.25),
    ('frexp', 8.0),
    ('isfinite', 1.25),
    ('isinf', float('inf')),
    ('isnan', float('nan')),
    ('modf', -1.25),
    ('ulp', 1.0),
    ('exp', 1.0),
    ('expm1', 1e-5),
    ('log', 2.0),
    ('log1p', 1e-5),
    ('log2', 8.0),
    ('log10', 1000.0),
    ('sqrt', 4.0),
    ('degrees', math.pi),
    ('radians', 180.0),
    ('acos', 0.5),
    ('asin', 0.5),
    ('atan', 0.5),
    ('cos', 0.5),
    ('sin', 0.5),
    ('tan', 0.5),
    ('acosh', 2.0),
    ('asinh', 0.5),
    ('atanh', 0.5),
    ('cosh', 0.5),
    ('sinh', 0.5),
    ('tanh', 0.5),
    ('erf', 0.5),
    ('erfc', 0.5),
    ('gamma', 5.0),
    ('lgamma', 5.0),
)


@pytest.mark.parametrize(('function_name', 'value'), UNARY_CASES)
def test_unary_math_matches_python_and_propagates_null(function_name, value):
    vector = Vector([value, None], name='source')

    result = getattr(vector.math, function_name)()
    expected = getattr(math, function_name)(value)

    assert result[0] == expected
    assert type(result[0]) is type(expected)
    assert result[1] is None
    assert result.vector_name is None


@pytest.mark.parametrize(
    ('function_name', 'expected_kind'),
    [
        ('ceil', int),
        ('isfinite', bool),
        ('frexp', tuple),
        ('exp', float),
    ],
)
def test_unary_math_has_known_nullable_result_schema(
    function_name,
    expected_kind,
):
    result = getattr(Vector([None, None]).math, function_name)()

    assert list(result) == [None, None]
    assert result.schema() == Schema(expected_kind, True)


def test_unary_math_preserves_python_domain_and_overflow_errors():
    with pytest.raises(ValueError):
        Vector([-1.0]).math.sqrt()

    with pytest.raises(OverflowError):
        Vector([1000.0]).math.exp()


def test_unary_math_lifts_over_table_columns_and_preserves_names():
    table = Table({
        'a': [1.0, math.e],
        'b': [math.e ** 2, None],
    })

    result = table.math.log()

    assert result.column_names() == ['a', 'b']
    assert result.to_dict() == {
        'a': [math.log(1.0), math.log(math.e)],
        'b': [math.log(math.e ** 2), None],
    }


MULTI_ARGUMENT_CASES = (
    pytest.param('comb', [5, None, 6], (Vector([2, 3, None]),), {}, id='comb'),
    pytest.param('perm', [5, None, 6], (Vector([2, 3, None]),), {}, id='perm'),
    pytest.param('copysign', [1.0, None, -2.0], (Vector([-1.0, 1.0, None]),), {}, id='copysign'),
    pytest.param('fmod', [5.5, None, 8.0], (2.0,), {}, id='fmod'),
    pytest.param(
        'isclose',
        [1.0, None, 3.0],
        (Vector([1.0001, 2.0, None]),),
        {'rel_tol': Vector([1e-3, 1e-3, 1e-3])},
        id='isclose',
    ),
    pytest.param('ldexp', [1.5, None, 2.0], (Vector([2, 3, None]),), {}, id='ldexp'),
    pytest.param('nextafter', [1.0, None, 3.0], (math.inf,), {}, id='nextafter'),
    pytest.param('remainder', [5.5, None, 8.0], (2.0,), {}, id='remainder'),
    pytest.param('pow', [2.0, None, 4.0], (Vector([3.0, 2.0, None]),), {}, id='pow'),
    pytest.param('atan2', [1.0, None, -1.0], (Vector([1.0, 2.0, None]),), {}, id='atan2'),
    pytest.param('log', [8.0, None, 16.0], (Vector([2.0, 4.0, None]),), {}, id='log'),
)


@pytest.mark.parametrize(
    ('function_name', 'values', 'args', 'kwargs'),
    MULTI_ARGUMENT_CASES,
)
def test_multi_argument_math_broadcasts_scalars_and_vectors(
    function_name,
    values,
    args,
    kwargs,
):
    result = getattr(Vector(values, name='source').math, function_name)(
        *args,
        **kwargs,
    )
    function = getattr(math, function_name)
    expected = []
    for index, value in enumerate(values):
        lane_args = [
            operand[index] if isinstance(operand, Vector) else operand
            for operand in args
        ]
        lane_kwargs = {
            name: operand[index] if isinstance(operand, Vector) else operand
            for name, operand in kwargs.items()
        }
        if (
            value is None
            or any(operand is None for operand in lane_args)
            or any(operand is None for operand in lane_kwargs.values())
        ):
            expected.append(None)
        else:
            expected.append(function(value, *lane_args, **lane_kwargs))

    assert list(result) == expected
    assert result.vector_name is None


def test_perm_scalar_none_uses_python_default_but_vector_null_propagates():
    values = Vector([4, None, 5])

    assert list(values.math.perm(None)) == [24, None, 120]
    assert list(values.math.perm(Vector([None, 2, 3]))) == [None, None, 60]


def test_scalar_none_propagates_for_required_math_argument():
    assert list(Vector([8.0, 16.0]).math.log(None)) == [None, None]


def test_multi_argument_math_rejects_length_mismatch_before_computing():
    with pytest.raises(SerifValueError, match='Length mismatch'):
        Vector([2.0, 3.0]).math.pow(Vector([2.0]))


def test_multi_argument_math_preserves_python_domain_errors():
    with pytest.raises(ValueError):
        Vector([-1.0]).math.pow(0.5)


def test_multi_argument_math_lifts_scalar_over_table_columns():
    table = Table({'a': [10.0, 100.0], 'b': [1000.0, None]})

    result = table.math.log(10.0)

    assert result.column_names() == ['a', 'b']
    assert result.to_dict() == {
        'a': [math.log(10.0, 10.0), math.log(100.0, 10.0)],
        'b': [math.log(1000.0, 10.0), None],
    }


def test_multi_argument_math_aligns_corresponding_table_columns():
    magnitude = Table({'a': [1.0, 2.0], 'b': [3.0, None]})
    sign = Table({'x': [-1.0, 1.0], 'y': [1.0, -1.0]})

    result = magnitude.math.copysign(sign)

    assert result.column_names() == ['a', 'b']
    assert result.to_dict() == {
        'a': [-1.0, 2.0],
        'b': [3.0, None],
    }
