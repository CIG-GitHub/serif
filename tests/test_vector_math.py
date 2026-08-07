"""Pure-Python pointwise ``Vector.math`` semantics."""

import math

import pytest

from serif import Schema
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
