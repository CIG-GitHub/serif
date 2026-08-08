"""Pure-Python whole-Vector mathematical reductions."""

import math

import pytest

from serif import SerifValueError
from serif import Table
from serif import Vector


def test_math_reductions_skip_nulls_and_match_python():
    vector = Vector([6, None, 10])
    known = [6, 10]

    assert vector.math.fsum() == math.fsum(known)
    assert type(vector.math.fsum()) is float
    assert vector.math.prod() == math.prod(known)
    assert vector.math.gcd() == math.gcd(*known)
    assert vector.math.lcm() == math.lcm(*known)
    assert vector.math.hypot() == math.hypot(*known)


def test_math_reductions_do_not_reserve_top_level_vector_names():
    vector = Vector([6, 10])

    for name in ('fsum', 'prod', 'gcd', 'lcm', 'hypot', 'dist'):
        assert not hasattr(vector, name)


def test_prod_preserves_exact_bigints():
    values = [2**62, 3, 5]

    assert Vector(values).math.prod() == math.prod(values)
    assert type(Vector(values).math.prod()) is int


def test_empty_and_all_null_math_reductions_return_identities():
    empty_untyped = Vector([])
    empty_int = Vector([], dtype=int)
    empty_float = Vector([], dtype=float)
    all_null_int = Vector([1, None])[Vector([False, True])]
    all_null_float = Vector([1.0, None])[Vector([False, True])]

    for vector in (empty_untyped, empty_int, all_null_int):
        assert vector.math.fsum() == 0.0
        assert vector.math.prod() == 1
        assert type(vector.math.prod()) is int
        assert vector.math.gcd() == 0
        assert vector.math.lcm() == 1
        assert vector.math.hypot() == 0.0

    for vector in (empty_float, all_null_float):
        assert vector.math.prod() == 1.0
        assert type(vector.math.prod()) is float


def test_fsum_preserves_math_fsum_nonfinite_errors():
    with pytest.raises(ValueError):
        Vector([math.inf, -math.inf]).math.fsum()


def test_dist_skips_unknown_coordinate_pairs():
    left = Vector([0.0, None, 3.0, 4.0])
    right = Vector([0.0, 2.0, None, 0.0])

    assert left.math.dist(right) == math.dist([0.0, 4.0], [0.0, 0.0])
    assert left.math.dist(list(right)) == left.math.dist(right)


def test_dist_validates_original_dimensions_before_skipping_nulls():
    with pytest.raises(SerifValueError, match='Length mismatch'):
        Vector([1.0, None]).math.dist(Vector([1.0]))


def test_dist_returns_identity_with_no_known_coordinate_pairs():
    assert Vector([None, None]).math.dist([None, None]) == 0.0
    assert Vector([]).math.dist([]) == 0.0


def test_math_reductions_lift_over_table_columns():
    table = Table({'a': [3.0, 4.0], 'b': [5.0, None]})
    other = Table({'x': [0.0, 0.0], 'y': [2.0, 9.0]})

    assert list(table.math.prod()) == [12.0, 5.0]
    assert list(table.math.hypot()) == [5.0, 5.0]
    assert list(table.math.dist(other)) == [5.0, 3.0]


def test_math_reductions_work_as_bound_grouped_aggregations():
    table = Table({
        'group': ['A', 'A', 'B'],
        'value': [6, None, 10],
    })

    result = table.aggregate(
        'group',
        {
            'fsum': table.value.math.fsum,
            'product': table.value.math.prod,
            'gcd': table.value.math.gcd,
            'lcm': table.value.math.lcm,
            'hypot': table.value.math.hypot,
        },
    )

    assert result.to_dict() == {
        'group': ['A', 'B'],
        'fsum': [6.0, 10.0],
        'product': [6, 10],
        'gcd': [6, 10],
        'lcm': [6, 10],
        'hypot': [6.0, 10.0],
    }
    assert list(result.fsum) == [6.0, 10.0]
    assert list(result.gcd) == [6, 10]
