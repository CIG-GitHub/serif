"""Pure-Python ``Vector.stats`` semantics."""

import math
import statistics
from datetime import date

import pytest

from serif import Schema
from serif import SerifTypeError
from serif import SerifValueError
from serif import Table
from serif import Vector


def test_stats_accessor_belongs_to_real_numeric_vectors():
    supported = (
        Vector([1, 2]),
        Vector([1.0, 2.0]),
        Vector([], dtype=float),
    )
    unsupported = (
        Vector([]),
        Vector([True, False]),
        Vector([1 + 2j]),
        Vector(['a']),
        Vector([date(2026, 8, 12)]),
        Vector([object()]),
    )

    for vector in supported:
        assert hasattr(vector, 'stats')
        assert 'stats' in dir(vector)
    for vector in unsupported:
        assert not hasattr(vector, 'stats')
        assert 'stats' not in dir(vector)


def test_stats_is_not_exposed_on_tables():
    table = Table({'a': [1.0], 'b': [2.0]})

    with pytest.raises(
        AttributeError,
        match="Table object has no attribute 'stats'",
    ):
        table.stats


@pytest.mark.parametrize(
    'function_name',
    [
        'mean',
        'fmean',
        'geometric_mean',
        'harmonic_mean',
        'median',
        'median_low',
        'median_high',
        'mode',
        'multimode',
        'pvariance',
        'variance',
        'pstdev',
        'stdev',
    ],
)
def test_univariate_statistics_skip_nulls_and_match_python(function_name):
    values = [1, None, 2, 2, 4]
    known = [1, 2, 2, 4]

    result = getattr(Vector(values).stats, function_name)()
    expected = getattr(statistics, function_name)(known)

    assert result == expected
    assert type(result) is type(expected)


@pytest.mark.parametrize('method', ['exclusive', 'inclusive'])
def test_quantiles_skip_nulls_and_preserve_parameters(method):
    values = [1.0, None, 2.0, 4.0, 8.0, 16.0]
    known = [1.0, 2.0, 4.0, 8.0, 16.0]

    assert Vector(values).stats.quantiles(n=5, method=method) == (
        statistics.quantiles(known, n=5, method=method)
    )


def test_variance_family_accepts_precomputed_centers():
    vector = Vector([1.0, None, 2.0, 4.0])
    known = [1.0, 2.0, 4.0]
    center = statistics.mean(known)

    assert math.isclose(
        vector.stats.pvariance(center),
        statistics.pvariance(known, center),
        rel_tol=1e-15,
    )
    assert math.isclose(
        vector.stats.variance(center),
        statistics.variance(known, center),
        rel_tol=1e-15,
    )
    assert math.isclose(
        vector.stats.pstdev(center),
        statistics.pstdev(known, center),
        rel_tol=1e-15,
    )
    assert math.isclose(
        vector.stats.stdev(center),
        statistics.stdev(known, center),
        rel_tol=1e-15,
    )


def test_weighted_harmonic_mean_skips_incomplete_pairs():
    values = Vector([40.0, None, 60.0, 80.0])
    weights = Vector([5.0, 10.0, None, 30.0])

    assert values.stats.harmonic_mean(weights) == statistics.harmonic_mean(
        [40.0, 80.0],
        weights=[5.0, 30.0],
    )
    assert values.stats.harmonic_mean(list(weights)) == (
        values.stats.harmonic_mean(weights)
    )


def test_statistics_return_none_when_too_few_known_values_remain():
    empty = Vector([], dtype=float)
    all_null = Vector([1.0, None])[Vector([False, True])]
    singleton = Vector([None, 3.0])

    for vector in (empty, all_null):
        for name in (
            'mean',
            'fmean',
            'geometric_mean',
            'harmonic_mean',
            'median',
            'median_low',
            'median_high',
            'mode',
            'pvariance',
            'variance',
            'pstdev',
            'stdev',
        ):
            assert getattr(vector.stats, name)() is None
        assert vector.stats.multimode() == []
        assert vector.stats.quantiles() is None

    assert singleton.stats.variance() is None
    assert singleton.stats.stdev() is None
    assert singleton.stats.quantiles() is None
    assert singleton.stats.pvariance() == 0.0
    assert singleton.stats.pstdev() == 0.0


def test_explicit_quantile_errors_are_not_hidden_by_empty_data():
    empty = Vector([], dtype=float)

    with pytest.raises(statistics.StatisticsError, match='at least 1'):
        empty.stats.quantiles(n=0)
    with pytest.raises(ValueError, match='Unknown method'):
        empty.stats.quantiles(method='nearest')


def test_statistics_preserve_python_domain_errors():
    with pytest.raises(statistics.StatisticsError):
        Vector([1.0, -1.0]).stats.geometric_mean()
    with pytest.raises(statistics.StatisticsError):
        Vector([1.0, -1.0]).stats.harmonic_mean()
    with pytest.raises(statistics.StatisticsError):
        Vector([1.0, 1.0]).stats.correlation([2.0, 3.0])


@pytest.mark.parametrize(
    'function_name',
    ['covariance', 'correlation', 'linear_regression'],
)
def test_paired_statistics_skip_incomplete_pairs(function_name):
    left = Vector([1.0, None, 3.0, 4.0, 5.0])
    right = Vector([2.0, 4.0, None, 8.0, 11.0])
    known_left = [1.0, 4.0, 5.0]
    known_right = [2.0, 8.0, 11.0]

    result = getattr(left.stats, function_name)(right)
    expected = getattr(statistics, function_name)(known_left, known_right)

    assert result == expected
    assert type(result) is type(expected)
    assert getattr(left.stats, function_name)(list(right)) == expected


def test_paired_statistics_validate_original_lengths_before_null_stripping():
    with pytest.raises(SerifValueError, match='Length mismatch'):
        Vector([1.0, None]).stats.covariance([1.0])


def test_paired_statistics_require_a_vector_or_iterable():
    with pytest.raises(SerifTypeError, match='Vector or iterable'):
        Vector([1.0, 2.0]).stats.correlation(3.0)


def test_paired_statistics_return_none_with_insufficient_complete_pairs():
    left = Vector([1.0, None, 3.0])
    right = Vector([None, 2.0, 6.0])

    assert left.stats.covariance(right) is None
    assert left.stats.correlation(right) is None
    assert left.stats.linear_regression(right) is None


def test_median_grouped_is_deliberately_not_exposed():
    assert not hasattr(Vector([1.0, 2.0]).stats, 'median_grouped')


def test_stats_reductions_work_as_bound_grouped_aggregations():
    table = Table({
        'group': ['A', 'A', 'A', 'B', 'B'],
        'value': [1.0, None, 3.0, 4.0, 8.0],
    })

    result = table.aggregate(
        'group',
        {
            'mean_value': table.value.stats.mean,
            'median_value': table.value.stats.median,
            'variance_value': table.value.stats.variance,
        },
    )

    assert result.to_dict() == {
        'group': ['A', 'B'],
        'mean_value': [2.0, 6.0],
        'median_value': [2.0, 6.0],
        'variance_value': [2.0, 8.0],
    }


def test_all_null_typed_vector_keeps_stats_accessor():
    vector = Vector([None, None], dtype=Schema(float, True))

    assert vector.stats.mean() is None
