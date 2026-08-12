"""Conformance tests for optional NumPy-accelerated statistics."""

import math
import statistics

import pytest

np = pytest.importorskip('numpy')

from serif import Schema
from serif import Vector
from serif._execution import DECLINED
from serif._vector._numpy import statistics as stats_mod


def _pure(function):
    saved = stats_mod._USE_NUMPY
    stats_mod._USE_NUMPY = False
    try:
        return function()
    finally:
        stats_mod._USE_NUMPY = saved


@pytest.mark.parametrize(
    ('function_name', 'values'),
    [
        ('median', [5, None, 1, 3]),
        ('median', [5.0, None, 1.0, 3.0, 7.0]),
        ('median_low', [1, None, 2, 8, 9]),
        ('median_high', [1.0, None, 2.0, 8.0, 9.0]),
    ],
)
def test_order_statistics_match_python_exactly(function_name, values):
    vector = Vector(values)
    fast = getattr(vector.stats, function_name)()
    pure = _pure(lambda: getattr(vector.stats, function_name)())

    assert fast == pure
    assert type(fast) is type(pure)
    assert getattr(stats_mod, function_name)(vector._storage) is not DECLINED


@pytest.mark.parametrize('method', ['exclusive', 'inclusive'])
@pytest.mark.parametrize(
    'values',
    [
        [1, None, 2, 4, 8, 16],
        [1.0, None, 2.0, 4.0, 8.0, 16.0],
    ],
)
def test_quantiles_match_python_exactly(values, method):
    vector = Vector(values)
    fast = vector.stats.quantiles(n=7, method=method)
    pure = _pure(lambda: vector.stats.quantiles(n=7, method=method))

    assert fast == pure
    assert all(type(value) is float for value in fast)
    assert stats_mod.quantiles(
        vector._storage,
        n=7,
        method=method,
    ) is not DECLINED


@pytest.mark.parametrize('function_name', ['mean', 'fmean'])
def test_float_averages_are_numerically_conformant(function_name):
    vector = Vector([1.5, None, 2.25, 8.0, -3.0])
    fast = getattr(vector.stats, function_name)()
    pure = _pure(lambda: getattr(vector.stats, function_name)())

    assert math.isclose(fast, pure, rel_tol=1e-15, abs_tol=0.0)
    assert type(fast) is float


def test_integer_fmean_accelerates_but_mean_preserves_pure_typing():
    vector = Vector([1, None, 2, 3])

    assert stats_mod.mean(vector._storage) is DECLINED
    assert type(vector.stats.mean()) is int
    assert vector.stats.mean() == statistics.mean([1, 2, 3])
    assert stats_mod.fmean(vector._storage) is not DECLINED
    assert vector.stats.fmean() == _pure(lambda: vector.stats.fmean())
    assert type(vector.stats.fmean()) is float


@pytest.mark.parametrize(
    'function_name',
    ['pvariance', 'variance', 'pstdev', 'stdev'],
)
def test_float_dispersion_is_numerically_conformant(function_name):
    vector = Vector([1.5, None, 2.25, 8.0, -3.0])
    fast = getattr(vector.stats, function_name)()
    pure = _pure(lambda: getattr(vector.stats, function_name)())

    assert math.isclose(fast, pure, rel_tol=1e-14, abs_tol=0.0)
    assert type(fast) is float
    assert getattr(stats_mod, function_name)(
        vector._storage,
    ) is not DECLINED


def test_float_dispersion_accepts_a_precomputed_center():
    vector = Vector([1.5, None, 2.25, 8.0, -3.0])
    center = vector.stats.mean()

    assert math.isclose(
        vector.stats.variance(center),
        _pure(lambda: vector.stats.variance(center)),
        rel_tol=1e-14,
    )
    assert math.isclose(
        vector.stats.pstdev(center),
        _pure(lambda: vector.stats.pstdev(center)),
        rel_tol=1e-14,
    )


def test_integer_dispersion_declines_to_preserve_exact_result_types():
    vector = Vector([1, 2, 3])

    for function_name in ('pvariance', 'variance', 'pstdev', 'stdev'):
        assert getattr(stats_mod, function_name)(
            vector._storage,
        ) is DECLINED
        assert getattr(vector.stats, function_name)() == _pure(
            lambda: getattr(vector.stats, function_name)()
        )


def test_empty_and_all_null_statistics_match_pure_semantics():
    empty = Vector([], dtype=float)
    all_null = Vector([None, None], dtype=Schema(float, True))

    for vector in (empty, all_null):
        for function_name in (
            'mean',
            'fmean',
            'median',
            'median_low',
            'median_high',
            'pvariance',
            'variance',
            'pstdev',
            'stdev',
        ):
            assert getattr(vector.stats, function_name)() is None
            assert getattr(vector.stats, function_name)() == _pure(
                lambda: getattr(vector.stats, function_name)()
            )
        assert vector.stats.quantiles() is None


def test_nan_and_nonfinite_cases_decline_to_python():
    nan_vector = Vector([1.0, float('nan'), 3.0])
    mixed_infinity = Vector([float('inf'), float('-inf')])

    for function_name in ('median', 'median_low', 'median_high'):
        assert getattr(stats_mod, function_name)(
            nan_vector._storage,
        ) is DECLINED
    assert stats_mod.quantiles(nan_vector._storage) is DECLINED
    assert stats_mod.mean(mixed_infinity._storage) is DECLINED
    assert stats_mod.fmean(mixed_infinity._storage) is DECLINED
    for function_name in ('pvariance', 'variance', 'pstdev', 'stdev'):
        assert getattr(stats_mod, function_name)(
            mixed_infinity._storage,
        ) is DECLINED


def test_fast_paths_engage_and_decline_where_designed(monkeypatch):
    calls = []
    original_median = stats_mod.median
    original_mean = stats_mod.mean

    def median_spy(storage):
        result = original_median(storage)
        calls.append(('median', result is not DECLINED))
        return result

    def mean_spy(storage):
        result = original_mean(storage)
        calls.append(('mean', result is not DECLINED))
        return result

    monkeypatch.setattr(stats_mod, 'median', median_spy)
    monkeypatch.setattr(stats_mod, 'mean', mean_spy)
    Vector([1, None, 3]).stats.median()
    Vector([1.0, None, 3.0]).stats.mean()
    Vector([1, 2, 3]).stats.mean()
    assert calls == [
        ('median', True),
        ('mean', True),
        ('mean', False),
    ]
