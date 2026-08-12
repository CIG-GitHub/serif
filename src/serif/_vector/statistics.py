"""Vector-aware access to Python's :mod:`statistics` functions."""

import statistics as _statistics
from collections.abc import Iterable

from .._execution import DECLINED
from ..errors import SerifTypeError
from ..errors import SerifValueError


def _known_values(vector):
    """Materialize the non-null observations for one statistic."""
    return [value for value in vector._storage if value is not None]


def _numpy_statistics():
    from ._numpy import statistics

    return statistics


def _univariate(
    vector,
    function_name,
    minimum=1,
    args=(),
    kwargs=None,
    *,
    accelerated=False,
):
    """Apply a canonical statistics function after Serif null stripping."""
    kwargs = kwargs or {}
    if accelerated:
        fast = getattr(_numpy_statistics(), function_name)(
            vector._storage,
            *args,
            **kwargs,
        )
        if fast is not DECLINED:
            return fast
    values = _known_values(vector)
    if len(values) < minimum:
        return None
    function = getattr(_statistics, function_name)
    return function(values, *args, **kwargs)


def _vector_class():
    from ..vector import Vector
    return Vector


def _paired_values(vector, other, function_name, other_name):
    """Validate dimensions, then retain pairs known on both sides."""
    Vector = _vector_class()
    if isinstance(other, Vector):
        if other.ndims() != 1:
            raise SerifTypeError(
                f"stats.{function_name}() cannot pair a Vector with a Table"
            )
        other_values = other._storage
    elif isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        other_values = tuple(other)
    else:
        raise SerifTypeError(
            f"stats.{function_name}() requires {other_name} to be a "
            "Vector or iterable"
        )

    if len(vector) != len(other_values):
        raise SerifValueError(
            f"Length mismatch: {len(vector)} != {len(other_values)}"
        )

    left = []
    right = []
    for left_value, right_value in zip(
        vector._storage,
        other_values,
        strict=True,
    ):
        if left_value is None or right_value is None:
            continue
        left.append(left_value)
        right.append(right_value)
    return left, right


def _bivariate(vector, other, function_name):
    left, right = _paired_values(
        vector,
        other,
        function_name,
        "another sample",
    )
    if len(left) < 2:
        return None
    return getattr(_statistics, function_name)(left, right)


class StatisticsAccessor:
    """Whole-Vector statistics with Python 3.10 semantics."""

    __slots__ = ('_vector',)
    _serif_accessor_name = 'stats'

    def __init__(self, vector):
        self._vector = vector

    @property
    def _serif_bound_vector(self):
        """Source Vector used by aggregate()/window() bound dispatch."""
        return self._vector

    def mean(self):
        return _univariate(self._vector, 'mean', accelerated=True)

    def fmean(self):
        return _univariate(self._vector, 'fmean', accelerated=True)

    def geometric_mean(self):
        return _univariate(self._vector, 'geometric_mean')

    def harmonic_mean(self, weights=None):
        if weights is None:
            return _univariate(self._vector, 'harmonic_mean')
        values, known_weights = _paired_values(
            self._vector,
            weights,
            'harmonic_mean',
            'weights',
        )
        if not values:
            return None
        return _statistics.harmonic_mean(values, weights=known_weights)

    def median(self):
        return _univariate(self._vector, 'median', accelerated=True)

    def median_low(self):
        return _univariate(self._vector, 'median_low', accelerated=True)

    def median_high(self):
        return _univariate(self._vector, 'median_high', accelerated=True)

    def mode(self):
        return _univariate(self._vector, 'mode')

    def multimode(self):
        return _statistics.multimode(_known_values(self._vector))

    def quantiles(self, *, n=4, method='exclusive'):
        # Preserve explicit argument errors even when null stripping leaves
        # too few observations to calculate cut points.
        if n < 1:
            raise _statistics.StatisticsError('n must be at least 1')
        if method not in ('exclusive', 'inclusive'):
            raise ValueError(f'Unknown method: {method!r}')
        return _univariate(
            self._vector,
            'quantiles',
            minimum=2,
            kwargs={'n': n, 'method': method},
            accelerated=True,
        )

    def pvariance(self, mu=None):
        return _univariate(
            self._vector,
            'pvariance',
            args=(mu,),
            accelerated=True,
        )

    def variance(self, xbar=None):
        return _univariate(
            self._vector,
            'variance',
            minimum=2,
            args=(xbar,),
            accelerated=True,
        )

    def pstdev(self, mu=None):
        return _univariate(
            self._vector,
            'pstdev',
            args=(mu,),
            accelerated=True,
        )

    def stdev(self, xbar=None):
        return _univariate(
            self._vector,
            'stdev',
            minimum=2,
            args=(xbar,),
            accelerated=True,
        )

    def covariance(self, other):
        return _bivariate(self._vector, other, 'covariance')

    def correlation(self, other):
        return _bivariate(self._vector, other, 'correlation')

    def linear_regression(self, other):
        return _bivariate(self._vector, other, 'linear_regression')


__all__ = ['StatisticsAccessor']
