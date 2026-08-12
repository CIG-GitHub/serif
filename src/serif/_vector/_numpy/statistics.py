"""Optional NumPy implementations for Vector statistics."""

import math

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import valid_values
from ..storage import ArrayStorage


def _prepared(storage):
    if not _USE_NUMPY or not isinstance(storage, ArrayStorage):
        return None
    return valid_values(storage)


def _has_nan(values):
    return values.dtype.kind == 'f' and _np.isnan(values).any()


def _python_scalar(value, kind):
    return int(value) if kind == 'i' else float(value)


def mean(storage):
    """Accelerate float mean; integer mean retains exact Python typing."""
    values = _prepared(storage)
    if (
        values is None
        or values.dtype.kind != 'f'
        or not _np.isfinite(values).all()
    ):
        return DECLINED
    if values.size == 0:
        return None
    return float(values.mean())


def fmean(storage):
    values = _prepared(storage)
    if values is None:
        return DECLINED
    if values.dtype.kind == 'f' and not _np.isfinite(values).all():
        return DECLINED
    if values.size == 0:
        return None
    return float(values.mean())


def _ordered(storage):
    values = _prepared(storage)
    if values is None or _has_nan(values):
        return None
    # Python's sort is stable. Preserve the first signed zero and any other
    # equal-value ordering observed by median_low()/median_high().
    return _np.sort(values, kind='stable')


def median(storage):
    ordered = _ordered(storage)
    if ordered is None:
        return DECLINED
    n = int(ordered.size)
    if n == 0:
        return None
    middle = n // 2
    kind = ordered.dtype.kind
    if n % 2:
        return _python_scalar(ordered[middle], kind)
    left = _python_scalar(ordered[middle - 1], kind)
    right = _python_scalar(ordered[middle], kind)
    return (left + right) / 2


def median_low(storage):
    ordered = _ordered(storage)
    if ordered is None:
        return DECLINED
    n = int(ordered.size)
    if n == 0:
        return None
    return _python_scalar(ordered[(n - 1) // 2], ordered.dtype.kind)


def median_high(storage):
    ordered = _ordered(storage)
    if ordered is None:
        return DECLINED
    n = int(ordered.size)
    if n == 0:
        return None
    return _python_scalar(ordered[n // 2], ordered.dtype.kind)


def quantiles(storage, *, n=4, method='exclusive'):
    """Use NumPy sorting with Python 3.10's interpolation formulas."""
    ordered = _ordered(storage)
    if ordered is None:
        return DECLINED
    length = int(ordered.size)
    if length < 2:
        return None
    kind = ordered.dtype.kind

    if method == 'inclusive':
        scale = length - 1
        cuts = []
        for index in range(1, n):
            lower, delta = divmod(index * scale, n)
            left = _python_scalar(ordered[lower], kind)
            right = _python_scalar(ordered[lower + 1], kind)
            cuts.append((left * (n - delta) + right * delta) / n)
        return cuts

    scale = length + 1
    cuts = []
    for index in range(1, n):
        lower = index * scale // n
        lower = min(max(lower, 1), length - 1)
        delta = index * scale - lower * n
        left = _python_scalar(ordered[lower - 1], kind)
        right = _python_scalar(ordered[lower], kind)
        cuts.append((left * (n - delta) + right * delta) / n)
    return cuts


def _dispersion(storage, center, *, sample, root):
    values = _prepared(storage)
    if values is None or values.dtype.kind != 'f':
        return DECLINED
    minimum = 2 if sample else 1
    size = int(values.size)
    if size < minimum:
        return None
    if not _np.isfinite(values).all():
        return DECLINED
    if center is not None:
        if type(center) not in (int, float):
            return DECLINED
        try:
            if not math.isfinite(center):
                return DECLINED
            mean_value = float(center)
        except OverflowError:
            return DECLINED
    else:
        mean_value = float(values.mean())
    deviations = values - mean_value
    result = float((deviations * deviations).sum()) / (
        size - (1 if sample else 0)
    )
    return result ** 0.5 if root else result


def pvariance(storage, mu=None):
    return _dispersion(storage, mu, sample=False, root=False)


def variance(storage, xbar=None):
    return _dispersion(storage, xbar, sample=True, root=False)


def pstdev(storage, mu=None):
    return _dispersion(storage, mu, sample=False, root=True)


def stdev(storage, xbar=None):
    return _dispersion(storage, xbar, sample=True, root=True)
