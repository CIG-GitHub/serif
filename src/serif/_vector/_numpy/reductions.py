"""Optional NumPy physical implementations for Vector reductions."""

from __future__ import annotations

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import valid_values
from ..storage import ArrayStorage


_U64 = 2**64


def _prepared(storage):
    """Return supported non-null lanes, or internal ``None`` decline."""
    if not _USE_NUMPY or not isinstance(storage, ArrayStorage):
        return None
    return valid_values(storage)


def _int_sum_or_declined(values):
    """Recover an exact integer sum, or return ``DECLINED``."""
    n = int(values.size)
    if n == 0:
        return 0
    minimum = int(values.min())
    maximum = int(values.max())
    if n * (maximum - minimum) >= _U64:
        return DECLINED
    wrapped = int(values.sum())
    spread_sum = (wrapped - n * minimum) % _U64
    return n * minimum + spread_sum


def sum_(storage):
    values = _prepared(storage)
    if values is None:
        return DECLINED
    if values.dtype.kind == 'i':
        return _int_sum_or_declined(values)
    if values.size == 0:
        return 0
    return float(values.sum())


def _minmax(storage, numpy_reduce):
    values = _prepared(storage)
    if values is None:
        return DECLINED
    if values.size == 0:
        return None
    result = numpy_reduce(values)
    if values.dtype.kind == 'i':
        return int(result)
    if _np.isnan(result):
        return DECLINED
    return float(result)


def min_(storage):
    if not _USE_NUMPY:
        return DECLINED
    return _minmax(storage, _np.min)


def max_(storage):
    if not _USE_NUMPY:
        return DECLINED
    return _minmax(storage, _np.max)


def mean(storage):
    values = _prepared(storage)
    if values is None:
        return DECLINED
    if values.size == 0:
        return None
    if values.dtype.kind == 'i':
        total = _int_sum_or_declined(values)
        if total is DECLINED:
            return DECLINED
        return total / int(values.size)
    return float(values.sum()) / int(values.size)


def stdev(storage, population=False):
    values = _prepared(storage)
    if values is None:
        return DECLINED
    n = int(values.size)
    if n < 2:
        return None
    if values.dtype.kind == 'i':
        total = _int_sum_or_declined(values)
        if total is DECLINED:
            return DECLINED
        mean_value = total / n
    else:
        mean_value = float(values.sum()) / n
    deviations = values - mean_value
    numerator = float((deviations * deviations).sum())
    return (numerator / (n - 1 + population)) ** 0.5
