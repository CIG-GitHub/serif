"""Optional NumPy physical implementations for Vector reductions."""

from __future__ import annotations

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import valid_values
from ..storage import ArrayStorage


_U64 = 2**64
_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1


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
        return 0.0
    return float(values.sum())


def gcd(storage):
    """Return the exact integer gcd, or ``DECLINED``."""
    values = _prepared(storage)
    if values is None or values.dtype.kind != 'i':
        return DECLINED
    if values.size == 0:
        return 0
    # abs(-2**63) is not representable in int64. Keep that exceptional
    # lane on the canonical Python path instead of relying on NumPy's
    # platform/version-specific overflow result.
    if (values == _I64_MIN).any():
        return DECLINED
    return int(_np.gcd.reduce(values))


def _magnitude_product_fits_i64(values):
    """Whether the product of the nontrivial magnitudes fits int64."""
    factors = values[values > 1]
    # At least 63 factors of magnitude two already exceed signed int64.
    # This also caps the scalar proof below at 62 iterations.
    if factors.size > 62:
        return False
    product = 1
    for factor in factors:
        factor = int(factor)
        if product > _I64_MAX // factor:
            return False
        product *= factor
    return True


def lcm(storage):
    """Return the exact integer lcm, or ``DECLINED``."""
    values = _prepared(storage)
    if values is None or values.dtype.kind != 'i':
        return DECLINED
    if values.size == 0:
        return 1
    if (values == 0).any():
        return 0
    if (values == _I64_MIN).any():
        return DECLINED

    # Repeated magnitudes do not change an lcm. Their distinct product is
    # an upper bound on the final result and therefore on every intermediate
    # result of the reduction.
    magnitudes = _np.unique(_np.abs(values))
    if not _magnitude_product_fits_i64(magnitudes):
        return DECLINED
    # NumPy leaves a singleton reduction untouched, while math.lcm(-n)
    # normalizes its result to positive. _I64_MIN was declined above, so
    # taking the Python-int absolute value is exact.
    return abs(int(_np.lcm.reduce(values)))


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
