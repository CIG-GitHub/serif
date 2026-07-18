"""
Reductions over storage buffers: sum, min, max, mean, stdev.

Each function mirrors its pure counterpart in base.py structurally —
compress out the null lanes, then reduce — and returns either a concrete
Python scalar or DECLINED, in which case the pure path runs and its
behavior is the specification. Scope: ArrayStorage 'q'/'d' only.
BoolStorage and object storages decline (the pure path handles them).

The three correctness rules this module exists to uphold:

  * Never silently wrong — int64 sums WRAP in numpy while pure Python
    promotes to bigint, so integer sum/mean/stdev run a cheap bounds pass
    first (n · max|v| < 2**63) and DECLINE when overflow is possible.

  * NaN-poisoned min/max decline — np.min/max return nan when any lane
    is nan, but Python's min/max are ORDER-DEPENDENT under nan (nan
    comparisons are all False). The pure path is the spec, incoherent or
    not, so the accelerator steps aside rather than "fix" it.

  * Float reductions are exempt from bit-identity (agreed doctrine).
    The pure path's builtin sum() is Neumaier-COMPENSATED on CPython
    >= 3.12 — often exactly rounded, and under catastrophic absorption
    MORE accurate than numpy's pairwise summation (O(log n)·ULP). The
    trade is explicit: the fast path buys speed at the cost of the last
    ULPs in pathological data; both paths stay within tight bounds of
    math.fsum, which is what the conformance tests anchor against —
    never one path against the other. Integer sums, and integer means
    (exact bigint sum ÷ n happens in Python on both paths), remain
    bit-identical.
"""

from __future__ import annotations

from . import _np, DECLINED, valid_values
from .._vector.storage import ArrayStorage

_I64_MAX = 2**63 - 1


def _prepared(storage):
    """Compressed valid-lane view, or None to decline."""
    if _np is None or not isinstance(storage, ArrayStorage):
        return None
    return valid_values(storage)


def _int_sum_or_declined(vals):
    """Exact integer sum, or DECLINED when int64 accumulation could wrap.

    |any partial sum| <= n · max|v|, so the guard bounds every intermediate
    numpy accumulates, not just the result. abs() happens on Python ints —
    abs(np.int64.min) overflows back to itself in numpy.
    """
    if vals.size == 0:
        return 0
    peak = max(abs(int(vals.max())), abs(int(vals.min())))
    if peak and vals.size > _I64_MAX // peak:
        return DECLINED
    return int(vals.sum())


def sum_(storage):
    vals = _prepared(storage)
    if vals is None:
        return DECLINED
    if vals.dtype.kind == 'i':
        return _int_sum_or_declined(vals)
    if vals.size == 0:
        return 0            # pure sum(()) is the int 0, not 0.0
    return float(vals.sum())   # nan propagates, same as the pure left-fold


def _minmax(storage, np_reduce):
    vals = _prepared(storage)
    if vals is None:
        return DECLINED
    if vals.size == 0:
        return None
    result = np_reduce(vals)
    if vals.dtype.kind == 'i':
        return int(result)
    if _np.isnan(result):
        # nan present: np.min/max say nan, pure min/max are order-dependent.
        return DECLINED
    return float(result)


def min_(storage):
    return _minmax(storage, _np.min)


def max_(storage):
    return _minmax(storage, _np.max)


def mean(storage):
    vals = _prepared(storage)
    if vals is None:
        return DECLINED
    if vals.size == 0:
        return None
    if vals.dtype.kind == 'i':
        total = _int_sum_or_declined(vals)
        if total is DECLINED:
            return DECLINED
        return total / int(vals.size)   # exact int ÷ int in Python — bit-identical to pure
    return float(vals.sum()) / int(vals.size)


def stdev(storage, population=False):
    vals = _prepared(storage)
    if vals is None:
        return DECLINED
    n = int(vals.size)
    if n < 2:
        return None
    if vals.dtype.kind == 'i':
        total = _int_sum_or_declined(vals)
        if total is DECLINED:
            return DECLINED
        m = total / n
    else:
        m = float(vals.sum()) / n
    dev = vals - m                       # float64 either way
    num = float((dev * dev).sum())       # non-negative terms: well-conditioned
    return (num / (n - 1 + population)) ** 0.5
