"""
Reductions over storage buffers: sum, min, max, mean, stdev.

Each function mirrors its pure counterpart in _vector/reductions.py structurally —
compress out the null lanes, then reduce — and returns either a concrete
Python scalar or DECLINED, in which case the pure path runs and its
behavior is the specification. Scope: ArrayStorage 'q'/'d' only.
BoolStorage and object storages decline (the pure path handles them).

The three correctness rules this module exists to uphold:

  * Never silently wrong — int64 sums WRAP in numpy while pure Python
    promotes to bigint. But numpy's integer arithmetic is documented
    MODULAR, and modular addition is order-blind: one wrapping np.sum
    IS the true sum mod 2**64, whatever the intermediate partial sums
    did. The max/min the guard must compute anyway confine the true sum
    to a window of width n·(max−min); when that window is narrower than
    2**64 the residue pins the exact bigint total — including totals
    far OUTSIDE int64 (clustered data like timestamp columns, provided
    n·spread stays under 2**64: ~200k rows of day-wide nanoseconds; a
    wider spread or longer column still declines — an ESTIMATE-centered
    window could shrink that band further, a game not yet played).
    Integer sum/mean/stdev DECLINE only when the residue is ambiguous.

  * NaN-poisoned min/max decline — np.min/max return nan when any lane
    is nan, but Python's min/max are ORDER-DEPENDENT under nan (nan
    comparisons are all False). The pure path is the spec, incoherent or
    not, so the accelerator steps aside rather than "fix" it.

  * Float reductions are exempt from bit-identity (agreed doctrine).
    The pure path uses math.fsum on every supported Python version — often
    exactly rounded, and under catastrophic absorption MORE accurate than
    numpy's pairwise summation (O(log n)·ULP). The
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

_U64 = 2**64


def _prepared(storage):
    """Compressed valid-lane view, or None to decline."""
    if _np is None or not isinstance(storage, ArrayStorage):
        return None
    return valid_values(storage)


def _int_sum_or_declined(vals):
    """Exact integer sum, or DECLINED when the residue is ambiguous.

        true_sum = n·min + S,   S = Σ(v − min) ∈ [0, n·(max − min)]

    np.sum wraps, but modular addition is associative, so the wrapped
    result is the true sum mod 2**64 regardless of intermediate wraps
    or reduction order. When the window n·(max − min) is narrower than
    2**64, S is recovered from that residue and the exact bigint total
    follows — one sum pass on top of the max/min the guard needs
    anyway, no extra allocation, and no int64 ceiling on the TOTAL,
    only on the SPREAD. int() conversions happen before any Python
    arithmetic — np.int64 would wrap the very bigints this exists for.
    """
    n = int(vals.size)
    if n == 0:
        return 0
    mn = int(vals.min())
    mx = int(vals.max())
    if n * (mx - mn) >= _U64:
        return DECLINED            # spread past 2**64/n — pure is the spec
    wrapped = int(vals.sum())      # ≡ true sum (mod 2**64), signed carrier
    s = (wrapped - n * mn) % _U64  # exact: S is confined to the window
    return n * mn + s


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
