"""
Elementwise binary ops over storage buffers: arithmetic and comparisons.

This is the BIT-IDENTICAL tier (unlike reduce.py's fsum-anchored tier):
each lane computes the same single IEEE operation the pure path computes,
so results must match exactly — and every case where numpy's fixed-width
model diverges from Python's numeric tower DECLINES to the pure path:

  * int overflow — Python ints promote (and serif degrades the result to
    TupleStorage via the OverflowError catch in numeric.py); np.int64
    wraps silently. Arithmetic on int lanes runs a bounds pass first
    (add/sub from the operands' min/max, mul from peak magnitudes) and
    declines when promotion is possible. Sentinel zeros in masked lanes
    only widen the bounds — over-declining is safe, wrapping is not.

  * division by zero — Python raises ZeroDivisionError (floats too);
    numpy yields inf/nan. Any zero divisor in a lane where the division
    actually EXECUTES (both operands valid) declines, and the pure path
    raises serif's error. Null-lane divisors are neutralized to 1 first:
    a zero under a null never divides in the pure path either.

  * int/float precision mixing — Python compares and divides int-vs-float
    EXACTLY at any magnitude; numpy converts int64→float64 first, which
    is lossy past 2**53. Mixed-kind compares and int/int true division
    decline when any integer magnitude exceeds 2**53.

Null semantics ride on buffers: result validity is valid_a & valid_b —
a bytewise AND of the two packed BitMask buffers, no unpacking. Values in
masked-out lanes are unobservable garbage (the mask covers them), which
is the same contract the pure path's 0-sentinels rely on.
"""

from __future__ import annotations

import array as _pyarray
import operator as _op

from . import _np, NP_DTYPES, valid_bits
from .._vector.nullable import BitMask
from .._vector.storage import ArrayStorage, BoolStorage

_I64_MAX = 2**63 - 1
_I64_MIN = -2**63
_EXACT_F64_INT = 2**53          # |int| beyond this loses precision as float64

_ARITH = {
    _op.add:      'add',
    _op.sub:      'subtract',
    _op.mul:      'multiply',
    _op.truediv:  'true_divide',
    _op.floordiv: 'floor_divide',
    _op.mod:      'mod',
}
_DIV_OPS = (_op.truediv, _op.floordiv, _op.mod)

_CMP = {
    _op.eq: 'equal',
    _op.ne: 'not_equal',
    _op.lt: 'less',
    _op.le: 'less_equal',
    _op.gt: 'greater',
    _op.ge: 'greater_equal',
}


def _view(storage):
    """ArrayStorage → (np view, mask) or None to decline."""
    if not isinstance(storage, ArrayStorage):
        return None
    np_dtype = NP_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return None
    return _np.frombuffer(storage._data, dtype=np_dtype), storage._mask


def _operands(lhs_storage, rhs):
    """Normalize (lhs_storage, rhs) → (va, ma, vb_or_scalar, mb, is_int_b).

    rhs may be an ArrayStorage or a Python int/float scalar (bool is an
    int subclass but bool columns/scalars never reach here — bool storage
    is BoolStorage and bool scalars decline upstream by promotion kind).
    Returns None to decline.
    """
    left = _view(lhs_storage)
    if left is None:
        return None
    va, ma = left

    if isinstance(rhs, ArrayStorage):
        right = _view(rhs)
        if right is None:
            return None
        vb, mb = right
        return va, ma, vb, mb, vb.dtype.kind == 'i'

    if type(rhs) is int:
        if not (_I64_MIN <= rhs <= _I64_MAX):
            return None            # doesn't fit int64 — pure path promotes
        return va, ma, rhs, None, True
    if type(rhs) is float:
        return va, ma, rhs, None, False
    return None


def _bounds(v):
    """(lo, hi) as Python ints — abs(np.int64.min) overflows in numpy."""
    if isinstance(v, int):
        return v, v
    if v.size == 0:
        return 0, 0
    return int(v.min()), int(v.max())


def _int_arith_safe(op_func, va_or_scalar, vb_or_scalar):
    """True when int lane arithmetic cannot leave int64."""
    lo_a, hi_a = _bounds(va_or_scalar)
    lo_b, hi_b = _bounds(vb_or_scalar)
    if op_func is _op.add:
        return lo_a + lo_b >= _I64_MIN and hi_a + hi_b <= _I64_MAX
    if op_func is _op.sub:
        return lo_a - hi_b >= _I64_MIN and hi_a - lo_b <= _I64_MAX
    if op_func is _op.mul:
        peak = max(abs(lo_a), abs(hi_a)) * max(abs(lo_b), abs(hi_b))
        return peak <= _I64_MAX
    if op_func in (_op.floordiv, _op.mod):
        # The one overflow: (-2**63) // -1. Cheaper to exclude the value
        # than to scan the divisor for -1.
        return lo_a > _I64_MIN
    return True


def _combined_valid(ma, mb, n):
    """(BitMask | None, np bool valid array | None) for the result.

    The packed AND is the whole trick: both masks share the LSB-first
    1=valid layout, so bytewise & of the buffers IS the lane-wise AND.
    """
    if ma is None and mb is None:
        return None, None
    if ma is None:
        buf = bytearray(mb._buf)
    elif mb is None:
        buf = bytearray(ma._buf)
    else:
        a = _np.frombuffer(ma._buf, dtype=_np.uint8)
        b = _np.frombuffer(mb._buf, dtype=_np.uint8)
        buf = bytearray((a & b).tobytes())
    mask = BitMask(buf, n)
    return mask, valid_bits(mask, n)


def binop_storage(lhs_storage, rhs, op_func, result_kind):
    """
    Arithmetic on buffers. rhs: ArrayStorage or int/float scalar.
    Returns an ArrayStorage, or None to decline.
    result_kind is the schema kind already resolved by
    _pre_compute_op_schema — the accelerator never re-derives semantics.
    """
    if _np is None or result_kind not in (int, float):
        return None
    np_name = _ARITH.get(op_func)
    if np_name is None:
        return None
    prepared = _operands(lhs_storage, rhs)
    if prepared is None:
        return None
    va, ma, vb, mb, b_is_int = prepared
    a_is_int = va.dtype.kind == 'i'
    n = len(va)

    mask, valid = _combined_valid(ma, mb, n)

    if a_is_int and b_is_int:
        if op_func is _op.truediv:
            # Python divides int/int exactly at any magnitude; float64
            # conversion is only exact through 2**53.
            lo_a, hi_a = _bounds(va)
            lo_b, hi_b = _bounds(vb)
            if max(abs(lo_a), abs(hi_a), abs(lo_b), abs(hi_b)) > _EXACT_F64_INT:
                return None
        elif not _int_arith_safe(op_func, va, vb):
            return None

    if op_func in _DIV_OPS:
        # Zero divisors: only lanes that actually DIVIDE (both valid) can
        # raise in the pure path; neutralize the rest to 1, then any zero
        # left means pure raises ZeroDivisionError — decline and let it.
        if isinstance(vb, (int, float)):
            if vb == 0:
                return None
            b_eff = vb
        else:
            b_eff = vb if valid is None else _np.where(valid, vb, 1)
            if (b_eff == 0).any():
                return None
    else:
        b_eff = vb

    out = getattr(_np, np_name)(va, b_eff)

    typecode = 'q' if result_kind is int else 'd'
    expected = _np.int64 if result_kind is int else _np.float64
    if out.dtype != expected:
        return None                # promotion surprise — pure path decides
    data = _pyarray.array(typecode)
    data.frombytes(out.tobytes())
    return ArrayStorage(data, mask)


def compare_storage(lhs_storage, rhs, op_func):
    """
    Comparison on buffers → BoolStorage, or None to decline.

    Same-kind compares are exact in both worlds. Mixed int/float declines
    past 2**53 — Python compares across the numeric tower exactly, numpy
    converts int64→float64 first ((2**53+1) == 2.0**53 must stay False).
    """
    if _np is None:
        return None
    np_name = _CMP.get(op_func)
    if np_name is None:
        return None
    prepared = _operands(lhs_storage, rhs)
    if prepared is None:
        return None
    va, ma, vb, mb, b_is_int = prepared
    a_is_int = va.dtype.kind == 'i'
    n = len(va)

    if a_is_int != b_is_int:
        lo, hi = _bounds(va if a_is_int else vb)
        if max(abs(lo), abs(hi)) > _EXACT_F64_INT:
            return None

    out = getattr(_np, np_name)(va, vb)   # np bool: one byte per lane
    mask, _ = _combined_valid(ma, mb, n)
    return BoolStorage(bytearray(out.tobytes()), mask)
