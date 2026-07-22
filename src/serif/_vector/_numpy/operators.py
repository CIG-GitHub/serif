"""Optional NumPy physical implementations for Vector operators."""

from __future__ import annotations

import array as _pyarray
import operator as _op

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import NP_DTYPES
from .storage import valid_bits
from ..nullable import BitMask
from ..storage import ArrayStorage
from ..storage import BoolStorage


_I64_MAX = 2**63 - 1
_I64_MIN = -2**63
_EXACT_F64_INT = 2**53

_ARITH = {
    _op.add: 'add',
    _op.sub: 'subtract',
    _op.mul: 'multiply',
    _op.truediv: 'true_divide',
    _op.floordiv: 'floor_divide',
    _op.mod: 'mod',
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
    if not isinstance(storage, ArrayStorage):
        return None
    np_dtype = NP_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return None
    return _np.frombuffer(storage._data, dtype=np_dtype), storage._mask


def _operands(lhs_storage, rhs):
    left = _view(lhs_storage)
    if left is None:
        return None
    left_values, left_mask = left

    if isinstance(rhs, ArrayStorage):
        right = _view(rhs)
        if right is None:
            return None
        right_values, right_mask = right
        return (
            left_values,
            left_mask,
            right_values,
            right_mask,
            right_values.dtype.kind == 'i',
        )

    if type(rhs) is int:
        if not (_I64_MIN <= rhs <= _I64_MAX):
            return None
        return left_values, left_mask, rhs, None, True
    if type(rhs) is float:
        return left_values, left_mask, rhs, None, False
    return None


def _bounds(values):
    if isinstance(values, int):
        return values, values
    if values.size == 0:
        return 0, 0
    return int(values.min()), int(values.max())


def _int_arith_safe(op_func, left, right):
    left_low, left_high = _bounds(left)
    right_low, right_high = _bounds(right)
    if op_func is _op.add:
        return (
            left_low + right_low >= _I64_MIN
            and left_high + right_high <= _I64_MAX
        )
    if op_func is _op.sub:
        return (
            left_low - right_high >= _I64_MIN
            and left_high - right_low <= _I64_MAX
        )
    if op_func is _op.mul:
        peak = (
            max(abs(left_low), abs(left_high))
            * max(abs(right_low), abs(right_high))
        )
        return peak <= _I64_MAX
    if op_func in (_op.floordiv, _op.mod):
        return left_low > _I64_MIN
    return True


def _combined_valid(left_mask, right_mask, n):
    if left_mask is None and right_mask is None:
        return None, None
    if left_mask is None:
        buffer = bytearray(right_mask._buf)
    elif right_mask is None:
        buffer = bytearray(left_mask._buf)
    else:
        left = _np.frombuffer(left_mask._buf, dtype=_np.uint8)
        right = _np.frombuffer(right_mask._buf, dtype=_np.uint8)
        buffer = bytearray((left & right).tobytes())
    mask = BitMask(buffer, n)
    return mask, valid_bits(mask, n)


def binop_storage(lhs_storage, rhs, op_func, result_kind):
    """Return arithmetic Serif storage or DECLINED."""
    if not _USE_NUMPY or result_kind not in (int, float):
        return DECLINED
    numpy_name = _ARITH.get(op_func)
    if numpy_name is None:
        return DECLINED
    prepared = _operands(lhs_storage, rhs)
    if prepared is None:
        return DECLINED
    left, left_mask, right, right_mask, right_is_int = prepared
    left_is_int = left.dtype.kind == 'i'
    n = len(left)

    mask, valid = _combined_valid(left_mask, right_mask, n)

    if left_is_int and right_is_int:
        if op_func is _op.truediv:
            left_low, left_high = _bounds(left)
            right_low, right_high = _bounds(right)
            if max(
                abs(left_low),
                abs(left_high),
                abs(right_low),
                abs(right_high),
            ) > _EXACT_F64_INT:
                return DECLINED
        elif not _int_arith_safe(op_func, left, right):
            return DECLINED

    if op_func in _DIV_OPS:
        if isinstance(right, (int, float)):
            if right == 0:
                return DECLINED
            effective_right = right
        else:
            effective_right = (
                right
                if valid is None
                else _np.where(valid, right, 1)
            )
            if (effective_right == 0).any():
                return DECLINED
    else:
        effective_right = right

    output = getattr(_np, numpy_name)(left, effective_right)

    typecode = 'q' if result_kind is int else 'd'
    expected = _np.int64 if result_kind is int else _np.float64
    if output.dtype != expected:
        return DECLINED
    data = _pyarray.array(typecode)
    data.frombytes(output.tobytes())
    return ArrayStorage(data, mask)


def _bool_view(storage):
    if not isinstance(storage, BoolStorage):
        return None
    values = _np.frombuffer(storage._data, dtype=_np.bool_)
    known = (
        None
        if storage._mask is None
        else valid_bits(storage._mask, len(values))
    )
    return values, known


def logical_storage(lhs_storage, rhs, op_name):
    """Return Kleene-logical BoolStorage or DECLINED."""
    if not _USE_NUMPY:
        return DECLINED
    left = _bool_view(lhs_storage)
    if left is None:
        return DECLINED
    left_values, left_known = left
    n = len(left_values)

    if isinstance(rhs, BoolStorage):
        right_values, right_known = _bool_view(rhs)
    elif rhs is None:
        right_values = _np.zeros(n, dtype=_np.bool_)
        right_known = _np.zeros(n, dtype=_np.bool_)
    elif type(rhs) is bool:
        right_values = _np.full(n, rhs, dtype=_np.bool_)
        right_known = None
    else:
        return DECLINED

    if op_name not in ('and', 'or', 'xor'):
        return DECLINED

    if left_known is None and right_known is None:
        if op_name == 'and':
            value = left_values & right_values
        elif op_name == 'or':
            value = left_values | right_values
        else:
            value = left_values ^ right_values
        return BoolStorage(bytearray(value.tobytes()), None)

    left_known = (
        _np.ones(n, dtype=_np.bool_)
        if left_known is None
        else left_known
    )
    right_known = (
        _np.ones(n, dtype=_np.bool_)
        if right_known is None
        else right_known
    )
    left_effective = left_values & left_known
    right_effective = right_values & right_known

    if op_name == 'and':
        valid = (
            (left_known & right_known)
            | (left_known & ~left_effective)
            | (right_known & ~right_effective)
        )
        value = left_effective & right_effective
    elif op_name == 'or':
        valid = (left_known & right_known) | left_effective | right_effective
        value = left_effective | right_effective
    else:
        valid = left_known & right_known
        value = (left_values ^ right_values) & valid

    value = value & valid
    if valid.all():
        return BoolStorage(bytearray(value.tobytes()), None)
    packed = _np.packbits(valid, bitorder='little')
    return BoolStorage(
        bytearray(value.tobytes()),
        BitMask(bytearray(packed.tobytes()), n),
    )


def invert_storage(storage):
    """Return Kleene-NOT BoolStorage or DECLINED."""
    if not _USE_NUMPY:
        return DECLINED
    left = _bool_view(storage)
    if left is None:
        return DECLINED
    values, known = left
    if known is None or known.all():
        value = ~values if known is None else (~values) & known
        return BoolStorage(bytearray(value.tobytes()), None)
    value = (~values) & known
    packed = _np.packbits(known, bitorder='little')
    return BoolStorage(
        bytearray(value.tobytes()),
        BitMask(bytearray(packed.tobytes()), len(values)),
    )


def compare_storage(lhs_storage, rhs, op_func):
    """Return comparison BoolStorage or DECLINED."""
    if not _USE_NUMPY:
        return DECLINED
    numpy_name = _CMP.get(op_func)
    if numpy_name is None:
        return DECLINED
    prepared = _operands(lhs_storage, rhs)
    if prepared is None:
        return DECLINED
    left, left_mask, right, right_mask, right_is_int = prepared
    left_is_int = left.dtype.kind == 'i'
    n = len(left)

    if left_is_int != right_is_int:
        low, high = _bounds(left if left_is_int else right)
        if max(abs(low), abs(high)) > _EXACT_F64_INT:
            return DECLINED

    output = getattr(_np, numpy_name)(left, right)
    mask, _ = _combined_valid(left_mask, right_mask, n)
    return BoolStorage(bytearray(output.tobytes()), mask)
