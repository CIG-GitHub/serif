"""Optional Arrow physical implementations for Vector operators."""

import operator as _op

from ..._execution import DECLINED
from . import _pa
from . import _pc
from . import _USE_ARROW
from .storage import bool_storage
from .storage import int64_array
from .storage import numeric_array
from .storage import numeric_storage
from .storage import string_array
from ..storage import ArrayStorage
from ..storage import StringStorage


_I64_MAX = 2**63 - 1
_I64_MIN = -(2**63)

_ARITH_KERNELS = {
    _op.add: "add_checked",
    _op.sub: "subtract_checked",
    _op.mul: "multiply_checked",
}

_CMP_KERNELS = {
    _op.eq: "equal",
    _op.ne: "not_equal",
    _op.lt: "less",
    _op.le: "less_equal",
    _op.gt: "greater",
    _op.ge: "greater_equal",
}


def binop_ints(left, right, op_func, result_kind):
    """Apply a checked int64 arithmetic kernel, or decline."""
    if not _USE_ARROW or result_kind is not int:
        return DECLINED
    kernel_name = _ARITH_KERNELS.get(op_func)
    if kernel_name is None:
        return DECLINED
    left_array = int64_array(left)
    if left_array is DECLINED:
        return DECLINED
    if isinstance(right, ArrayStorage):
        right_operand = int64_array(right)
        if right_operand is DECLINED:
            return DECLINED
    elif type(right) is int and _I64_MIN <= right <= _I64_MAX:
        right_operand = _pa.scalar(right, type=_pa.int64())
    else:
        return DECLINED
    try:
        result = getattr(_pc, kernel_name)(left_array, right_operand)
    except _pa.ArrowInvalid:
        return DECLINED
    return numeric_storage(result)


def div_floats(left, right, op_func, result_kind):
    """Apply Arrow's floating-point division kernel, or decline."""
    if (
        not _USE_ARROW
        or op_func is not _op.truediv
        or result_kind is not float
    ):
        return DECLINED
    left_array = numeric_array(left)
    if left_array is DECLINED:
        return DECLINED
    left_is_int = left._data.typecode == 'q'
    if isinstance(right, ArrayStorage):
        right_operand = numeric_array(right)
        if right_operand is DECLINED:
            return DECLINED
        right_is_int = right._data.typecode == 'q'
    elif type(right) is float:
        if right == 0.0:
            return DECLINED
        right_operand = _pa.scalar(right, type=_pa.float64())
        right_is_int = False
    elif type(right) is int:
        if right == 0 or not (_I64_MIN <= right <= _I64_MAX):
            return DECLINED
        right_operand = _pa.scalar(right, type=_pa.int64())
        right_is_int = True
    else:
        return DECLINED
    if left_is_int and right_is_int:
        return DECLINED
    try:
        result = _pc.divide_checked(left_array, right_operand)
    except (_pa.ArrowInvalid, _pa.ArrowNotImplementedError):
        return DECLINED
    return numeric_storage(result)


def compare_strings(left, right, op_func):
    """Compare string storage with a string storage or scalar, or decline."""
    if not _USE_ARROW:
        return DECLINED
    kernel_name = _CMP_KERNELS.get(op_func)
    if kernel_name is None:
        return DECLINED
    left_array = string_array(left)
    if left_array is DECLINED:
        return DECLINED
    if isinstance(right, StringStorage):
        right_operand = string_array(right)
        if right_operand is DECLINED:
            return DECLINED
    elif type(right) is str:
        right_operand = right
    else:
        return DECLINED
    result = getattr(_pc, kernel_name)(left_array, right_operand)
    return bool_storage(result)
