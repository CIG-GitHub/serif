"""Optional Rust physical implementations for Vector operators.

Nullable int64 arithmetic in one compiled pass: validity, the operation and
the overflow check happen together. The kernel declines on any overflow in
a valid lane, so the pure path's widening to exact Python ints is unchanged.
"""

from __future__ import annotations

import array as _pyarray
import operator as _op

from ..._execution import DECLINED
from . import _rs
from . import _USE_RUST
from ..nullable import BitMask
from ..operators import _reverse_add
from ..operators import _reverse_floordiv
from ..operators import _reverse_mod
from ..operators import _reverse_sub
from ..storage import ArrayStorage


_I64_MAX = 2**63 - 1
_I64_MIN = -2**63

# op_func -> (kernel code, operands swapped)
_INT_OPS = {
    _op.add: (0, False),
    _reverse_add: (0, True),
    _op.sub: (1, False),
    _reverse_sub: (1, True),
    _op.mul: (2, False),
    _op.floordiv: (3, False),
    _reverse_floordiv: (3, True),
    _op.mod: (4, False),
    _reverse_mod: (4, True),
}


def _int_column(storage):
    if isinstance(storage, ArrayStorage) and storage._data.typecode == 'q':
        return storage._data, storage._mask
    return None


def _operand(rhs):
    """Return (values, mask) for an int64 column or scalar, else None."""
    if isinstance(rhs, ArrayStorage):
        return _int_column(rhs)
    if type(rhs) is int and _I64_MIN <= rhs <= _I64_MAX:
        return rhs, None
    return None


def binop_storage(lhs_storage, rhs, op_func, result_kind):
    """Return int64 arithmetic Serif storage or DECLINED."""
    if not _USE_RUST or result_kind is not int:
        return DECLINED
    spec = _INT_OPS.get(op_func)
    if spec is None:
        return DECLINED
    left = _int_column(lhs_storage)
    right = _operand(rhs)
    if left is None or right is None:
        return DECLINED
    code, swapped = spec
    if swapped:
        left, right = right, left
    (left_values, left_mask), (right_values, right_mask) = left, right

    n = len(lhs_storage)
    out = _pyarray.array('q', [0]) * n
    out_mask = None
    if left_mask is not None or right_mask is not None:
        out_mask = bytearray((n + 7) // 8)

    ok = _rs.int_binary(
        code,
        left_values,
        None if left_mask is None else left_mask._buf,
        right_values,
        None if right_mask is None else right_mask._buf,
        out,
        out_mask,
    )
    if not ok:
        return DECLINED
    mask = None if out_mask is None else BitMask(out_mask, n)
    return ArrayStorage(out, mask)
