"""Vector pointwise operator semantics."""

import operator
import warnings
from collections.abc import Iterable

from .._accel.api import _accel_binop
from .._accel.api import _accel_compare
from .._accel.api import _accel_invert
from .._accel.api import _accel_logical
from ..errors import SerifTypeError
from ..errors import SerifValueError
from .dtype import Schema
from .dtype import infer_dtype
from .storage import ArrayStorage
from .storage import TupleStorage


def _vector_class():
    # Local import avoids a cycle while the concrete Vector delegates here.
    from ..vector import Vector
    return Vector


def _reverse_add(y, x):
    return x + y


def _reverse_sub(y, x):
    return x - y


def _reverse_truediv(y, x):
    return x / y


def _reverse_floordiv(y, x):
    return x // y


def _reverse_mod(y, x):
    return x % y


def _reverse_pow(y, x):
    return x ** y


# Kleene three-valued logic (see docs/null-semantics.md). The known operand
# may settle the result; otherwise unknown propagates.
def _kleene_and(x, y):
    if x is not None and not x:
        return False
    if y is not None and not y:
        return False
    if x is None or y is None:
        return None
    return True


def _kleene_or(x, y):
    if x is not None and x:
        return True
    if y is not None and y:
        return True
    if x is None or y is None:
        return None
    return False


def _kleene_xor(x, y):
    # No settling operand for xor: unknown with anything is unknown.
    if x is None or y is None:
        return None
    return bool(x) != bool(y)


# Accelerator dispatch: only these functions have a vectorized twin. Anything
# else passed to logical_elementwise declines to the pure zip.
_KLEENE_OP_NAMES = {
    _kleene_and: 'and',
    _kleene_or: 'or',
    _kleene_xor: 'xor',
}


def _pre_compute_op_schema(lhs_schema, rhs, op_func=None):
    """Resolve a binary operation's output Schema without touching data.

    Returns None when the result type cannot be determined from types alone,
    including object dtype, temporal operations, and unknown right-hand kinds.
    """
    from .numeric import _KIND_PROMOTION

    if lhs_schema is None or lhs_schema.kind is object:
        return None
    lhs_kind = lhs_schema.kind

    is_truediv = op_func is operator.truediv or op_func is _reverse_truediv
    Vector = _vector_class()

    if isinstance(rhs, Vector):
        rhs_schema = rhs._dtype
        if rhs_schema is None or rhs_schema.kind is object:
            return None
        result_kind = _KIND_PROMOTION.get((lhs_kind, rhs_schema.kind))
        if result_kind is None:
            return None
        if is_truediv and result_kind in (bool, int):
            result_kind = float
        return Schema(
            result_kind,
            lhs_schema.nullable or rhs_schema.nullable,
        )

    rhs_kind = type(rhs)
    result_kind = _KIND_PROMOTION.get((lhs_kind, rhs_kind))
    if result_kind is None:
        return None
    if is_truediv and result_kind in (bool, int):
        result_kind = float
    return Schema(result_kind, lhs_schema.nullable)


def elementwise_compare(vector, other, op):
    other = vector._check_duplicate(other)
    Vector = _vector_class()

    if isinstance(other, Vector) and other.ndims() > 1:
        return other._lift_comparison_from(vector, op)

    # Element-wise: unknown in, unknown out (docs/null-semantics.md).
    if isinstance(other, Vector):
        if len(vector) != len(other):
            raise SerifValueError(
                f"Length mismatch: {len(vector)} != {len(other)}"
            )
        other_schema = other.schema()
        nullable = (
            (vector._dtype.nullable if vector._dtype is not None else True)
            or (other_schema.nullable if other_schema is not None else True)
        )
        fast = _accel_compare(vector._storage, other._storage, op, nullable)
        if fast is not None:
            return fast
        return Vector._from_iterable_known_dtype(
            (
                None if (x is None or y is None) else bool(op(x, y))
                for x, y in zip(vector, other, strict=True)
            ),
            Schema(bool, nullable),
        )

    if isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        if len(vector) != len(other):
            raise SerifValueError(
                f"Length mismatch: {len(vector)} != {len(other)}"
            )
        values = [
            None if (x is None or y is None) else bool(op(x, y))
            for x, y in zip(vector, other, strict=True)
        ]
        return Vector._from_iterable_known_dtype(
            values,
            Schema(bool, any(value is None for value in values)),
        )

    if other is None and op in (operator.eq, operator.ne):
        warnings.warn(
            "Null comparison: `v == None` yields null for every element. "
            "Use `v.is_na()` to test for nulls.",
            stacklevel=3,
        )
    nullable = (
        (vector._dtype.nullable if vector._dtype is not None else True)
        or other is None
    )
    fast = _accel_compare(vector._storage, other, op, nullable)
    if fast is not None:
        return fast
    return Vector._from_iterable_known_dtype(
        (
            None if (x is None or other is None) else bool(op(x, other))
            for x in vector
        ),
        Schema(bool, nullable),
    )


def eq(vector, other):
    return vector._elementwise_compare(other, operator.eq)


def ge(vector, other):
    return vector._elementwise_compare(other, operator.ge)


def gt(vector, other):
    return vector._elementwise_compare(other, operator.gt)


def le(vector, other):
    return vector._elementwise_compare(other, operator.le)


def lt(vector, other):
    return vector._elementwise_compare(other, operator.lt)


def ne(vector, other):
    return vector._elementwise_compare(other, operator.ne)


def logical_elementwise(vector, other, kleene_func):
    """Apply a Kleene three-valued logical operation."""
    other = vector._check_duplicate(other)
    Vector = _vector_class()

    if isinstance(other, Vector) and other.ndims() > 1:
        return other._lift_logical_from(vector, kleene_func)

    op_name = _KLEENE_OP_NAMES.get(kleene_func)
    if isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        if len(vector) != len(other):
            raise SerifValueError(
                f"Length mismatch: {len(vector)} != {len(other)}"
            )
        if op_name is not None and isinstance(other, Vector):
            fast = _accel_logical(vector._storage, other._storage, op_name)
            if fast is not None:
                return fast
        values = [
            kleene_func(x, y)
            for x, y in zip(vector, other, strict=True)
        ]
    else:
        if op_name is not None and (other is None or type(other) is bool):
            fast = _accel_logical(vector._storage, other, op_name)
            if fast is not None:
                return fast
        values = [kleene_func(x, other) for x in vector]
    return Vector._from_iterable_known_dtype(
        values,
        Schema(bool, any(value is None for value in values)),
    )


def bitwise_kind_error(vector, op_symbol):
    kind = vector._dtype.kind if vector._dtype is not None else None
    kind_name = kind.__name__ if kind is not None else 'object'
    return SerifTypeError(
        f"Unsupported operand type(s) for '{op_symbol}': "
        f"Vector<{kind_name}>. '{op_symbol}' is Kleene-logical on bool "
        f"vectors and bitwise on int vectors; other dtypes raise, as in "
        f"plain Python."
    )


def bit_and(vector, other):
    kind = vector._dtype.kind if vector._dtype is not None else None
    if kind is int:
        return vector._elementwise_operation(
            other,
            operator.and_,
            '__and__',
            '&',
        )
    if kind is bool:
        return vector._logical_elementwise(other, _kleene_and)
    raise vector._bitwise_kind_error('&')


def bit_or(vector, other):
    kind = vector._dtype.kind if vector._dtype is not None else None
    if kind is int:
        return vector._elementwise_operation(
            other,
            operator.or_,
            '__or__',
            '|',
        )
    if kind is bool:
        return vector._logical_elementwise(other, _kleene_or)
    raise vector._bitwise_kind_error('|')


def bit_xor(vector, other):
    kind = vector._dtype.kind if vector._dtype is not None else None
    if kind is int:
        return vector._elementwise_operation(
            other,
            operator.xor,
            '__xor__',
            '^',
        )
    if kind is bool:
        return vector._logical_elementwise(other, _kleene_xor)
    raise vector._bitwise_kind_error('^')


def elementwise_operation(vector, other, op_func, op_name, op_symbol):
    """Apply a binary operation with Serif's scalar broadcast rules."""
    other = vector._check_duplicate(other)
    Vector = _vector_class()

    if isinstance(other, Vector) and other.ndims() > 1:
        return other._lift_operation_from(
            vector,
            op_func,
            op_name,
            op_symbol,
        )

    if isinstance(other, Vector):
        if len(vector) != len(other):
            raise SerifValueError(
                f"Length mismatch: {len(vector)} != {len(other)}"
            )
        result_dtype = _pre_compute_op_schema(vector._dtype, other, op_func)
        if result_dtype is not None:
            fast = _accel_binop(
                vector._storage,
                other._storage,
                op_func,
                result_dtype,
            )
            if fast is not None:
                return fast
        try:
            result_values = tuple(
                None if (x is None or y is None) else op_func(x, y)
                for x, y in zip(vector, other, strict=True)
            )
        except TypeError:
            lhs = (
                vector._dtype.kind.__name__
                if vector._dtype is not None
                else 'object'
            )
            rhs_schema = other.schema()
            rhs = (
                rhs_schema.kind.__name__
                if rhs_schema is not None
                else 'object'
            )
            raise SerifTypeError(
                f"Unsupported operand type(s) for '{op_symbol}': "
                f"Vector<{lhs}> and Vector<{rhs}>."
            )
        if result_dtype is None:
            result_dtype = infer_dtype(result_values)
        return Vector(result_values, dtype=result_dtype, name=None)

    if isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        if len(vector) != len(other):
            raise SerifValueError(
                f"Length mismatch: {len(vector)} != {len(other)}"
            )
        try:
            result_values = tuple(
                None if (x is None or y is None) else op_func(x, y)
                for x, y in zip(vector, other, strict=True)
            )
        except TypeError:
            lhs = (
                vector._dtype.kind.__name__
                if vector._dtype is not None
                else 'object'
            )
            raise SerifTypeError(
                f"Unsupported operand type(s) for '{op_symbol}': "
                f"Vector<{lhs}> and {type(other).__name__} elements."
            )
        result_dtype = infer_dtype(result_values)
        return Vector(result_values, dtype=result_dtype, name=None)

    result_dtype = _pre_compute_op_schema(vector._dtype, other, op_func)
    if result_dtype is not None:
        fast = _accel_binop(
            vector._storage,
            other,
            op_func,
            result_dtype,
        )
        if fast is not None:
            return fast
    try:
        result_values = tuple(
            None if x is None else op_func(x, other)
            for x in vector._storage
        )
        if result_dtype is None:
            result_dtype = infer_dtype(result_values)
        return Vector(result_values, dtype=result_dtype, name=None)
    except TypeError:
        lhs = (
            vector._dtype.kind.__name__
            if vector._dtype is not None
            else 'object'
        )
        raise SerifTypeError(
            f"Unsupported operand type(s) for '{op_symbol}': "
            f"'{lhs}' and '{type(other).__name__}'."
        )


def unary_operation(vector, op_func, op_name):
    """Apply a unary operation to every non-null element."""
    storage = vector._storage
    if isinstance(storage, ArrayStorage):
        from array import array as _array
        typecode = storage._data.typecode
        new_data = _array(
            typecode,
            (op_func(storage._data[i]) for i in range(len(storage._data))),
        )
        new_storage = ArrayStorage(new_data, storage._mask)
    else:
        new_storage = TupleStorage(tuple(
            None if value is None else op_func(value)
            for value in storage
        ))
    return vector._clone(new_storage)


def add(vector, other):
    return vector._elementwise_operation(other, operator.add, '__add__', '+')


def mul(vector, other):
    return vector._elementwise_operation(other, operator.mul, '__mul__', '*')


def sub(vector, other):
    return vector._elementwise_operation(other, operator.sub, '__sub__', '-')


def neg(vector):
    return vector._unary_operation(operator.neg, '__neg__')


def pos(vector):
    return vector._unary_operation(operator.pos, '__pos__')


def abs(vector):
    return vector._unary_operation(operator.abs, '__abs__')


def invert(vector):
    # Boolean inversion is Kleene logical NOT: NOT unknown is unknown.
    if vector._dtype and vector._dtype.kind is bool:
        fast = _accel_invert(vector._storage, vector._dtype.nullable)
        if fast is not None:
            return fast
        Vector = _vector_class()
        return Vector._from_iterable_known_dtype(
            (None if value is None else (not value) for value in vector),
            Schema(bool, vector._dtype.nullable),
        )
    return vector._unary_operation(operator.invert, '__invert__')


def truediv(vector, other):
    return vector._elementwise_operation(
        other,
        operator.truediv,
        '__truediv__',
        '/',
    )


def floordiv(vector, other):
    return vector._elementwise_operation(
        other,
        operator.floordiv,
        '__floordiv__',
        '//',
    )


def mod(vector, other):
    return vector._elementwise_operation(other, operator.mod, '__mod__', '%')


def pow(vector, other):
    return vector._elementwise_operation(other, operator.pow, '__pow__', '**')


def radd(vector, other):
    return vector._elementwise_operation(other, _reverse_add, '__radd__', '+')


def rmul(vector, other):
    return vector.__mul__(other)


def rsub(vector, other):
    return vector._elementwise_operation(other, _reverse_sub, '__rsub__', '-')


def rtruediv(vector, other):
    return vector._elementwise_operation(
        other,
        _reverse_truediv,
        '__rtruediv__',
        '/',
    )


def rfloordiv(vector, other):
    return vector._elementwise_operation(
        other,
        _reverse_floordiv,
        '__rfloordiv__',
        '//',
    )


def rmod(vector, other):
    return vector._elementwise_operation(other, _reverse_mod, '__rmod__', '%')


def rpow(vector, other):
    return vector._elementwise_operation(other, _reverse_pow, '__rpow__', '**')


def bit_lshift(vector, other):
    return vector._elementwise_operation(
        other,
        operator.lshift,
        'bit_lshift',
        '<<',
    )


def bit_rshift(vector, other):
    return vector._elementwise_operation(
        other,
        operator.rshift,
        'bit_rshift',
        '>>',
    )
