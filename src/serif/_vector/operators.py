"""Vector pointwise operator semantics and deterministic dispatch."""

import operator
import warnings
from collections.abc import Iterable

from .._execution import DECLINED
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ._python import operators as _python_ops
from .dtype import Schema
from .dtype import infer_dtype
from .dtype import promote_kinds
from .storage import BoolStorage


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


_KLEENE_OP_NAMES = {
    _kleene_and: 'and',
    _kleene_or: 'or',
    _kleene_xor: 'xor',
}

_FORWARD_DIVISION_OPS = (
    operator.truediv,
    operator.floordiv,
    operator.mod,
)


def _pre_compute_op_schema(lhs_schema, rhs, op_func=None):
    """Resolve a binary operation's output Schema without touching data.

    Returns None when the result type cannot be determined from types alone,
    including object dtype, temporal operations, and unknown right-hand kinds.
    """
    if lhs_schema is None or lhs_schema.kind is object:
        return None
    lhs_kind = lhs_schema.kind

    is_truediv = op_func is operator.truediv or op_func is _reverse_truediv
    Vector = _vector_class()

    if isinstance(rhs, Vector):
        rhs_schema = rhs._dtype
        if rhs_schema is None or rhs_schema.kind is object:
            return None
        result_kind = promote_kinds(lhs_kind, rhs_schema.kind)
        if result_kind is None:
            return None
        if is_truediv and result_kind in (bool, int):
            result_kind = float
        return Schema(
            result_kind,
            lhs_schema.nullable or rhs_schema.nullable,
        )

    rhs_kind = type(rhs)
    result_kind = promote_kinds(lhs_kind, rhs_kind)
    if result_kind is None:
        return None
    if is_truediv and result_kind in (bool, int):
        result_kind = float
    return Schema(result_kind, lhs_schema.nullable)


def _wrap_storage(storage, schema):
    """Wrap backend storage with the same metadata as the pure constructor."""
    Vector = _vector_class()
    result = Vector._from_storage(storage, schema)
    result._wild = True
    return result


def _dispatch_compare(storage, rhs, op_func):
    """Try fixed-width comparison, then string comparison."""
    from ._numpy import operators as numpy_ops

    result = numpy_ops.compare_storage(storage, rhs, op_func)
    if result is not DECLINED:
        return result

    from ._arrow import operators as arrow_ops

    return arrow_ops.compare_strings(storage, rhs, op_func)


def _dispatch_logical(storage, rhs, op_name):
    from ._numpy import operators as numpy_ops

    return numpy_ops.logical_storage(storage, rhs, op_name)


def _dispatch_invert(storage):
    from ._numpy import operators as numpy_ops

    return numpy_ops.invert_storage(storage)


def _dispatch_binary(storage, rhs, op_func, result_kind):
    """Run operator backends in their explicit, stable priority order."""
    if op_func is operator.truediv:
        from ._arrow import operators as arrow_ops

        result = arrow_ops.div_floats(
            storage,
            rhs,
            op_func,
            result_kind,
        )
        if result is not DECLINED:
            return result

    from ._numpy import operators as numpy_ops

    result = numpy_ops.binop_storage(
        storage,
        rhs,
        op_func,
        result_kind,
    )
    if result is not DECLINED:
        return result

    from ._arrow import operators as arrow_ops

    return arrow_ops.binop_ints(storage, rhs, op_func, result_kind)


def _validate_forward_division(left, right, op_func):
    """Raise Python's division error before an optional backend runs."""
    if op_func not in _FORWARD_DIVISION_OPS:
        return
    Vector = _vector_class()
    if isinstance(right, Vector):
        pairs = zip(left, right, strict=True)
        for left_value, right_value in pairs:
            if (
                left_value is not None
                and right_value is not None
                and right_value == 0
            ):
                op_func(left_value, right_value)
        return
    if right is None or right != 0:
        return
    for left_value in left:
        if left_value is not None:
            op_func(left_value, right)


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
        fast = _dispatch_compare(vector._storage, other._storage, op)
        if fast is not DECLINED:
            return _wrap_storage(fast, Schema(bool, nullable))
        return _wrap_storage(
            _python_ops.compare_vector(vector, other, op),
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
        storage = _python_ops.compare_vector(vector, other, op)
        assert isinstance(storage, BoolStorage)
        return _wrap_storage(
            storage,
            Schema(bool, storage._mask is not None),
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
    fast = _dispatch_compare(vector._storage, other, op)
    if fast is not DECLINED:
        return _wrap_storage(fast, Schema(bool, nullable))
    return _wrap_storage(
        _python_ops.compare_scalar(vector._storage, other, op),
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
            fast = _dispatch_logical(
                vector._storage,
                other._storage,
                op_name,
            )
            if fast is not DECLINED:
                return _wrap_storage(
                    fast,
                    Schema(bool, fast._mask is not None),
                )
        values = _python_ops.logical_vector(vector, other, kleene_func)
    else:
        if op_name is not None and (other is None or type(other) is bool):
            fast = _dispatch_logical(vector._storage, other, op_name)
            if fast is not DECLINED:
                return _wrap_storage(
                    fast,
                    Schema(bool, fast._mask is not None),
                )
        values = _python_ops.logical_scalar(
            vector._storage,
            other,
            kleene_func,
        )
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
            _validate_forward_division(vector, other, op_func)
            fast = _dispatch_binary(
                vector._storage,
                other._storage,
                op_func,
                result_dtype.kind,
            )
            if fast is not DECLINED:
                return _wrap_storage(fast, result_dtype)
        try:
            result_values = _python_ops.binary_vector(
                vector,
                other,
                op_func,
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
            result_values = _python_ops.binary_vector(
                vector,
                other,
                op_func,
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
        _validate_forward_division(vector, other, op_func)
        fast = _dispatch_binary(
            vector._storage,
            other,
            op_func,
            result_dtype.kind,
        )
        if fast is not DECLINED:
            return _wrap_storage(fast, result_dtype)
    try:
        result_values = _python_ops.binary_scalar(
            vector._storage,
            other,
            op_func,
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
    return vector._clone(
        _python_ops.unary_storage(vector._storage, op_func)
    )


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
        fast = _dispatch_invert(vector._storage)
        if fast is not DECLINED:
            return _wrap_storage(
                fast,
                Schema(bool, vector._dtype.nullable),
            )
        Vector = _vector_class()
        return Vector._from_iterable_known_dtype(
            _python_ops.invert_bool(vector),
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
