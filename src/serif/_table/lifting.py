"""Recursive Table lifting through ordinary Vector semantics."""

import operator
import warnings

from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates lifted operations here.
    from ..table import Table
    return Table


def _resolve_binary_name(left_name, right_name):
    """Return the left-biased result name and any warning case."""
    if right_name is None or right_name == left_name:
        return left_name, None
    if left_name is None:
        return None, "right-named-left-unnamed"
    return None, "mismatch"


def map_columns(table, function):
    """Apply a value-producing Vector operation to every Table column."""
    Table = _table_class()
    result = []
    for source in iter_columns(table):
        derived = function(source)
        if not isinstance(derived, Vector) or derived.ndims() != 1:
            raise SerifTypeError(
                "Table column operation must produce one Vector per column"
            )
        derived._name = source._name
        derived._wild = False
        result.append(derived)
    return Table(result, name=table._name)


def cast(table, target_type):
    return map_columns(table, lambda column: column.cast(target_type))


def to_object(table):
    return map_columns(table, lambda column: column.to_object())


def fillna(table, value):
    return map_columns(table, lambda column: column.fillna(value))


def is_na(table):
    return map_columns(table, lambda column: column.is_na())


def is_type(table, types):
    return map_columns(table, lambda column: column.is_type(types))


def compare(table, other, op):
    """Lift a comparison over columns, preserving left column names."""
    Table = _table_class()
    other = table._check_duplicate(other)
    left_columns = tuple(iter_columns(table))

    if isinstance(other, Table):
        right_columns = tuple(iter_columns(other))
        if len(left_columns) != len(right_columns):
            raise SerifValueError(
                f"Column count mismatch: "
                f"{len(left_columns)} != {len(right_columns)}"
            )
        result_columns = [
            left._elementwise_compare(right, op)
            for left, right in zip(
                left_columns,
                right_columns,
                strict=True,
            )
        ]
    else:
        result_columns = [
            column._elementwise_compare(other, op)
            for column in left_columns
        ]

    for source, result in zip(
        left_columns,
        result_columns,
        strict=True,
    ):
        result._name = source._name
        result._wild = False
    return Table(result_columns)


def compare_from(table, left, op):
    """Lift ``left op table`` when a scalar Vector sees a nested operand."""
    return table.copy(tuple(
        left._elementwise_compare(column, op)
        for column in iter_columns(table)
    ))


def binary_operation(table, other, op_func, op_name, op_symbol):
    """Lift an arithmetic operation with Table naming coordination."""
    Table = _table_class()
    left_columns = tuple(iter_columns(table))

    if not isinstance(other, Table):
        result_columns = tuple(
            op_func(column, other)
            for column in left_columns
        )
        for source, result in zip(
            left_columns,
            result_columns,
            strict=True,
        ):
            result._name = source._name
            result._wild = source._wild
        return Table(result_columns)

    right_columns = tuple(iter_columns(other))
    if len(left_columns) != len(right_columns):
        raise SerifValueError(
            f"Table width mismatch: "
            f"{len(left_columns)} != {len(right_columns)}"
        )

    result_columns = []
    warnings_to_emit = []
    for index, (left_column, right_column) in enumerate(zip(
        left_columns,
        right_columns,
    )):
        result_column = op_func(left_column, right_column)
        result_name, warning_case = _resolve_binary_name(
            left_column._name,
            right_column._name,
        )
        result_column._name = result_name
        result_column._wild = False
        if warning_case is not None:
            warnings_to_emit.append((
                index,
                left_column._name,
                right_column._name,
                warning_case,
            ))
        result_columns.append(result_column)

    if warnings_to_emit:
        lines = [
            f"Table operation ({op_symbol}) produced unusual column naming "
            f"in {len(warnings_to_emit)} column(s):"
        ]
        for index, left_name, right_name, warning_case in warnings_to_emit:
            if warning_case == "mismatch":
                lines.append(
                    f"  idx {index}: left={left_name!r} "
                    f"right={right_name!r} → dropped"
                )
            else:
                lines.append(
                    f"  idx {index}: left=None right={right_name!r} "
                    "→ kept None (left-biased)"
                )
        warnings.warn("\n".join(lines), UserWarning, stacklevel=3)

    return Table(tuple(result_columns))


def operation_from(table, left, op_func, op_name, op_symbol):
    """Lift ``left op table`` for a nested right-hand operand."""
    return table.copy(tuple(
        left._elementwise_operation(
            column,
            op_func,
            op_name,
            op_symbol,
        )
        for column in iter_columns(table)
    ))


def reverse_scalar_operation(table, other, op_func):
    """Apply ``other op column`` while retaining the Table schema."""
    return map_columns(table, lambda column: op_func(other, column))


def add(table, other):
    return binary_operation(table, other, operator.add, '__add__', '+')


def sub(table, other):
    return binary_operation(table, other, operator.sub, '__sub__', '-')


def mul(table, other):
    return binary_operation(table, other, operator.mul, '__mul__', '*')


def truediv(table, other):
    return binary_operation(
        table,
        other,
        operator.truediv,
        '__truediv__',
        '/',
    )


def floordiv(table, other):
    return binary_operation(
        table,
        other,
        operator.floordiv,
        '__floordiv__',
        '//',
    )


def mod(table, other):
    return binary_operation(table, other, operator.mod, '__mod__', '%')


def pow(table, other):
    return binary_operation(table, other, operator.pow, '__pow__', '**')


def radd(table, other):
    return reverse_scalar_operation(table, other, operator.add)


def rmul(table, other):
    return reverse_scalar_operation(table, other, operator.mul)


def rsub(table, other):
    return reverse_scalar_operation(table, other, operator.sub)


def rtruediv(table, other):
    return reverse_scalar_operation(table, other, operator.truediv)


def rfloordiv(table, other):
    return reverse_scalar_operation(table, other, operator.floordiv)


def rmod(table, other):
    return reverse_scalar_operation(table, other, operator.mod)


def rpow(table, other):
    return reverse_scalar_operation(table, other, operator.pow)


def neg(table):
    return map_columns(table, operator.neg)


def pos(table):
    return map_columns(table, operator.pos)


def abs(table):
    return map_columns(table, operator.abs)


def invert(table):
    return map_columns(table, operator.invert)


def logical_from(table, left, kleene_func):
    """Lift a logical operation from a scalar Vector into a Table."""
    return table.copy(tuple(
        left._logical_elementwise(column, kleene_func)
        for column in iter_columns(table)
    ))


def bitwise(table, other, op_dunder):
    """Lift ``&``, ``|``, or ``^`` using each column's Vector dispatch."""
    Table = _table_class()
    other = table._check_duplicate(other)
    source_columns = tuple(iter_columns(table))
    result_columns = [
        getattr(column, op_dunder)(other)
        for column in source_columns
    ]
    for source, result in zip(
        source_columns,
        result_columns,
        strict=True,
    ):
        result._name = source._name
        result._wild = False
    return Table(result_columns)


def bitwise_from(table, left, op_dunder):
    """Lift ``left op table`` while preserving right-operand behavior."""
    return table.copy(tuple(
        getattr(left, op_dunder)(column)
        for column in iter_columns(table)
    ))


def reverse_bitwise(table, other, op_dunder):
    """Preserve scalar broadcast and nested-Vector reflected behavior."""
    if isinstance(other, Vector):
        return bitwise_from(table, other, op_dunder)
    return bitwise(table, other, op_dunder)


def bit_and(table, other):
    return bitwise(table, other, '__and__')


def bit_or(table, other):
    return bitwise(table, other, '__or__')


def bit_xor(table, other):
    return bitwise(table, other, '__xor__')


def rbit_and(table, other):
    return reverse_bitwise(table, other, '__and__')


def rbit_or(table, other):
    return reverse_bitwise(table, other, '__or__')


def rbit_xor(table, other):
    return reverse_bitwise(table, other, '__xor__')


def bit_lshift(table, other):
    return map_columns(table, lambda column: column.bit_lshift(other))


def bit_rshift(table, other):
    return map_columns(table, lambda column: column.bit_rshift(other))
