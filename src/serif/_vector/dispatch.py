"""Optional accelerator routing for Vector physical operations."""

import operator

from .._execution import first_supported


def compare(storage, rhs, op_func):
    """Try fixed-width comparison, then string comparison."""
    def numpy_compare():
        from ._numpy import operators

        return operators.compare_storage(storage, rhs, op_func)

    def arrow_compare():
        from ._arrow import operators

        return operators.compare_strings(storage, rhs, op_func)

    return first_supported(
        numpy_compare,
        arrow_compare,
    )


def logical(storage, rhs, op_name):
    from ._numpy import operators as numpy_ops

    return numpy_ops.logical_storage(storage, rhs, op_name)


def invert(storage):
    from ._numpy import operators as numpy_ops

    return numpy_ops.invert_storage(storage)


def binary(storage, rhs, op_func, result_kind):
    """Run arithmetic accelerators in their explicit priority order."""
    def arrow_float_division():
        from ._arrow import operators

        return operators.div_floats(
            storage,
            rhs,
            op_func,
            result_kind,
        )

    def numpy_binary():
        from ._numpy import operators

        return operators.binop_storage(
            storage,
            rhs,
            op_func,
            result_kind,
        )

    def arrow_integer_binary():
        from ._arrow import operators

        return operators.binop_ints(
            storage,
            rhs,
            op_func,
            result_kind,
        )

    attempts = []
    if op_func is operator.truediv:
        attempts.append(arrow_float_division)
    attempts.extend((
        numpy_binary,
        arrow_integer_binary,
    ))
    return first_supported(*attempts)


def unary_math(storage, function_name):
    from ._numpy import math

    return math.unary_storage(storage, function_name)


def binary_math(storage, operand, function_name):
    from ._numpy import math

    return math.binary_storage(storage, operand, function_name)


def reduction(function_name, storage, *args, **kwargs):
    from ._numpy import reductions

    return getattr(reductions, function_name)(storage, *args, **kwargs)


def statistic(function_name, storage, *args, **kwargs):
    from ._numpy import statistics

    return getattr(statistics, function_name)(storage, *args, **kwargs)


def filter_storage(storage, mask):
    from ._numpy import selection

    return selection.filter_storage(storage, mask)


def take_storage(storage, indices):
    from ._numpy import selection

    return selection.take_storage(storage, indices)


def take_pad_storage(storage, indices):
    from ._numpy import selection

    return selection.take_pad_storage(storage, indices)


def popcount(storage):
    from ._numpy import selection

    return selection.popcount_storage(storage)
