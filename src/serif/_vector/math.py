"""Explicit pointwise access to Python's :mod:`math` functions."""

import math as _math

from ..errors import SerifTypeError
from ..errors import SerifValueError


_MISSING = object()


def _vector_class():
    # Local import avoids a cycle while Vector delegates its ``math`` property
    # to this module.
    from ..vector import Vector
    return Vector


def _table_class():
    from ..table import Table
    return Table


def _table_operand_columns(operand):
    Vector = _vector_class()
    if isinstance(operand, Vector) and operand.ndims() == 2:
        return tuple(operand.cols())
    return None


def _apply_table(vector, function_name, result_kind, args, kwargs):
    """Lift one pointwise math call over corresponding Table columns."""
    source_columns = tuple(vector.cols())
    positional_columns = tuple(_table_operand_columns(arg) for arg in args)
    keyword_columns = {
        name: _table_operand_columns(value)
        for name, value in kwargs.items()
    }

    for columns in (
        *positional_columns,
        *keyword_columns.values(),
    ):
        if columns is not None and len(columns) != len(source_columns):
            raise SerifValueError(
                f"Table width mismatch: "
                f"{len(source_columns)} != {len(columns)}"
            )

    result_columns = []
    for index, source in enumerate(source_columns):
        column_args = tuple(
            columns[index] if columns is not None else arg
            for arg, columns in zip(args, positional_columns, strict=True)
        )
        column_kwargs = {
            name: (
                keyword_columns[name][index]
                if keyword_columns[name] is not None
                else value
            )
            for name, value in kwargs.items()
        }
        result = _apply_pointwise(
            source,
            function_name,
            result_kind,
            column_args,
            column_kwargs,
        )
        result._name = source._name
        result._wild = False
        result_columns.append(result)

    return _table_class()(result_columns, name=vector._name)


def _normalize_operand(vector, function_name, operand):
    """Resolve a scalar or same-length Vector broadcast operand."""
    Vector = _vector_class()
    if not isinstance(operand, Vector):
        return False, operand
    if operand.ndims() != 1:
        raise SerifTypeError(
            f"math.{function_name}() cannot broadcast a Table into a Vector"
        )
    if len(vector) != len(operand):
        raise SerifValueError(
            f"Length mismatch: {len(vector)} != {len(operand)}"
        )
    return True, operand._storage


def _apply_pointwise(
    vector,
    function_name,
    result_kind,
    args=(),
    kwargs=None,
):
    """Apply one scalar ``math`` call, broadcasting Vector arguments."""
    if kwargs is None:
        kwargs = {}
    if vector.ndims() == 2:
        return _apply_table(
            vector,
            function_name,
            result_kind,
            args,
            kwargs,
        )

    function = getattr(_math, function_name)
    positional_operands = tuple(
        _normalize_operand(vector, function_name, operand)
        for operand in args
    )
    keyword_operands = {
        name: _normalize_operand(vector, function_name, operand)
        for name, operand in kwargs.items()
    }

    def values():
        for index, value in enumerate(vector._storage):
            lane_args = tuple(
                operand[index] if is_vector else operand
                for is_vector, operand in positional_operands
            )
            lane_kwargs = {
                name: operand[index] if is_vector else operand
                for name, (is_vector, operand) in keyword_operands.items()
            }
            if (
                value is None
                or any(operand is None for operand in lane_args)
                or any(operand is None for operand in lane_kwargs.values())
            ):
                yield None
            else:
                yield function(value, *lane_args, **lane_kwargs)

    return _vector_class()._from_iterable_known_kind(values(), result_kind)


class MathAccessor:
    """Pointwise, null-propagating access to Python's ``math`` module."""

    __slots__ = ('_vector',)

    def __init__(self, vector):
        self._vector = vector

    def log(self, base=_MISSING):
        """Apply ``math.log`` pointwise, optionally broadcasting ``base``."""
        args = () if base is _MISSING else (base,)
        return _apply_pointwise(self._vector, 'log', float, args)

    def comb(self, k):
        return _apply_pointwise(self._vector, 'comb', int, (k,))

    def gcd(self, *integers):
        return _apply_pointwise(self._vector, 'gcd', int, integers)

    def lcm(self, *integers):
        return _apply_pointwise(self._vector, 'lcm', int, integers)

    def perm(self, k=None):
        # Scalar None is math.perm's documented default. A None inside a
        # Vector k remains a data null and propagates through that lane.
        args = () if k is None else (k,)
        return _apply_pointwise(self._vector, 'perm', int, args)

    def copysign(self, sign):
        return _apply_pointwise(self._vector, 'copysign', float, (sign,))

    def fmod(self, divisor):
        return _apply_pointwise(self._vector, 'fmod', float, (divisor,))

    def isclose(
        self,
        other,
        *,
        rel_tol=1e-09,
        abs_tol=0.0,
    ):
        return _apply_pointwise(
            self._vector,
            'isclose',
            bool,
            (other,),
            {'rel_tol': rel_tol, 'abs_tol': abs_tol},
        )

    def ldexp(self, exponent):
        return _apply_pointwise(self._vector, 'ldexp', float, (exponent,))

    def nextafter(self, target):
        return _apply_pointwise(self._vector, 'nextafter', float, (target,))

    def remainder(self, divisor):
        return _apply_pointwise(self._vector, 'remainder', float, (divisor,))

    def pow(self, exponent):
        return _apply_pointwise(self._vector, 'pow', float, (exponent,))

    def atan2(self, x):
        return _apply_pointwise(self._vector, 'atan2', float, (x,))

    def hypot(self, *coordinates):
        return _apply_pointwise(
            self._vector,
            'hypot',
            float,
            coordinates,
        )


def _unary_method(function_name, result_kind):
    def method(self):
        return _apply_pointwise(self._vector, function_name, result_kind)

    method.__name__ = function_name
    method.__qualname__ = f"MathAccessor.{function_name}"
    method.__doc__ = (
        f"Apply ``math.{function_name}`` pointwise; null positions pass through."
    )
    return method


# This is deliberately an explicit Python 3.10-baseline surface. Functions
# added in later Python versions must not appear conditionally based on the
# interpreter running Serif.
_UNARY_RESULT_KINDS = {
    # Integer and rounding functions.
    'ceil': int,
    'factorial': int,
    'floor': int,
    'isqrt': int,
    'trunc': int,

    # Floating-point manipulation and classification.
    'fabs': float,
    'frexp': tuple,
    'isfinite': bool,
    'isinf': bool,
    'isnan': bool,
    'modf': tuple,
    'ulp': float,

    # Powers and logarithms. ``log`` has a handwritten optional-base method.
    'exp': float,
    'expm1': float,
    'log1p': float,
    'log2': float,
    'log10': float,
    'sqrt': float,

    # Angular conversion and trigonometry.
    'degrees': float,
    'radians': float,
    'acos': float,
    'asin': float,
    'atan': float,
    'cos': float,
    'sin': float,
    'tan': float,

    # Hyperbolic functions.
    'acosh': float,
    'asinh': float,
    'atanh': float,
    'cosh': float,
    'sinh': float,
    'tanh': float,

    # Special functions.
    'erf': float,
    'erfc': float,
    'gamma': float,
    'lgamma': float,
}


for _function_name, _result_kind in _UNARY_RESULT_KINDS.items():
    setattr(
        MathAccessor,
        _function_name,
        _unary_method(_function_name, _result_kind),
    )
del _function_name, _result_kind


__all__ = ['MathAccessor']
