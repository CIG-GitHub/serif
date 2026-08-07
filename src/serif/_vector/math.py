"""Explicit pointwise access to Python's :mod:`math` functions."""

import math as _math


def _vector_class():
    # Local import avoids a cycle while Vector delegates its ``math`` property
    # to this module.
    from ..vector import Vector
    return Vector


def _apply_unary(vector, function_name, result_kind):
    """Apply one scalar ``math`` function, propagating null positions."""
    if vector.ndims() == 2:
        return vector._map_columns(
            lambda column: _apply_unary(
                column,
                function_name,
                result_kind,
            )
        )

    function = getattr(_math, function_name)
    values = (
        function(value) if value is not None else None
        for value in vector._storage
    )
    return _vector_class()._from_iterable_known_kind(values, result_kind)


class MathAccessor:
    """Pointwise, null-propagating access to Python's ``math`` module."""

    __slots__ = ('_vector',)

    def __init__(self, vector):
        self._vector = vector


def _unary_method(function_name, result_kind):
    def method(self):
        return _apply_unary(self._vector, function_name, result_kind)

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

    # Powers and logarithms. ``log`` gains its optional base when the
    # multi-argument broadcasting surface is added.
    'exp': float,
    'expm1': float,
    'log': float,
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
