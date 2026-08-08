"""Vector reduction semantics.

Reductions consume the innermost dimension. Rank-2 values lift the same
reduction over their columns; rank-1 values reduce their scalar storage.
"""

import warnings
from collections.abc import Iterable

from .._execution import DECLINED
from ..errors import SerifEmptyReductionWarning
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ._python import reductions as _python_reductions


def _numpy_reductions():
    from ._numpy import reductions

    return reductions


def _check_on_empty(method_name, on_empty):
    # Identity checks, not truthiness: on_empty=1 is a bug, not a True.
    if on_empty is None or on_empty is True or on_empty is False:
        return
    raise SerifTypeError(
        f"{method_name}(): on_empty must be True or False (or None, the "
        f"default, which returns the identity and warns on zero valid "
        f"values); got {on_empty!r}"
    )


def max(vector):
    if vector.ndims() == 2:
        return vector.copy((c.max() for c in vector.cols()), name=None)
    fast = _numpy_reductions().max_(vector._storage)
    if fast is not DECLINED:
        return fast
    return _python_reductions.max_(vector._storage)


def min(vector):
    if vector.ndims() == 2:
        return vector.copy((c.min() for c in vector.cols()), name=None)
    fast = _numpy_reductions().min_(vector._storage)
    if fast is not DECLINED:
        return fast
    return _python_reductions.min_(vector._storage)


def first(vector):
    if vector.ndims() == 2:
        return vector.copy((c.first() for c in vector.cols()), name=None)
    return _python_reductions.first(vector._storage)


def last(vector):
    if vector.ndims() == 2:
        return vector.copy((c.last() for c in vector.cols()), name=None)
    return _python_reductions.last(vector._storage)


def sum(vector):
    if vector.ndims() == 2:
        return vector.copy((c.sum() for c in vector.cols()), name=None)
    fast = _numpy_reductions().sum_(vector._storage)
    if fast is not DECLINED:
        return fast
    return _python_reductions.sum_(
        vector._storage,
        vector.schema().kind,
    )


def fsum(vector):
    if vector.ndims() == 2:
        return vector.copy((c.math.fsum() for c in vector.cols()), name=None)
    return _python_reductions.fsum(vector._storage)


def prod(vector):
    if vector.ndims() == 2:
        return vector.copy((c.math.prod() for c in vector.cols()), name=None)
    schema = vector.schema()
    return _python_reductions.prod(
        vector._storage,
        schema.kind if schema is not None else object,
    )


def gcd(vector):
    if vector.ndims() == 2:
        return vector.copy((c.math.gcd() for c in vector.cols()), name=None)
    return _python_reductions.gcd(vector._storage)


def lcm(vector):
    if vector.ndims() == 2:
        return vector.copy((c.math.lcm() for c in vector.cols()), name=None)
    return _python_reductions.lcm(vector._storage)


def hypot(vector):
    if vector.ndims() == 2:
        return vector.copy((c.math.hypot() for c in vector.cols()), name=None)
    return _python_reductions.hypot(vector._storage)


def _vector_class():
    from ..vector import Vector
    return Vector


def _dist_values(vector, other):
    Vector = _vector_class()
    if isinstance(other, Vector):
        if other.ndims() != 1:
            raise SerifTypeError(
                "dist() cannot compare a Vector with a Table"
            )
        values = other._storage
    elif isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        values = tuple(other)
    else:
        raise SerifTypeError(
            "dist() requires another Vector or numeric iterable"
        )

    if len(vector) != len(values):
        raise SerifValueError(
            f"Length mismatch: {len(vector)} != {len(values)}"
        )
    return values


def dist(vector, other):
    Vector = _vector_class()
    if vector.ndims() == 2:
        left_columns = tuple(vector.cols())
        if isinstance(other, Vector) and other.ndims() == 2:
            right_columns = tuple(other.cols())
            if len(left_columns) != len(right_columns):
                raise SerifValueError(
                    f"Table width mismatch: "
                    f"{len(left_columns)} != {len(right_columns)}"
                )
            return vector.copy(
                (
                    left.math.dist(right)
                    for left, right in zip(
                        left_columns,
                        right_columns,
                        strict=True,
                    )
                ),
                name=None,
            )
        return vector.copy(
            (column.math.dist(other) for column in left_columns),
            name=None,
        )

    right_values = _dist_values(vector, other)
    return _python_reductions.dist(vector._storage, right_values)


def _no_verdict(vector, method_name, on_empty, identity):
    if on_empty is not None:
        return on_empty
    n = len(vector._storage)
    detail = "empty vector" if n == 0 else f"length {n}, all null"
    warnings.warn(
        f"{method_name}() over zero valid values ({detail}): returning "
        f"{identity}, the identity, as Python's {method_name}([]) does. "
        f"Pass on_empty=True or on_empty=False to state the empty-case "
        f"verdict yourself and silence this warning.",
        SerifEmptyReductionWarning,
        stacklevel=4,
    )
    return identity


def all(vector, on_empty=None):
    _check_on_empty('all', on_empty)
    if vector.ndims() == 2:
        return vector.copy(
            (c.all(on_empty=on_empty) for c in vector.cols()),
            name=None,
        )
    verdict = _python_reductions.all_(vector._storage)
    if verdict is None:
        return _no_verdict(vector, 'all', on_empty, True)
    return verdict


def any(vector, on_empty=None):
    _check_on_empty('any', on_empty)
    if vector.ndims() == 2:
        return vector.copy(
            (c.any(on_empty=on_empty) for c in vector.cols()),
            name=None,
        )
    verdict = _python_reductions.any_(vector._storage)
    if verdict is None:
        return _no_verdict(vector, 'any', on_empty, False)
    return verdict


def mean(vector):
    if vector.ndims() == 2:
        return vector.copy((c.mean() for c in vector.cols()), name=None)
    fast = _numpy_reductions().mean(vector._storage)
    if fast is not DECLINED:
        return fast
    return _python_reductions.mean(vector._storage)


def stdev(vector, population=False):
    if vector.ndims() == 2:
        return vector.copy(
            (c.stdev(population) for c in vector.cols()),
            name=None,
        )
    fast = _numpy_reductions().stdev(
        vector._storage,
        population=population,
    )
    if fast is not DECLINED:
        return fast
    return _python_reductions.stdev(
        vector._storage,
        population=population,
    )


def count(vector):
    if vector.ndims() == 2:
        return vector.copy((c.count() for c in vector.cols()), name=None)
    return _python_reductions.count(vector._storage)
