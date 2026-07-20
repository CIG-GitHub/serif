"""Vector reduction semantics.

Reductions consume the innermost dimension. Rank-2 values lift the same
reduction over their columns; rank-1 values reduce their scalar storage.
"""

import builtins as _builtins
import math
from itertools import chain

from .._accel.api import _accel_reduce
from ..errors import SerifEmptyReductionError
from ..errors import SerifTypeError


def _check_on_empty(method_name, on_empty):
    # Identity checks, not truthiness: on_empty=1 is a bug, not a True.
    if on_empty is None or on_empty is True or on_empty is False:
        return
    raise SerifTypeError(
        f"{method_name}(): on_empty must be True or False (or None, the "
        f"default, which raises on zero valid values); got {on_empty!r}"
    )


def max(vector):
    if vector.ndims() == 2:
        return vector.copy((c.max() for c in vector.cols()), name=None)
    ok, fast = _accel_reduce(vector._storage, 'max_')
    if ok:
        return fast
    non_none = [v for v in vector._storage if v is not None]
    return _builtins.max(non_none) if non_none else None


def min(vector):
    if vector.ndims() == 2:
        return vector.copy((c.min() for c in vector.cols()), name=None)
    ok, fast = _accel_reduce(vector._storage, 'min_')
    if ok:
        return fast
    non_none = [v for v in vector._storage if v is not None]
    return _builtins.min(non_none) if non_none else None


def first(vector):
    if vector.ndims() == 2:
        return vector.copy((c.first() for c in vector.cols()), name=None)
    return vector._storage[0] if len(vector._storage) else None


def last(vector):
    if vector.ndims() == 2:
        return vector.copy((c.last() for c in vector.cols()), name=None)
    n = len(vector._storage)
    return vector._storage[n - 1] if n else None


def sum(vector):
    if vector.ndims() == 2:
        return vector.copy((c.sum() for c in vector.cols()), name=None)
    ok, fast = _accel_reduce(vector._storage, 'sum_')
    if ok:
        return fast
    # Exclude None values from sum.
    values = (v for v in vector._storage if v is not None)
    if vector.schema().kind is not float:
        return _builtins.sum(values)

    first_value = next(values, None)
    if first_value is None:
        return 0
    try:
        return math.fsum(chain((first_value,), values))
    except (OverflowError, ValueError):
        # math.fsum rejects mixtures such as +inf and -inf. Preserve
        # Python's non-finite behavior while finite sums remain stable
        # across every supported Python version.
        return _builtins.sum(v for v in vector._storage if v is not None)


def _no_verdict(vector, method_name, on_empty):
    if on_empty is not None:
        return on_empty
    n = len(vector._storage)
    detail = "empty vector" if n == 0 else f"length {n}, all null"
    raise SerifEmptyReductionError(
        f"{method_name}() over zero valid values ({detail}): no verdict "
        f"is possible. Pass on_empty=True or on_empty=False to choose "
        f"the empty-case verdict, or fillna()/dropna() upstream."
    )


def all(vector, on_empty=None):
    _check_on_empty('all', on_empty)
    if vector.ndims() == 2:
        return vector.copy(
            (c.all(on_empty=on_empty) for c in vector.cols()),
            name=None,
        )
    seen_valid = False
    for value in vector._storage:
        if value is None:
            continue
        if not value:
            return False
        seen_valid = True
    if seen_valid:
        return True
    return _no_verdict(vector, 'all', on_empty)


def any(vector, on_empty=None):
    _check_on_empty('any', on_empty)
    if vector.ndims() == 2:
        return vector.copy(
            (c.any(on_empty=on_empty) for c in vector.cols()),
            name=None,
        )
    seen_valid = False
    for value in vector._storage:
        if value is None:
            continue
        if value:
            return True
        seen_valid = True
    if seen_valid:
        return False
    return _no_verdict(vector, 'any', on_empty)


def mean(vector):
    if vector.ndims() == 2:
        return vector.copy((c.mean() for c in vector.cols()), name=None)
    ok, fast = _accel_reduce(vector._storage, 'mean')
    if ok:
        return fast
    # Exclude None values from mean.
    non_none = [v for v in vector._storage if v is not None]
    return _builtins.sum(non_none) / len(non_none) if non_none else None


def stdev(vector, population=False):
    if vector.ndims() == 2:
        return vector.copy(
            (c.stdev(population) for c in vector.cols()),
            name=None,
        )
    ok, fast = _accel_reduce(
        vector._storage,
        'stdev',
        population=population,
    )
    if ok:
        return fast
    # Exclude None values from stdev.
    non_none = [v for v in vector._storage if v is not None]
    if len(non_none) < 2:
        return None
    mean_value = _builtins.sum(non_none) / len(non_none)
    # The zero-dependency fallback is the specification; NumPy is the fast
    # path when it can produce the same result.
    numerator = _builtins.sum(
        (value - mean_value) * (value - mean_value)
        for value in non_none
    )
    return (numerator / (len(non_none) - 1 + population)) ** 0.5


def count(vector):
    if vector.ndims() == 2:
        return vector.copy((c.count() for c in vector.cols()), name=None)
    return _builtins.sum(1 for v in vector._storage if v is not None)
