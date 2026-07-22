"""Vector reduction semantics.

Reductions consume the innermost dimension. Rank-2 values lift the same
reduction over their columns; rank-1 values reduce their scalar storage.
"""

from .._execution import DECLINED
from ..errors import SerifEmptyReductionError
from ..errors import SerifTypeError
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
        f"default, which raises on zero valid values); got {on_empty!r}"
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
    verdict = _python_reductions.all_(vector._storage)
    if verdict is None:
        return _no_verdict(vector, 'all', on_empty)
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
        return _no_verdict(vector, 'any', on_empty)
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
