"""Vector indexing and selection semantics."""

import warnings

from .._execution import DECLINED
from ..errors import SerifIndexError
from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ._python import selection as _python_selection


def _numpy_selection():
    from ._numpy import selection

    return selection


def filter_storage(storage, mask):
    """Filter validated storage through NumPy, then canonical Python."""
    result = _numpy_selection().filter_storage(storage, mask)
    if result is not DECLINED:
        return result
    return _python_selection.filter_storage(storage, mask)


def take_storage(storage, indices):
    """Gather validated positions through NumPy, then storage.take()."""
    result = _numpy_selection().take_storage(storage, indices)
    if result is not DECLINED:
        return result
    return _python_selection.take_storage(storage, indices)


def take_pad_storage(storage, indices):
    """Try the optional padded gather; its caller owns pure wrapping."""
    return _numpy_selection().take_pad_storage(storage, indices)


def take_pad_values(storage, indices):
    """Return canonical Python values for a declined padded gather."""
    return _python_selection.take_pad_values(storage, indices)


def popcount(mask_storage):
    """Count selected mask lanes through NumPy, then canonical Python."""
    result = _numpy_selection().popcount_storage(mask_storage)
    if result is not DECLINED:
        return result
    return _python_selection.popcount(mask_storage)


def _vector_class():
    # Local import avoids a cycle while Vector delegates __getitem__ here.
    from ..vector import Vector
    return Vector


def getitem(vector, key):
    Vector = _vector_class()

    if isinstance(key, int):
        try:
            return vector._storage[key]
        except IndexError:
            raise SerifIndexError(
                f"Index {key} out of range for vector length {len(vector)}"
            ) from None

    if isinstance(key, tuple):
        # Rank-1 vectors accept only single-element tuples. Deeper indexing
        # belongs to Table.
        if len(key) != len(vector.shape):
            raise SerifKeyError(
                "Matrix indexing must provide an index in each dimension: "
                f"{vector.shape}"
            )
        return vector[key[0]]

    key = vector._check_duplicate(key)
    if isinstance(key, Vector) and key.schema().kind == bool:
        # A nullable-mask null excludes the row (SQL WHERE semantics).
        if len(vector) != len(key):
            raise SerifValueError(
                f"Boolean mask length mismatch: {len(vector)} != {len(key)}"
            )
        return vector._clone(
            filter_storage(vector._storage, key._storage)
        )

    if isinstance(key, list) and {type(element) for element in key} == {bool}:
        if len(vector) != len(key):
            raise SerifValueError(
                f"Boolean mask length mismatch: {len(vector)} != {len(key)}"
            )
        return vector._clone(filter_storage(vector._storage, key))

    if isinstance(key, slice):
        return vector._clone(vector._storage.slice(key))

    # Integer-position collections are supported but discouraged.
    if (
        isinstance(key, Vector)
        and key.schema().kind == int
        and not key.schema().nullable
    ):
        if len(vector) > 1000:
            warnings.warn(
                "Subscript indexing is sub-optimal for large vectors; "
                "prefer slices or boolean masks"
            )
        return vector.copy(
            (vector[index] for index in key),
            name=vector._name,
        )

    if isinstance(key, list) and {type(element) for element in key} == {int}:
        if len(vector) > 1000:
            warnings.warn(
                "Subscript indexing is sub-optimal for large vectors"
            )
        return vector.copy(
            (vector[index] for index in key),
            name=vector._name,
        )

    raise SerifTypeError(
        "Vector indices must be boolean vectors, integer vectors or integers, "
        f"not {str(type(key))}"
    )
