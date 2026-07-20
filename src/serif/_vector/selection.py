"""Vector indexing and selection semantics."""

import warnings

from .._accel.api import _accel_filter
from ..errors import SerifIndexError
from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError


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
        fast = _accel_filter(vector._storage, key._storage)
        if fast is not None:
            return vector._clone(fast)
        return vector.copy(
            (x for x, flag in zip(vector, key, strict=True) if flag),
            name=vector._name,
        )

    if isinstance(key, list) and {type(element) for element in key} == {bool}:
        if len(vector) != len(key):
            raise SerifValueError(
                f"Boolean mask length mismatch: {len(vector)} != {len(key)}"
            )
        fast = _accel_filter(vector._storage, key)
        if fast is not None:
            return vector._clone(fast)
        return vector.copy(
            (x for x, flag in zip(vector, key, strict=True) if flag),
            name=vector._name,
        )

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
