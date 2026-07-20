"""Vector assignment, ownership, and copy-on-write semantics."""

from collections.abc import Iterable

from ..errors import SerifIndexError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from .dtype import Schema
from .dtype import infer_dtype
from .dtype import validate_scalar


def _vector_class():
    # Local import avoids a cycle while Vector delegates mutation here.
    from ..vector import Vector
    return Vector


def require_mutable(vector):
    """Raise if vector is a frozen table-owned column."""
    if vector._frozen:
        column = vector._name if vector._name is not None else 'col'
        raise SerifTypeError(
            "Read-out columns are values: this vector is owned by a "
            "Table and is frozen. Write through the owning table "
            "instead:\n"
            f"    t[key, {column!r}] = value\n"
            "For an independent mutable vector use .copy(); for bulk "
            "point-write loops use `with t.batch() as m:`."
        )


def require_mutable_metadata(vector):
    """Reject metadata mutation through a table-owned column."""
    if vector._frozen:
        column = vector._name if vector._name is not None else 'col'
        raise SerifTypeError(
            "Read-out columns are values: this vector is owned by a "
            "Table and its metadata is frozen. Rename through the table "
            "instead:\n"
            f"    t = t.rename({{{column!r}: 'new_name'}})\n"
            "For an independent renameable vector use .copy()."
        )


def setitem(vector, key, value):
    vector._require_mutable()
    vector._setitem_impl(key, value)


def setitem_impl(vector, key, value):
    """Plan, validate, and apply a Vector assignment."""
    Vector = _vector_class()
    key = vector._check_duplicate(key)
    value = vector._check_duplicate(value)

    is_sequence_value = (
        isinstance(value, Iterable)
        and not isinstance(value, (str, bytes, bytearray))
    )

    length = len(vector)
    underlying = vector._storage
    updates = []

    # Boolean mask. A null mask entry assigns nothing.
    if (
        isinstance(key, Vector) and key.schema().kind == bool
    ) or (
        isinstance(key, list)
        and all(isinstance(element, bool) for element in key)
    ):
        if len(key) != length:
            raise SerifValueError(
                "Boolean mask length must match vector length."
            )

        true_indices = [
            index
            for index, flag in enumerate(key)
            if flag
        ]
        true_count = len(true_indices)

        if is_sequence_value:
            if true_count != len(value):
                raise SerifValueError(
                    "Iterable length must match number of True mask elements."
                )
            for index, new_value in zip(true_indices, value):
                updates.append((index, new_value))
        else:
            for index in true_indices:
                updates.append((index, value))

    elif isinstance(key, slice):
        start, stop, step = key.indices(length)
        slice_length = len(range(start, stop, step))

        if is_sequence_value:
            if slice_length != len(value):
                raise SerifValueError(
                    "Slice length and value length must match."
                )
            values_to_assign = value
        else:
            values_to_assign = [value] * slice_length

        for index, new_value in zip(
            range(start, stop, step),
            values_to_assign,
        ):
            updates.append((index, new_value))

    elif isinstance(key, int):
        if key < 0:
            key += length
        if not (0 <= key < length):
            raise SerifIndexError(
                f"Index {key} out of range for vector length {length}"
            )
        updates.append((key, value))

    elif (
        isinstance(key, Vector)
        and key.schema().kind == int
        and not key.schema().nullable
    ):
        if is_sequence_value:
            if len(key) != len(value):
                raise SerifValueError(
                    "Index-vector length must match value length."
                )
            pairs = zip(key, value)
        else:
            pairs = ((index, value) for index in key)

        for index, new_value in pairs:
            if index < 0:
                index += length
            if not (0 <= index < length):
                raise SerifIndexError(f"Index {index} out of range.")
            updates.append((index, new_value))

    elif (
        isinstance(key, (list, tuple))
        and all(isinstance(element, int) for element in key)
    ):
        if is_sequence_value:
            if len(key) != len(value):
                raise SerifValueError(
                    "Index list must match value length."
                )
            pairs = zip(key, value)
        else:
            pairs = ((index, value) for index in key)

        for index, new_value in pairs:
            if index < 0:
                index += length
            if not (0 <= index < length):
                raise SerifIndexError(f"Index {index} out of range.")
            updates.append((index, new_value))

    else:
        raise SerifTypeError(
            f"Invalid key type: {type(key)}. Must be boolean mask, slice, "
            "int, integer vector, or list/tuple of ints."
        )

    if updates:
        new_values = [new_value for _, new_value in updates]

        if vector._dtype is not None and vector._dtype.kind is not object:
            incompatible = None
            saw_none = False
            for new_value in new_values:
                if new_value is None:
                    saw_none = True
                    continue
                try:
                    validate_scalar(new_value, vector._dtype)
                except TypeError:
                    incompatible = new_value
                    break

            if incompatible is not None:
                required_dtype = infer_dtype([incompatible])
                try:
                    vector._promote(required_dtype.kind)
                    underlying = vector._storage
                except SerifTypeError:
                    raise SerifTypeError(
                        f"Cannot set {required_dtype.kind.__name__} in "
                        f"{vector._dtype.kind.__name__} vector. "
                        f"Promotion not supported."
                    )

            if saw_none and not vector._dtype.nullable:
                vector._dtype = Schema(vector._dtype.kind, True)

    # In-place writes are legal only inside a batch scope whose buffers were
    # privatized on entry. Every other write rebuilds storage.
    if vector._inplace_ok and updates:
        write = getattr(vector._storage, 'write_inplace', None)
        if write is not None and write(updates):
            return

    data = list(underlying)
    for index, new_value in updates:
        data[index] = new_value

    nullable = vector._dtype.nullable if vector._dtype is not None else True
    from .construction import _storage_for_dtype
    vector._storage = _storage_for_dtype(vector._dtype, data, nullable)
