"""Vector assignment, ownership, and copy-on-write semantics."""

from collections.abc import Iterable
from datetime import date
from datetime import datetime
from weakref import ref

from ..errors import SerifIndexError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from .dtype import Schema
from .dtype import infer_dtype
from .dtype import validate_scalar
from .storage import storage_from_known_iterable


class _EditToken:
    """Short-lived authority for mutating columns in one batch scope."""

    __slots__ = ('active', '_table', '_refresh_metadata')

    def __init__(self, table, refresh_metadata):
        self.active = True
        self._table = ref(table)
        self._refresh_metadata = refresh_metadata

    def metadata_changed(self, vector):
        table = self._table()
        if self.active and table is not None:
            self._refresh_metadata(table, changed_column=vector)


def _is_editable(vector):
    owner = vector._owner
    return isinstance(owner, _EditToken) and owner.active


def metadata_changed(vector):
    """Refresh owner metadata after an editable column is renamed."""
    owner = vector._owner
    if isinstance(owner, _EditToken):
        owner.metadata_changed(vector)


def _vector_class():
    # Local import avoids a cycle while Vector delegates mutation here.
    from ..vector import Vector
    return Vector


def require_mutable(vector):
    """Raise if vector is a frozen table-owned column."""
    if vector._owner is not None and not _is_editable(vector):
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
    if vector._owner is not None and not _is_editable(vector):
        column = vector._name if vector._name is not None else 'col'
        raise SerifTypeError(
            "Read-out columns are values: this vector is owned by a "
            "Table and its metadata is frozen. Rename through the table "
            "instead:\n"
            f"    t = t.rename_columns({{{column!r}: 'new_name'}})\n"
            "For an independent renameable vector use .copy()."
        )


def setitem(vector, key, value):
    require_mutable(vector)
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

    target_dtype = vector._dtype
    validated_updates = updates
    if updates and target_dtype is not None and target_dtype.kind is not object:
        new_values = [new_value for _, new_value in updates]
        saw_none = any(new_value is None for new_value in new_values)
        incompatible = None
        for new_value in new_values:
            if new_value is None:
                continue
            try:
                validate_scalar(new_value, target_dtype)
            except SerifTypeError:
                incompatible = new_value
                break

        if incompatible is not None:
            required_dtype = infer_dtype([incompatible])
            current_kind = target_dtype.kind
            required_kind = required_dtype.kind
            if current_kind is int and required_kind in (float, complex):
                target_kind = required_kind
            elif current_kind is float and required_kind is complex:
                target_kind = complex
            elif current_kind is date and required_kind is datetime:
                target_kind = datetime
            else:
                raise SerifTypeError(
                    f"Cannot set {required_kind.__name__} in "
                    f"{current_kind.__name__} vector. "
                    f"Promotion not supported."
                )
            target_dtype = Schema(target_kind, target_dtype.nullable)

        if saw_none and not target_dtype.nullable:
            target_dtype = Schema(target_dtype.kind, True)

        # Validate every value against the final planned schema before any
        # dtype, class, storage, or in-place buffer mutation occurs.
        validated_updates = [
            (index, validate_scalar(new_value, target_dtype))
            for index, new_value in updates
        ]

    # The active batch token is granted only after buffers are privatized.
    # Every other write rebuilds storage.
    if (
        _is_editable(vector)
        and validated_updates
        and (
            vector._dtype is None
            or target_dtype.kind is vector._dtype.kind
        )
    ):
        write = getattr(vector._storage, 'write_inplace', None)
        if write is not None and write(validated_updates):
            vector._dtype = target_dtype
            return

    data = list(underlying)
    for index, new_value in validated_updates:
        data[index] = new_value

    if target_dtype is not None and target_dtype.kind is not object:
        data = [validate_scalar(value, target_dtype) for value in data]

    kind = target_dtype.kind if target_dtype is not None else None
    new_storage = storage_from_known_iterable(data, kind)
    target_class = type(vector)
    if (
        vector._dtype is not None
        and target_dtype.kind is not vector._dtype.kind
    ):
        target_class = type(Vector._from_storage(new_storage, target_dtype))

    # Commit only after planning, validation, coercion, and storage building
    # have all succeeded.
    vector._storage = new_storage
    vector._dtype = target_dtype
    vector.__class__ = target_class
