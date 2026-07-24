"""Vector cast, null, type, uniqueness, and ordering transforms."""

import builtins as _builtins
from datetime import date
from datetime import datetime

from ..errors import SerifTypeError
from ..errors import SerifValueError
from .dtype import Schema
from .dtype import infer_dtype
from .selection import take_storage
from .dtype import validate_scalar
from .storage import ArrayStorage
from .storage import BoolStorage
from .storage import StringStorage
from .storage import TupleStorage


def _vector_class():
    # Local import avoids a cycle while Vector delegates transforms here.
    from ..vector import Vector
    return Vector


def _null_sort_flag(is_null: bool, reverse: bool, na_last: bool) -> bool:
    """Place nulls first or last independently of sort direction."""
    return (is_null != reverse) if na_last else (is_null == reverse)


def cast(vector, target_type):
    py_target_type = target_type
    Vector = _vector_class()

    if target_type is date:
        def caster(value):
            if isinstance(value, date):
                return value
            return date.fromisoformat(value)
    elif target_type is datetime:
        def caster(value):
            if isinstance(value, datetime):
                return value
            return datetime.fromisoformat(value)
    else:
        caster = target_type

    def converted_values():
        for index, element in enumerate(vector._storage):
            if element is None:
                yield None
                continue

            try:
                if isinstance(element, Vector):
                    converted = element.cast(target_type)
                else:
                    converted = caster(element)
            except Exception as exc:
                type_name = getattr(
                    py_target_type,
                    "__name__",
                    repr(py_target_type),
                )
                raise SerifValueError(
                    f"Cast failed at index {index}: {element!r} cannot be "
                    f"converted to {type_name}"
                ) from exc
            yield converted

    if isinstance(py_target_type, type):
        return Vector._from_iterable_known_kind(
            converted_values(),
            py_target_type,
            name=vector._name,
        )

    output = list(converted_values())
    return Vector(
        output,
        dtype=infer_dtype(output),
        name=vector._name,
    )


def to_object(vector):
    return vector._clone(
        TupleStorage.from_iterable(vector._storage),
        dtype=Schema(
            object,
            vector._dtype.nullable if vector._dtype is not None else True,
        ),
        name=vector._name,
    )


def fillna(vector, value):
    dtype = vector.schema()
    Vector = _vector_class()

    if dtype is not None and value is not None:
        try:
            validate_scalar(value, dtype)
        except TypeError:
            required_dtype = infer_dtype([value])
            try:
                source_kind = dtype.kind
                target_kind = required_dtype.kind
                if target_kind is float and source_kind is int:
                    promote = float
                elif (
                    target_kind is complex
                    and source_kind in (int, float)
                ):
                    promote = complex
                elif target_kind is datetime and source_kind is date:
                    def promote(item):
                        return datetime.combine(item, datetime.min.time())
                else:
                    raise SerifTypeError(
                        f'Cannot convert Vector from '
                        f'{source_kind.__name__} to '
                        f'{target_kind.__name__}.'
                    )
                return Vector._from_iterable_known_dtype(
                    (
                        value if item is None else promote(item)
                        for item in vector._storage
                    ),
                    Schema(target_kind, False),
                    name=vector._name,
                )
            except SerifTypeError:
                raise SerifValueError(
                    f"fillna: value {value!r} "
                    f"(type {type(value).__name__}) cannot be used with "
                    f"{dtype.kind.__name__} vector. Promotion not supported."
                )

    new_nullable = value is None and (
        vector._dtype.nullable if vector._dtype is not None else True
    )
    new_dtype = None if dtype is None else Schema(dtype.kind, new_nullable)
    values = (
        value if item is None else item
        for item in vector._storage
    )
    if new_dtype is not None:
        return Vector._from_iterable_known_dtype(
            values,
            new_dtype,
            name=vector._name,
        )
    return Vector(tuple(values), dtype=None, name=vector._name)


def dropna(vector):
    storage = vector._storage
    new_dtype = (
        Schema(vector._dtype.kind, False)
        if vector._dtype is not None
        else None
    )
    kept = [index for index in range(len(storage)) if not storage.is_null(index)]
    return vector._clone(take_storage(storage, kept), dtype=new_dtype)


def is_na(vector):
    storage = vector._storage
    Vector = _vector_class()
    if isinstance(storage, (ArrayStorage, StringStorage, BoolStorage)):
        if storage._mask is None:
            result = BoolStorage(bytearray(len(storage)))
        else:
            result = BoolStorage(bytearray(
                1 if is_null else 0
                for is_null in storage._mask
            ))
        return Vector._from_storage(result, Schema(bool, False))
    return Vector._from_iterable_known_dtype(
        (element is None for element in storage),
        Schema(bool, False),
    )


def is_type(vector, types):
    Vector = _vector_class()
    return Vector._from_iterable_known_dtype(
        (isinstance(element, types) for element in vector._storage),
        Schema(bool, False),
    )


def unique(vector):
    Vector = _vector_class()
    seen = set()
    output = []
    has_none = False

    try:
        for element in vector._storage:
            if element not in seen:
                seen.add(element)
                output.append(element)
                if element is None:
                    has_none = True
        if vector._dtype is not None:
            return Vector(
                output,
                dtype=Schema(vector._dtype.kind, has_none),
            )
        return Vector(output)
    except TypeError:
        pass

    output = []
    has_none = False
    for element in vector._storage:
        if not _builtins.any(element == prior for prior in output):
            output.append(element)
            if element is None:
                has_none = True
    if vector._dtype is not None:
        return Vector(
            output,
            dtype=Schema(vector._dtype.kind, has_none),
        )
    return Vector(output)


def sort_by(vector, reverse=False, na_last=True):
    storage = vector._storage
    count = len(storage)

    def key(index):
        is_null = storage.is_null(index)
        return (
            _null_sort_flag(is_null, reverse, na_last),
            storage[index] if not is_null else 0,
        )

    order = _builtins.sorted(
        range(count),
        key=key,
        reverse=reverse,
    )
    return vector._clone(take_storage(storage, order))
