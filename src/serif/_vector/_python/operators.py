"""Canonical pure-Python physical implementations for Vector operators."""

from array import array as _array

from ..storage import ArrayStorage
from ..storage import BoolStorage
from ..storage import TupleStorage
from ..storage import storage_from_known_iterable


class _BinaryOperationTypeError(TypeError):
    """Distinguish operator failures from known-storage construction errors."""


def compare_vector(left, right, op):
    return BoolStorage.from_iterable(
        None if (x is None or y is None) else bool(op(x, y))
        for x, y in zip(left, right, strict=True)
    )


def compare_scalar(storage, other, op):
    return BoolStorage.from_iterable(
        None if (x is None or other is None) else bool(op(x, other))
        for x in storage
    )


def logical_vector(left, right, kleene_func):
    return BoolStorage.from_iterable(
        kleene_func(x, y)
        for x, y in zip(left, right, strict=True)
    )


def logical_scalar(storage, other, kleene_func):
    return BoolStorage.from_iterable(
        kleene_func(x, other)
        for x in storage
    )


def _known_binary_vector_values(left, right, op_func):
    for x, y in zip(left, right, strict=True):
        if x is None or y is None:
            yield None
            continue
        try:
            value = op_func(x, y)
        except TypeError as error:
            raise _BinaryOperationTypeError from error
        yield value


def _known_binary_scalar_values(storage, other, op_func):
    for x in storage:
        if x is None:
            yield None
            continue
        try:
            value = op_func(x, other)
        except TypeError as error:
            raise _BinaryOperationTypeError from error
        yield value


def binary_vector(left, right, op_func, result_kind=None):
    if result_kind is None:
        return tuple(
            None if (x is None or y is None) else op_func(x, y)
            for x, y in zip(left, right, strict=True)
        )
    return storage_from_known_iterable(
        _known_binary_vector_values(left, right, op_func),
        result_kind,
    )


def binary_scalar(storage, other, op_func, result_kind=None):
    if result_kind is None:
        return tuple(
            None if x is None else op_func(x, other)
            for x in storage
        )
    return storage_from_known_iterable(
        _known_binary_scalar_values(storage, other, op_func),
        result_kind,
    )


def unary_storage(storage, op_func):
    if isinstance(storage, ArrayStorage):
        typecode = storage._data.typecode
        new_data = _array(
            typecode,
            (op_func(storage._data[i]) for i in range(len(storage._data))),
        )
        return ArrayStorage(new_data, storage._mask)
    return TupleStorage(tuple(
        None if value is None else op_func(value)
        for value in storage
    ))


def invert_bool(storage):
    return BoolStorage.from_iterable(
        None if value is None else (not value)
        for value in storage
    )
