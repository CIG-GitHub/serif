"""Vector construction, subtype selection, and storage selection."""

from datetime import date

from ..errors import SerifValueError
from .dtype import Schema
from .dtype import infer_dtype
from .dtype import infer_kind
from .dtype import promote_dtype
from .storage import ArrayStorage
from .storage import BoolStorage
from .storage import StringStorage
from .storage import TupleStorage
from .storage import storage_from_known_iterable


def _vector_class():
    # Local import avoids a cycle while Vector delegates construction here.
    from ..vector import Vector
    return Vector


def _collect_and_infer(iterable, dtype_hint):
    """Collect once while detecting Table candidates and inferring dtype."""
    Vector = _vector_class()
    data = []
    all_vectors = True
    dtype = dtype_hint
    saw_none = False
    saw_vector = False

    for value in iterable:
        data.append(value)
        if isinstance(value, Vector):
            # Columns do not inform a scalar dtype. If the collection later
            # proves mixed, inference is repeated over the collected values.
            saw_vector = True
            continue
        all_vectors = False
        if dtype is None:
            if value is None:
                saw_none = True
                continue
            dtype = Schema(infer_kind(value), saw_none)
        else:
            dtype = promote_dtype(dtype, value)

    if saw_vector and not all_vectors:
        dtype = dtype_hint
        saw_none = False
        for value in data:
            if dtype is None:
                if value is None:
                    saw_none = True
                    continue
                dtype = Schema(infer_kind(value), saw_none)
            else:
                dtype = promote_dtype(dtype, value)

    if dtype is None and saw_none:
        dtype = Schema(object, True)

    return data, all_vectors, dtype


def _storage_for_dtype(dtype, data, nullable):
    """Build storage from a Schema, including post-promotion rebuilds."""
    kind = dtype.kind if dtype is not None else None
    if kind is int:
        try:
            return ArrayStorage.from_iterable(
                data,
                typecode='q',
                nullable=nullable,
            )
        except OverflowError:
            # Python integers remain exact when they do not fit int64.
            return TupleStorage.from_iterable(data, nullable=nullable)
    if kind is float:
        return ArrayStorage.from_iterable(
            data,
            typecode='d',
            nullable=nullable,
        )
    if kind is str:
        return StringStorage.from_iterable(data)
    return TupleStorage.from_iterable(data, nullable=nullable)


def _storage_has_nulls(storage):
    if isinstance(storage, (ArrayStorage, BoolStorage, StringStorage)):
        return storage._mask is not None
    return any(value is None for value in storage)


def _pick_target_class(dtype):
    """Return the concrete Vector subclass for a Schema."""
    Vector = _vector_class()
    if dtype is None:
        return Vector
    if dtype.kind is str:
        from .string import _String
        return _String
    if dtype.kind is int:
        from .numeric import _Int
        return _Int
    if dtype.kind is float:
        from .numeric import _Float
        return _Float
    if dtype.kind is date:
        from .dates import _Date
        return _Date
    return Vector


def new(cls, initial=(), dtype=None, name=None, **kwargs):
    Vector = _vector_class()

    # Subclasses allocate normally; initialize() handles their state.
    if cls is not Vector:
        return object.__new__(cls)

    if dtype is not None and not isinstance(dtype, Schema):
        dtype_hint = Schema(dtype, False)
    else:
        dtype_hint = dtype

    # Internal materialized calls with a full Schema bypass inference.
    if isinstance(initial, (list, tuple)) and isinstance(dtype_hint, Schema):
        data = initial
        is_table = False
        dtype = dtype_hint
    else:
        data, is_table, dtype = _collect_and_infer(initial, dtype_hint)

    if is_table and data:
        if len({len(value) for value in data}) == 1:
            from ..table import Table
            return Table(initial=data, dtype=dtype, name=name)
        raise SerifValueError(
            "Passing vectors of different length will not produce a Table."
        )

    target_class = _pick_target_class(dtype)
    instance = object.__new__(target_class)
    instance._dtype = dtype
    instance._name = name
    instance._wild = True
    nullable = dtype.nullable if dtype is not None else True
    instance._storage = instance._build_storage(data, nullable)
    return instance


def initialize(vector, initial=(), dtype=None, name=None, **kwargs):
    # new() fully initializes factory-created instances.
    if '_storage' in vector.__dict__:
        return

    vector._name = name
    vector._wild = True
    if dtype is not None:
        if not isinstance(dtype, Schema):
            dtype = Schema(dtype, False)
        vector._dtype = dtype
    nullable = vector._dtype.nullable if vector._dtype is not None else True
    vector._storage = vector._build_storage(initial, nullable)


def build_storage(vector, data, nullable):
    typecode = getattr(vector, 'typecode', None)
    if typecode is not None:
        return ArrayStorage.from_iterable(
            data,
            typecode=typecode,
            nullable=nullable,
        )
    if getattr(vector, '_dtype', None) is not None and vector._dtype.kind is str:
        return StringStorage.from_iterable(data)
    if getattr(vector, '_dtype', None) is not None and vector._dtype.kind is bool:
        return BoolStorage.from_iterable(data, nullable=nullable)
    return TupleStorage.from_iterable(data, nullable=nullable)


def clone(vector, new_storage, dtype=..., name=...):
    instance = object.__new__(type(vector))
    instance._dtype = vector._dtype if dtype is ... else dtype
    instance._name = vector._name if name is ... else name
    instance._wild = True
    instance._storage = new_storage
    return instance


def from_storage(cls, storage, dtype, name=None):
    target_class = _pick_target_class(dtype) if dtype is not None else cls
    instance = object.__new__(target_class)
    instance._dtype = dtype
    instance._name = name
    instance._wild = False
    instance._storage = storage
    return instance


def from_iterable_known_dtype(cls, iterable, dtype, *, name=None):
    target_class = _pick_target_class(dtype)
    instance = object.__new__(target_class)
    instance._dtype = dtype
    instance._name = name
    instance._wild = True
    instance._storage = storage_from_known_iterable(iterable, dtype.kind)
    return instance


def from_iterable_known_kind(cls, iterable, kind, *, name=None):
    """Build storage first, deriving only result nullability from its mask."""
    storage = storage_from_known_iterable(iterable, kind)
    dtype = Schema(kind, _storage_has_nulls(storage))
    instance = from_storage(cls, storage, dtype, name=name)
    instance._wild = True
    return instance


def filled(cls, value, length, typesafe=False):
    if length:
        assert isinstance(length, int)
        dtype = infer_dtype([value])
        if typesafe:
            dtype = Schema(dtype.kind, False)
        return cls([value for _ in range(length)], dtype=dtype)

    dtype = (
        infer_dtype([value])
        if value is not None
        else Schema(object, False)
    )
    if typesafe:
        dtype = Schema(dtype.kind, False)
    return cls(dtype=dtype)


def copy(vector, new_values=None, name=...):
    use_name = vector._name if name is ... else name
    if vector._dtype is not None:
        if new_values is None:
            return vector._clone(vector._storage, name=use_name)
        return vector._clone(
            vector._build_storage(new_values, vector._dtype.nullable),
            name=use_name,
        )

    # A dtype-unknown Vector or Table needs full class dispatch. An explicitly
    # empty replacement is distinct from copying the original storage.
    source = (
        list(new_values)
        if new_values is not None
        else list(vector._storage)
    )
    return _vector_class()(source, dtype=None, name=use_name)
