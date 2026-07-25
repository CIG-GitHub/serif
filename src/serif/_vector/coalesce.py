"""Left-biased null coalescing for one-dimensional Vectors."""

from datetime import datetime

from ..errors import SerifTypeError
from ..errors import SerifValueError
from .dtype import Schema
from .dtype import is_numeric_kind
from .dtype import is_temporal_kind
from .dtype import validate_scalar


def _vector_class():
    # Local import avoids a cycle while Vector delegates coalescing here.
    from ..vector import Vector
    return Vector


def _has_non_null(vector):
    storage = vector._storage
    return any(
        not storage.is_null(index)
        for index in range(len(storage))
    )


def _promote_kinds(left, right):
    if left is right:
        return left
    if left is object or right is object:
        return object
    if is_numeric_kind(left) and is_numeric_kind(right):
        if left is complex or right is complex:
            return complex
        if left is float or right is float:
            return float
        if left is int or right is int:
            return int
        return bool
    if is_temporal_kind(left) and is_temporal_kind(right):
        if left is datetime or right is datetime:
            return datetime
        return left
    return None


def _common_kind(vectors):
    contributors = []
    saw_null_object = False

    for vector in vectors:
        schema = vector.schema()
        if schema is None:
            continue
        if schema.kind is object and not _has_non_null(vector):
            # An all-null object vector is a polymorphic NULL source, not
            # evidence that the result must use heterogeneous storage.
            saw_null_object = True
            continue
        contributors.append(vector)

    if not contributors:
        return (object if saw_null_object else None), contributors

    kind = contributors[0].schema().kind
    for vector in contributors[1:]:
        next_kind = vector.schema().kind
        promoted = _promote_kinds(kind, next_kind)
        if promoted is None:
            kinds = ", ".join(
                contributor.schema().kind.__name__
                for contributor in contributors
            )
            raise SerifTypeError(
                f"coalesce() cannot combine incompatible kinds: {kinds}"
            )
        kind = promoted
    return kind, contributors


def _shared_categories(kind, contributors):
    if kind is not str or not contributors:
        return None

    from .categorical import _Category

    if not all(isinstance(vector, _Category) for vector in contributors):
        return None
    categories = contributors[0].categories
    if all(vector.categories == categories for vector in contributors[1:]):
        return categories
    return None


def coalesce(vector, *others):
    """Take the first non-None value at each position."""
    Vector = _vector_class()
    if not others:
        raise SerifValueError(
            "Vector.coalesce() requires at least one fallback Vector"
        )

    vectors = (vector, *others)
    length = len(vector)
    for position, other in enumerate(others, start=1):
        if not isinstance(other, Vector) or other.ndims() != 1:
            raise SerifTypeError(
                "Vector.coalesce() expects only Vector arguments; "
                f"argument {position} is {type(other).__name__}"
            )
        if len(other) != length:
            raise SerifValueError(
                "Vector.coalesce() length mismatch at argument "
                f"{position}: {length} != {len(other)}"
            )

    result_kind, contributors = _common_kind(vectors)
    if result_kind is None:
        # All operands are dtype-less empty vectors.
        return vector.copy(name=vector._name)

    target_schema = Schema(result_kind, True)

    def values():
        storages = tuple(candidate._storage for candidate in vectors)
        for index in range(length):
            for storage in storages:
                value = storage[index]
                if value is None:
                    continue
                if result_kind is object:
                    yield value
                else:
                    try:
                        yield validate_scalar(value, target_schema)
                    except TypeError as exc:
                        raise SerifTypeError(
                            "coalesce() could not convert value at row "
                            f"{index} to {result_kind.__name__}"
                        ) from exc
                break
            else:
                yield None

    categories = _shared_categories(result_kind, contributors)
    if categories is not None:
        from .categorical import _Category
        return _Category.from_values(
            values(),
            categories,
            name=vector._name,
        )
    return Vector._from_iterable_known_kind(
        values(),
        result_kind,
        name=vector._name,
    )
