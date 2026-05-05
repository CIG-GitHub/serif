"""
Type inference and scalar validation for Vector / Table.

Schema is a lightweight namedtuple (kind, nullable) returned by Vector.schema().
All promotion, inference, and validation logic lives here as standalone functions.
"""

from __future__ import annotations
from collections import namedtuple
from datetime import date
from datetime import datetime
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Type
import warnings


Schema = namedtuple('Schema', ['kind', 'nullable'])
"""
Describes the semantic type of a Vector column.

Fields
------
kind : type
    Python type (int, float, str, date, etc.)
nullable : bool
    Whether the column may contain None values
"""


def is_numeric_kind(kind: Type) -> bool:
    """True if kind is bool, int, float, or complex."""
    try:
        return issubclass(kind, (int, float, complex, bool))
    except TypeError:
        return False


def is_temporal_kind(kind: Type) -> bool:
    """True if kind is date or datetime."""
    try:
        return issubclass(kind, (date, datetime))
    except TypeError:
        return False


def promote_dtype(schema: Schema, value: Any) -> Schema:
    """
    Return a new Schema promoted to accommodate value.

    None values update nullable only; the kind is unchanged.
    Mixed incompatible types degrade to object with a warning.
    """
    if value is None:
        if schema.nullable:
            return schema
        return Schema(schema.kind, True)

    vtype = type(value)

    if vtype is schema.kind:
        return schema

    if is_numeric_kind(schema.kind) and isinstance(value, (int, float, complex, bool)):
        if schema.kind is complex or vtype is complex:
            new_kind = complex
        elif schema.kind is float or vtype is float:
            new_kind = float
        elif schema.kind is int or vtype is int:
            new_kind = int
        else:
            new_kind = bool
        if new_kind is not schema.kind:
            return Schema(new_kind, schema.nullable)
        return schema

    if is_temporal_kind(schema.kind) and isinstance(value, (date, datetime)):
        if schema.kind is datetime or vtype is datetime:
            new_kind = datetime
        else:
            new_kind = date
        if new_kind is not schema.kind:
            return Schema(new_kind, schema.nullable)
        return schema

    if schema.kind in (str, bytes) and vtype is schema.kind:
        return schema

    if schema.kind is not object:
        warnings.warn(
            f"Degrading column<{schema.kind.__name__}> to column<object> "
            f"due to incompatible value of type {vtype.__name__}",
            stacklevel=3,
        )
        return Schema(object, schema.nullable)

    return schema


def infer_kind(value: Any) -> Optional[Type]:
    """Infer Python type for a single scalar. Returns None for None values."""
    if value is None:
        return None
    if isinstance(value, bool):
        return bool
    if isinstance(value, int):
        return int
    if isinstance(value, float):
        return float
    if isinstance(value, complex):
        return complex
    if isinstance(value, str):
        return str
    if isinstance(value, bytes):
        return bytes
    if isinstance(value, datetime):
        return datetime
    if isinstance(value, date):
        return date
    if isinstance(value, list):
        return list
    if isinstance(value, dict):
        return dict
    if isinstance(value, tuple):
        return tuple
    return type(value)


def infer_dtype(values: Iterable[Any]) -> Schema:
    """Infer a Schema (kind + nullable) from an iterable of Python scalars."""
    schema: Optional[Schema] = None

    for v in values:
        if schema is None:
            k = infer_kind(v)
            schema = Schema(object, True) if k is None else Schema(k, False)
        else:
            schema = promote_dtype(schema, v)

    if schema is None:
        return Schema(object, True)

    return schema


def validate_scalar(value: Any, dtype: Schema) -> Any:
    """Validate (and possibly coerce) a scalar before writing into a vector."""
    if value is None:
        if not dtype.nullable:
            raise TypeError(
                f"Cannot store None in non-nullable {dtype.kind.__name__} column"
            )
        return None

    vtype = type(value)

    if vtype is dtype.kind:
        return value

    if dtype.kind is float and vtype in (int, bool):
        return float(value)
    if dtype.kind is int and vtype is bool:
        return int(value)
    if dtype.kind is complex and vtype in (int, float, bool):
        return complex(value)

    if dtype.kind is datetime and vtype is date:
        return datetime.combine(value, datetime.min.time())

    raise TypeError(
        f"Incompatible value {value!r} for column<{dtype.kind.__name__}>"
    )
