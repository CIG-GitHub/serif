"""
DataType system for Vector / Table.

Pure metadata design:
  - DataType describes column semantics (type + nullable flag)
  - Null masks live on Vector instances, not in DataType
  - Promotion is functional (immutable DataType instances)
  - Backend-agnostic and stable
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Type
import warnings


@dataclass(frozen=True)
class DataType:
    """
    Describes the semantic type of a Vector column.

    Attributes
    ----------
    kind : Type
        Python type (int, float, str, date, etc.)
    nullable : bool
        Whether the column may contain None values
    """

    kind: Type[Any]
    nullable: bool = False

    def __repr__(self):
        if self.nullable:
            return f"<{self.kind.__name__}?>"
        return f"<{self.kind.__name__}>"

    def with_nullable(self, nullable: bool) -> DataType:
        return DataType(self.kind, nullable=nullable)

    @property
    def is_numeric(self) -> bool:
        """True if kind is bool, int, float, or complex."""
        try:
            return issubclass(self.kind, (int, float, complex, bool))
        except TypeError:
            return False

    @property
    def is_temporal(self) -> bool:
        """True if kind is date or datetime."""
        try:
            return issubclass(self.kind, (date, datetime))
        except TypeError:
            return False

    def promote_with(self, value: Any) -> DataType:
        """Promote this DataType to accommodate a new Python value."""
        if value is None:
            if self.nullable:
                return self
            return DataType(self.kind, nullable=True)

        vtype = type(value)

        if vtype is self.kind:
            return self

        if self.is_numeric and isinstance(value, (int, float, complex, bool)):
            if self.kind is complex or vtype is complex:
                new_kind = complex
            elif self.kind is float or vtype is float:
                new_kind = float
            elif self.kind is int or vtype is int:
                new_kind = int
            else:
                new_kind = bool
            if new_kind != self.kind:
                return DataType(new_kind, self.nullable)
            return self

        if self.is_temporal and isinstance(value, (date, datetime)):
            if self.kind is datetime or vtype is datetime:
                new_kind = datetime
            else:
                new_kind = date
            if new_kind != self.kind:
                return DataType(new_kind, self.nullable)
            return self

        if self.kind in (str, bytes) and vtype is self.kind:
            return self

        if self.kind is not object:
            warnings.warn(
                f"Degrading column<{self.kind.__name__}> to column<object> "
                f"due to incompatible value of type {vtype.__name__}",
                stacklevel=3,
            )
            return DataType(object, self.nullable)

        return self


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


def infer_dtype(values: Iterable[Any]) -> DataType:
    """Infer a DataType from an iterable of Python scalars."""
    dtype: Optional[DataType] = None

    for v in values:
        if dtype is None:
            k = infer_kind(v)
            if k is None:
                dtype = DataType(object, nullable=True)
            else:
                dtype = DataType(k, nullable=False)
        else:
            dtype = dtype.promote_with(v)

    if dtype is None:
        return DataType(object, nullable=True)

    return dtype


def validate_scalar(value: Any, dtype: DataType) -> Any:
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
