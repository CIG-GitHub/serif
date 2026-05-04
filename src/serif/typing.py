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

    Notes
    -----
    - DataType holds zero instance data (no masks, no defaults)
    - Promotion never mutates — always returns new DataType
    - This is backend-agnostic and forms the semantic core
    
    Examples
    --------
    >>> DataType(int)
    DataType(kind=<class 'int'>, nullable=False)
    >>> DataType(int, nullable=True)
    DataType(kind=<class 'int'>, nullable=True)
    >>> DataType(float).promote_with(None)
    <float?>
    """

    kind: Type[Any]
    nullable: bool = False

    def __repr__(self):
        if self.nullable:
            return f"<{self.kind.__name__}?>"
        return f"<{self.kind.__name__}>"
    
    def with_nullable(self, nullable: bool) -> DataType:
        """Return a new DataType with the specified nullable flag.
        
        Parameters
        ----------
        nullable : bool
            Whether the new type should be nullable
        
        Returns
        -------
        DataType
            New DataType instance with updated nullable flag
        
        Examples
        --------
        >>> dt = DataType(int, nullable=True)
        >>> dt.with_nullable(False)
        DataType(kind=<class 'int'>, nullable=False)
        """
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

    def promote_with(self, value: Any) -> "DataType":
        """
        Promote this DataType to accommodate a new Python value.
        
        Never mutates; always returns new DataType.
        
        Parameters
        ----------
        value : Any
            Python scalar to accommodate
            
        Returns
        -------
        DataType
            New (possibly promoted) DataType
        """
        # Case 1: None just lifts nullability
        if value is None:
            if self.nullable:
                return self
            return DataType(self.kind, nullable=True)

        vtype = type(value)

        # Case 2: Exact match
        if vtype is self.kind:
            return self

        # Case 3: Numeric ladder (bool → int → float → complex)
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

        # Case 4: Temporal ladder (date → datetime)
        if self.is_temporal and isinstance(value, (date, datetime)):
            if self.kind is datetime or vtype is datetime:
                new_kind = datetime
            else:
                new_kind = date
            
            if new_kind != self.kind:
                return DataType(new_kind, self.nullable)
            return self

        # Case 5: String/bytes stay as-is for same type
        if self.kind in (str, bytes) and vtype is self.kind:
            return self

        # Case 6: Degrade to object
        if self.kind is not object:
            warnings.warn(
                f"Degrading column<{self.kind.__name__}> to column<object> "
                f"due to incompatible value of type {vtype.__name__}",
                stacklevel=3,
            )
            return DataType(object, self.nullable)

        # Already object — trivial
        return self


def infer_kind(value: Any) -> Optional[Type]:
    """
    Infer Python type for a single scalar.
    
    Returns None for None values.
    """
    if value is None:
        return None
    
    # Check bool BEFORE int (bool is subclass of int)
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
    
    # Check datetime BEFORE date (datetime is subclass of date)
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
    
    # For any other type, return its actual type instead of generic 'object'
    # This allows uniform columns (e.g., all decimal.Decimal) to show the specific type
    return type(value)


def infer_dtype(values: Iterable[Any]) -> DataType:
    """
    Infer a DataType from an iterable of Python scalars.
    
    Applies promotion across all values.
    
    Parameters
    ----------
    values : Iterable[Any]
        Python scalars to analyze
        
    Returns
    -------
    DataType
        Inferred dtype
        
    Examples
    --------
    >>> infer_dtype([1, 2, 3])
    <int>
    >>> infer_dtype([1, 2.5, 3])
    <float>
    >>> infer_dtype([1, None, 3])
    <int?>
    >>> infer_dtype([1, "hello"])
    <object>
    """
    dtype: Optional[DataType] = None

    for v in values:
        if dtype is None:
            # First element
            k = infer_kind(v)
            if k is None:
                dtype = DataType(object, nullable=True)
            else:
                dtype = DataType(k, nullable=False)
        else:
            dtype = dtype.promote_with(v)

    # If all values were None or empty iterable
    if dtype is None:
        return DataType(object, nullable=True)

    return dtype


def validate_scalar(value: Any, dtype: DataType) -> Any:
    """
    Validate (and possibly coerce) a scalar before writing into a vector.
    
    Parameters
    ----------
    value : Any
        Scalar to validate
    dtype : DataType
        Target dtype
        
    Returns
    -------
    Any
        Validated/coerced scalar
        
    Raises
    ------
    TypeError
        If value is incompatible with dtype
    """
    if value is None:
        if not dtype.nullable:
            raise TypeError(
                f"Cannot store None in non-nullable {dtype.kind.__name__} column"
            )
        return None

    vtype = type(value)

    # Exact match
    if vtype is dtype.kind:
        return value

    # Numeric coercions
    if dtype.kind is float and vtype in (int, bool):
        return float(value)
    if dtype.kind is int and vtype is bool:
        return int(value)
    if dtype.kind is complex and vtype in (int, float, bool):
        return complex(value)

    # Temporal promotion
    if dtype.kind is datetime and vtype is date:
        return datetime.combine(value, datetime.min.time())

    # Otherwise incompatible
    raise TypeError(
        f"Incompatible value {value!r} for column<{dtype.kind.__name__}>"
    )




