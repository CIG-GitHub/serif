"""
DataType system for Vector / Table.

Pure metadata design:
  - DataType describes column semantics (type + nullable flag)
  - Null masks live on Vector instances, not in DataType
  - Promotion is functional (immutable DataType instances)
  - Backend-agnostic and stable
  
Sized Type System (0.2.0+):
  - Explicit numeric sizes: int8, int16, int32, int64
  - Unsigned support: uint8, uint16, uint32, uint64
  - Float precision: float32, float64
  - Automatic promotion on overflow/mixed operations
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Type
from typing import Union
import warnings


# ============================================================
# Sized Type Registry
# ============================================================

# Type metadata: (category, size_bits, signed, min, max)
_TYPE_REGISTRY = {
    # Signed integers
    'int8': ('int', 8, True, -(2**7), 2**7 - 1),
    'int16': ('int', 16, True, -(2**15), 2**15 - 1),
    'int32': ('int', 32, True, -(2**31), 2**31 - 1),
    'int64': ('int', 64, True, -(2**63), 2**63 - 1),
    
    # Unsigned integers
    'uint8': ('uint', 8, False, 0, 2**8 - 1),
    'uint16': ('uint', 16, False, 0, 2**16 - 1),
    'uint32': ('uint', 32, False, 0, 2**32 - 1),
    'uint64': ('uint', 64, False, 0, 2**64 - 1),
    
    # Floating point
    'float32': ('float', 32, True, None, None),
    'float64': ('float', 64, True, None, None),
}

# Legacy Python type → default sized type
_LEGACY_TYPE_MAP = {
    int: 'int64',
    float: 'float64',
    bool: 'uint8',  # bool fits in 0-255 range
}

# Promotion ladders (ordered by increasing capacity)
_INT_PROMOTION_LADDER = ['int8', 'int16', 'int32', 'int64']
_UINT_PROMOTION_LADDER = ['uint8', 'uint16', 'uint32', 'uint64']
_FLOAT_PROMOTION_LADDER = ['float32', 'float64']


def normalize_type(kind: Union[Type, str]) -> Union[Type, str]:
    """
    Normalize a type to canonical form.
    
    - Python types map to sized equivalents (int → int64, float → float64)
    - String types are validated and returned as-is
    - Other Python types (str, date, object) returned as-is
    
    Parameters
    ----------
    kind : Type or str
        Python type or string type name
        
    Returns
    -------
    Type or str
        Normalized type
        
    Examples
    --------
    >>> normalize_type(int)
    'int64'
    >>> normalize_type('int32')
    'int32'
    >>> normalize_type(str)
    <class 'str'>
    """
    # Handle legacy Python numeric types
    if kind in _LEGACY_TYPE_MAP:
        return _LEGACY_TYPE_MAP[kind]
    
    # Validate string types
    if isinstance(kind, str):
        if kind not in _TYPE_REGISTRY:
            raise ValueError(f"Unknown sized type: {kind!r}")
        return kind
    
    # Pass through other Python types (str, date, object, etc.)
    return kind


def get_type_metadata(kind: Union[Type, str]) -> Optional[tuple]:
    """Get metadata for a sized type, or None for Python types."""
    if isinstance(kind, str):
        return _TYPE_REGISTRY.get(kind)
    return None


def promote_numeric_types(left: str, right: str) -> Union[str, Type]:
    """
    Determine promoted type for numeric operation between two sized types.
    
    Rules:
    - Same ladder (both int or both uint): promote to wider
    - Mixed signedness: promote to wider signed, or object if can't fit
    - Int + float: promote to float of sufficient precision
    - Float + float: promote to float64
    
    Parameters
    ----------
    left, right : str
        Sized type names (e.g., 'int32', 'uint16', 'float32')
        
    Returns
    -------
    str or Type
        Promoted type, or `object` if no sized type can represent result
    """
    left_meta = _TYPE_REGISTRY.get(left)
    right_meta = _TYPE_REGISTRY.get(right)
    
    if not left_meta or not right_meta:
        return object
    
    left_cat, left_bits, left_signed, _, _ = left_meta
    right_cat, right_bits, right_signed, _, _ = right_meta
    
    # Float + anything → float
    if left_cat == 'float' or right_cat == 'float':
        # Use float64 for safety (covers all int ranges)
        return 'float64'
    
    # Both signed int: promote to wider
    if left_cat == 'int' and right_cat == 'int':
        target_bits = max(left_bits, right_bits)
        for typ in _INT_PROMOTION_LADDER:
            if _TYPE_REGISTRY[typ][1] >= target_bits:
                return typ
        return object  # Exceeds int64
    
    # Both unsigned int: promote to wider
    if left_cat == 'uint' and right_cat == 'uint':
        target_bits = max(left_bits, right_bits)
        for typ in _UINT_PROMOTION_LADDER:
            if _TYPE_REGISTRY[typ][1] >= target_bits:
                return typ
        return object  # Exceeds uint64
    
    # Mixed signed/unsigned: promote to signed with double bits (if possible)
    if (left_cat == 'int' and right_cat == 'uint') or (left_cat == 'uint' and right_cat == 'int'):
        max_bits = max(left_bits, right_bits)
        target_bits = max_bits * 2
        
        # Try to find signed int with sufficient capacity
        for typ in _INT_PROMOTION_LADDER:
            if _TYPE_REGISTRY[typ][1] >= target_bits:
                return typ
        
        # Can't fit in int64 → demote to object
        return object
    
    return object


def promote_to_next_size(current: str) -> Union[str, Type]:
    """
    Promote a sized type to the next larger size in its ladder.
    
    Returns `object` if already at max size (int64/uint64/float64).
    
    Parameters
    ----------
    current : str
        Current sized type name
        
    Returns
    -------
    str or Type
        Next type in ladder, or `object` if at max
    """
    if current in _INT_PROMOTION_LADDER:
        idx = _INT_PROMOTION_LADDER.index(current)
        if idx < len(_INT_PROMOTION_LADDER) - 1:
            return _INT_PROMOTION_LADDER[idx + 1]
        return object
    
    if current in _UINT_PROMOTION_LADDER:
        idx = _UINT_PROMOTION_LADDER.index(current)
        if idx < len(_UINT_PROMOTION_LADDER) - 1:
            return _UINT_PROMOTION_LADDER[idx + 1]
        return object
    
    if current in _FLOAT_PROMOTION_LADDER:
        idx = _FLOAT_PROMOTION_LADDER.index(current)
        if idx < len(_FLOAT_PROMOTION_LADDER) - 1:
            return _FLOAT_PROMOTION_LADDER[idx + 1]
        return object
    
    return object


def value_fits_in_type(value: Any, type_name: str) -> bool:
    """
    Check if a Python value fits in the range of a sized type.
    
    Parameters
    ----------
    value : Any
        Python scalar
    type_name : str
        Sized type name (e.g., 'int32', 'uint8')
        
    Returns
    -------
    bool
        True if value fits, False otherwise
    """
    if value is None:
        return True
    
    metadata = _TYPE_REGISTRY.get(type_name)
    if not metadata:
        return False
    
    category, bits, signed, min_val, max_val = metadata
    
    # Float types: check if numeric
    if category == 'float':
        return isinstance(value, (int, float, bool))
    
    # Integer types: check bounds
    if not isinstance(value, (int, bool)):
        return False
    
    return min_val <= value <= max_val


def get_type_name(kind: Union[Type, str]) -> str:
    """
    Get the display name for a type (sized or Python).
    
    Parameters
    ----------
    kind : Type or str
        Python type or sized type name
        
    Returns
    -------
    str
        Display name for the type
    """
    if isinstance(kind, str):
        return kind
    return kind.__name__


@dataclass(frozen=True)
class DataType:
    """
    Describes the semantic type of a Vector column.

    Attributes
    ----------
    kind : Type or str
        Python type (str, date, object) or sized type name ('int32', 'float64')
    nullable : bool
        Whether the column may contain None values

    Notes
    -----
    - DataType holds zero instance data (no masks, no defaults)
    - Promotion never mutates — always returns new DataType
    - This is backend-agnostic and forms the semantic core
    - Sized types ('int32', 'uint64', 'float32') enable array.array backend
    
    Examples
    --------
    >>> DataType(int)
    <int64>
    >>> DataType('int32')
    <int32>
    >>> DataType('int32', nullable=True)
    <int32?>
    >>> DataType(str)
    <str>
    """

    kind: Union[Type[Any], str]
    nullable: bool = False
    
    def __post_init__(self):
        """Normalize and validate type on construction."""
        normalized = normalize_type(self.kind)
        # Use object.__setattr__ since dataclass is frozen
        object.__setattr__(self, 'kind', normalized)

    def __repr__(self):
        if isinstance(self.kind, str):
            # Sized type: use string name
            kind_name = self.kind
        else:
            # Python type: use __name__
            kind_name = self.kind.__name__
        
        if self.nullable:
            return f"<{kind_name}?>"
        return f"<{kind_name}>"
    
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
        >>> dt = DataType('int32', nullable=True)
        >>> dt.with_nullable(False)
        <int32>
        """
        return DataType(self.kind, nullable=nullable)

    @property
    def is_numeric(self) -> bool:
        """True if kind is a numeric type (int/uint/float or Python numeric)."""
        if isinstance(self.kind, str):
            metadata = get_type_metadata(self.kind)
            return metadata is not None and metadata[0] in ('int', 'uint', 'float')
        
        try:
            return issubclass(self.kind, (int, float, complex, bool))
        except TypeError:
            return False
    
    @property
    def is_integer(self) -> bool:
        """True if kind is an integer type (signed/unsigned/bool, sized or Python)."""
        if isinstance(self.kind, str):
            metadata = get_type_metadata(self.kind)
            return metadata is not None and metadata[0] in ('int', 'uint')
        
        try:
            return issubclass(self.kind, (int, bool))
        except TypeError:
            return False
    
    @property
    def is_bool(self) -> bool:
        """True if kind is bool or uint8 (used for bool storage)."""
        if isinstance(self.kind, str):
            return self.kind == 'uint8'  # bool is stored as uint8
        
        try:
            return issubclass(self.kind, bool)
        except TypeError:
            return False

    @property
    def is_temporal(self) -> bool:
        """True if kind is date or datetime."""
        if isinstance(self.kind, str):
            return False
        
        try:
            return issubclass(self.kind, (date, datetime))
        except TypeError:
            return False
    
    @property
    def is_sized(self) -> bool:
        """True if this is a sized type (int32, uint64, float32, etc.)."""
        return isinstance(self.kind, str)

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

        # Case 2: Sized type validation
        if isinstance(self.kind, str):
            metadata = get_type_metadata(self.kind)
            if metadata:
                current_category = metadata[0]  # 'int', 'uint', or 'float'
                
                # Check type compatibility first
                if isinstance(value, bool):
                    # bool can go into uint8, or promote to int/float
                    if current_category in ('int', 'uint'):
                        if value_fits_in_type(value, self.kind):
                            return self
                        # Try next size
                        next_type = promote_to_next_size(self.kind)
                        if next_type is not object and isinstance(next_type, str):
                            if value_fits_in_type(value, next_type):
                                return DataType(next_type, self.nullable)
                    elif current_category == 'float':
                        # bool → float is fine
                        return self
                    
                elif isinstance(value, int):
                    # int value with sized int/uint type
                    if current_category in ('int', 'uint'):
                        if value_fits_in_type(value, self.kind):
                            return self
                        # Try promoting within same ladder
                        next_type = promote_to_next_size(self.kind)
                        if next_type is not object and isinstance(next_type, str):
                            if value_fits_in_type(value, next_type):
                                return DataType(next_type, self.nullable)
                        # Can't fit → degrade to object
                        warnings.warn(
                            f"Value {value!r} exceeds {self.kind} range, degrading to object",
                            stacklevel=3,
                        )
                        return DataType(object, self.nullable)
                    elif current_category == 'float':
                        # int → float is fine (implicit conversion)
                        return self
                
                elif isinstance(value, float):
                    # float value
                    if current_category in ('int', 'uint'):
                        # int/uint type + float value → promote to float64
                        return DataType('float64', self.nullable)
                    elif current_category == 'float':
                        # float type + float value → ok
                        return self
                
                # Incompatible type → degrade to object
                warnings.warn(
                    f"Value {value!r} exceeds {self.kind} range, degrading to object",
                    stacklevel=3,
                )
                return DataType(object, self.nullable)

        # Case 3: Exact match (Python types)
        if vtype is self.kind:
            return self

        # Case 4: Numeric ladder (bool → int → float → complex)
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

        # Case 5: Temporal ladder (date → datetime)
        if self.is_temporal and isinstance(value, (date, datetime)):
            if self.kind is datetime or vtype is datetime:
                new_kind = datetime
            else:
                new_kind = date
            
            if new_kind != self.kind:
                return DataType(new_kind, self.nullable)
            return self

        # Case 6: String/bytes stay as-is for same type
        if self.kind in (str, bytes) and vtype is self.kind:
            return self

        # Case 7: Degrade to object
        if self.kind is not object:
            warnings.warn(
                f"Degrading column<{self.kind if isinstance(self.kind, str) else self.kind.__name__}> to column<object> "
                f"due to incompatible value of type {vtype.__name__}",
                stacklevel=3,
            )
            return DataType(object, self.nullable)

        # Already object — trivial
        return self
    
    def promote_with_type(self, other: "DataType") -> "DataType":
        """
        Promote this DataType with another DataType (for binary operations).
        
        Parameters
        ----------
        other : DataType
            Other dtype to promote with
            
        Returns
        -------
        DataType
            Promoted dtype
        """
        # Handle nullable
        new_nullable = self.nullable or other.nullable
        
        # Both sized types: use numeric promotion
        if isinstance(self.kind, str) and isinstance(other.kind, str):
            promoted = promote_numeric_types(self.kind, other.kind)
            return DataType(promoted, nullable=new_nullable)
        
        # One sized, one Python type: normalize to sized if possible
        if isinstance(self.kind, str) and other.kind in _LEGACY_TYPE_MAP:
            other_sized = _LEGACY_TYPE_MAP[other.kind]
            promoted = promote_numeric_types(self.kind, other_sized)
            return DataType(promoted, nullable=new_nullable)
        
        if isinstance(other.kind, str) and self.kind in _LEGACY_TYPE_MAP:
            self_sized = _LEGACY_TYPE_MAP[self.kind]
            promoted = promote_numeric_types(self_sized, other.kind)
            return DataType(promoted, nullable=new_nullable)
        
        # Both Python types: use old logic
        if self.kind is object or other.kind is object:
            return DataType(object, nullable=new_nullable)
        
        # Numeric promotion (Python types)
        if self.is_numeric and other.is_numeric:
            if self.kind is complex or other.kind is complex:
                return DataType(complex, nullable=new_nullable)
            if self.kind is float or other.kind is float:
                return DataType(float, nullable=new_nullable)
            if self.kind is int or other.kind is int:
                return DataType(int, nullable=new_nullable)
            return DataType(bool, nullable=new_nullable)
        
        # Temporal promotion
        if self.is_temporal and other.is_temporal:
            if self.kind is datetime or other.kind is datetime:
                return DataType(datetime, nullable=new_nullable)
            return DataType(date, nullable=new_nullable)
        
        # Otherwise degrade to object
        return DataType(object, nullable=new_nullable)


def infer_kind(value: Any) -> Optional[Union[Type, str]]:
    """
    Infer type for a single scalar.
    
    Returns sized type for numeric values, defaulting to 64-bit precision.
    Returns Python type for non-numeric values.
    
    Returns None for None values.
    
    Defaults:
    - Integers → 'int64' (signed 64-bit)
    - Floats → 'float64' (double precision)
    - Booleans → 'uint8' (stored as 0/1)
    """
    if value is None:
        return None
    
    # Check bool BEFORE int (bool is subclass of int)
    if isinstance(value, bool):
        return 'uint8'  # Store bool as 0/1 in uint8
    
    if isinstance(value, int):
        # Default to 64-bit signed integer
        # Users can explicitly specify smaller types if needed
        return 'int64'
    
    if isinstance(value, float):
        # Default to double precision
        return 'float64'
    
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
                f"Cannot store None in non-nullable {dtype.kind if isinstance(dtype.kind, str) else dtype.kind.__name__} column"
            )
        return None

    # Sized type validation
    if isinstance(dtype.kind, str):
        if value_fits_in_type(value, dtype.kind):
            return value
        raise TypeError(
            f"Value {value!r} does not fit in {dtype.kind} range"
        )

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
    kind_name = dtype.kind if isinstance(dtype.kind, str) else dtype.kind.__name__
    raise TypeError(
        f"Incompatible value {value!r} for column<{kind_name}>"
    )




