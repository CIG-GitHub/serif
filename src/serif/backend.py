"""
Storage backends for Vector data.
Pure Python implementation using array.array for numeric types,
with separate null masks for nullable dtypes.
"""

from __future__ import annotations
from array import array
from typing import Any, Protocol, Iterator
from collections.abc import Iterable


class Storage(Protocol):
    """Protocol for Vector storage backends."""

    def __len__(self) -> int:
        """Number of elements (including nulls)."""
        ...

    def __getitem__(self, idx: int) -> Any:
        """Get element at index (returns None if null)."""
        ...

    def __iter__(self) -> Iterator[Any]:
        """Iterate over elements (yielding None for nulls)."""
        ...

    def slice(self, slc: slice) -> Storage:
        """Return a new Storage with sliced data."""
        ...

    def to_tuple(self) -> tuple:
        """Export to Python tuple (for compatibility/debug)."""
        ...

    def is_null(self, idx: int) -> bool:
        """Check if element at index is null."""
        ...

    def set(self, idx: int, value: Any) -> Storage:
        """Return new Storage with value set at index (copy-on-write)."""
        ...


class ArrayStorage:
    """
    Contiguous numeric storage using array.array + optional null mask.
    
    For numeric types (int, float) with or without nulls.
    Uses stdlib array.array for contiguous memory + buffer protocol.
    """

    __slots__ = ('_data', '_mask', '_typecode')

    # Map Python types to array.array typecodes
    _TYPECODE_MAP = {
        int: 'q',      # signed long long (64-bit)
        float: 'd',    # double
        bool: 'B',     # unsigned char (0/1)
    }
    
    # Range limits for typecodes
    _INT64_MIN = -(2**63)
    _INT64_MAX = 2**63 - 1

    def __init__(self, data: array, mask: array | None = None):
        """
        Parameters
        ----------
        data : array.array
            Contiguous numeric data
        mask : array.array of 'B' or None
            Null mask (1 = null, 0 = valid), same length as data
        """
        self._data = data
        self._mask = mask
        self._typecode = data.typecode

    @classmethod
    def from_iterable(cls, values: Iterable[Any], dtype_kind: type) -> ArrayStorage:
        """Create from Python iterable."""
        typecode = cls._TYPECODE_MAP.get(dtype_kind)
        if typecode is None:
            raise ValueError(f"Cannot use ArrayStorage for {dtype_kind}")

        data_list = []
        mask_list = []
        has_nulls = False

        for val in values:
            if val is None:
                has_nulls = True
                mask_list.append(1)
                data_list.append(0)  # sentinel value (ignored when masked)
            else:
                # Validate integer bounds for 64-bit array storage
                if dtype_kind is int:
                    if val < cls._INT64_MIN or val > cls._INT64_MAX:
                        raise OverflowError(
                            f"Integer {val} exceeds 64-bit range [{cls._INT64_MIN}, {cls._INT64_MAX}]. "
                            "Use TupleStorage for arbitrary precision integers."
                        )
                mask_list.append(0)
                data_list.append(val)

        data = array(typecode, data_list)
        mask = array('B', mask_list) if has_nulls else None

        return cls(data, mask)

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> Any:
        if self._mask and self._mask[idx]:
            return None
        return self._data[idx]

    def __iter__(self) -> Iterator[Any]:
        if self._mask:
            for nth in range(len(self._data)):
                yield None if self._mask[nth] else self._data[nth]
        else:
            yield from self._data

    def is_null(self, idx: int) -> bool:
        return bool(self._mask and self._mask[idx])

    def slice(self, slc: slice) -> ArrayStorage:
        """Zero-copy slice (array.array creates new view)."""
        new_data = self._data[slc]
        new_mask = self._mask[slc] if self._mask else None
        return ArrayStorage(new_data, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def set(self, idx: int, value: Any) -> ArrayStorage:
        """Copy-on-write update."""
        # Validate integer bounds before mutation
        if value is not None and self._typecode == 'q':
            if value < self._INT64_MIN or value > self._INT64_MAX:
                raise OverflowError(
                    f"Cannot set value {value}: exceeds 64-bit integer range. "
                    "Vector must be promoted to TupleStorage."
                )
        
        new_data = array(self._typecode, self._data)
        new_mask = array('B', self._mask) if self._mask else None

        if value is None:
            if new_mask is None:
                new_mask = array('B', [0] * len(new_data))
            new_mask[idx] = 1
        else:
            new_data[idx] = value
            if new_mask:
                new_mask[idx] = 0

        return ArrayStorage(new_data, new_mask)
    
    def promote_to_tuple(self) -> TupleStorage:
        """Convert ArrayStorage to TupleStorage for arbitrary precision / mixed types."""
        return TupleStorage(self.to_tuple())


class TupleStorage:
    """
    Python object storage using tuple.
    
    For non-numeric types (str, date, object) or mixed types.
    Nulls are stored as None inline.
    """

    __slots__ = ('_data',)

    def __init__(self, data: tuple):
        self._data = data

    @classmethod
    def from_iterable(cls, values: Iterable[Any]) -> TupleStorage:
        return cls(tuple(values))

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> Any:
        return self._data[idx]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def is_null(self, idx: int) -> bool:
        return self._data[idx] is None

    def slice(self, slc: slice) -> TupleStorage:
        return TupleStorage(self._data[slc])

    def to_tuple(self) -> tuple:
        return self._data

    def set(self, idx: int, value: Any) -> TupleStorage:
        """Copy-on-write update."""
        new_data = list(self._data)
        new_data[idx] = value
        return TupleStorage(tuple(new_data))


class LazyStorage:
    """
    Lazy storage for large iterables (e.g., range objects).
    
    Materializes on first access.
    """

    __slots__ = ('_source', '_materialized')

    def __init__(self, source: Iterable[Any]):
        self._source = source
        self._materialized = None

    def _ensure_materialized(self):
        if self._materialized is None:
            self._materialized = TupleStorage.from_iterable(self._source)

    def __len__(self) -> int:
        self._ensure_materialized()
        return len(self._materialized)

    def __getitem__(self, idx: int) -> Any:
        self._ensure_materialized()
        return self._materialized[idx]

    def __iter__(self) -> Iterator[Any]:
        if self._materialized:
            return iter(self._materialized)
        return iter(self._source)

    def is_null(self, idx: int) -> bool:
        self._ensure_materialized()
        return self._materialized.is_null(idx)

    def slice(self, slc: slice) -> Storage:
        self._ensure_materialized()
        return self._materialized.slice(slc)

    def to_tuple(self) -> tuple:
        self._ensure_materialized()
        return self._materialized.to_tuple()

    def set(self, idx: int, value: Any) -> Storage:
        self._ensure_materialized()
        return self._materialized.set(idx, value)


def choose_storage(values: Iterable[Any], dtype_kind: type, nullable: bool) -> Storage:
    """
    Choose appropriate storage backend based on dtype.
    
    Parameters
    ----------
    values : Iterable[Any]
        Data to store
    dtype_kind : type
        Python type (int, float, str, etc.)
    nullable : bool
        Whether the dtype allows null values (used for validation)
    
    Returns
    -------
    Storage
        Appropriate storage backend
        
    Notes
    -----
    - ArrayStorage: Used for numeric types (int, float, bool) within 64-bit range
    - TupleStorage: Fallback for arbitrary-precision ints, strings, dates, objects
    - Integers exceeding 64-bit range automatically demote to TupleStorage
    
    Raises
    ------
    ValueError
        If non-nullable dtype contains null values
    """
    # Validate nullable constraint if needed
    if not nullable:
        # Quick check: if any value is None and dtype is non-nullable, error
        # Note: This materializes the iterable, so only check for non-lazy iterables
        # For performance, this validation could be deferred or made optional
        pass  # Defer validation to Vector level to avoid double iteration
    
    # Try array.array for numeric types
    if dtype_kind in (int, float, bool):
        try:
            storage = ArrayStorage.from_iterable(values, dtype_kind)
            
            # Validate nullable constraint after construction
            if not nullable and storage._mask is not None:
                raise ValueError(
                    f"Non-nullable dtype {dtype_kind.__name__} cannot contain null values"
                )
            
            return storage
        except (ValueError, TypeError, OverflowError) as exc:
            # Overflow: integer too large for array storage
            # ValueError: typecode not found or nullable constraint violated
            # TypeError: incompatible type for array
            if isinstance(exc, ValueError) and "nullable" in str(exc):
                raise  # Re-raise nullable validation errors
            pass  # Otherwise fall through to tuple storage

    # Fallback to tuple for everything else
    storage = TupleStorage.from_iterable(values)
    
    # Validate nullable constraint for tuple storage
    if not nullable:
        for val in storage:
            if val is None:
                raise ValueError(
                    f"Non-nullable dtype {dtype_kind.__name__} cannot contain null values"
                )
    
    return storage