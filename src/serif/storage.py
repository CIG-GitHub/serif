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
    
    def __getitem__(self, i: int) -> Any:
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
    
    def is_null(self, i: int) -> bool:
        """Check if element at index is null."""
        ...
    
    def set(self, i: int, value: Any) -> Storage:
        """Return new Storage with value set at index (copy-on-write)."""
        ...


class ArrayStorage:
    """
    Contiguous numeric storage using array.array + optional null mask.
    
    For numeric types (int, uint, float) with or without nulls.
    Uses stdlib array.array for contiguous memory + buffer protocol.
    """
    
    __slots__ = ('_data', '_mask', '_typecode', '_dtype_name')
    
    # Map sized type names to array.array typecodes
    _TYPECODE_MAP = {
        'int8': 'b',
        'int16': 'h',
        'int32': 'i',
        'int64': 'q',
        'uint8': 'B',
        'uint16': 'H',
        'uint32': 'I',
        'uint64': 'Q',
        'float32': 'f',
        'float64': 'd',
    }
    
    # Range limits for integer types (typecode → (min, max))
    _INT_BOUNDS = {
        'b': (-(2**7), 2**7 - 1),
        'h': (-(2**15), 2**15 - 1),
        'i': (-(2**31), 2**31 - 1),
        'q': (-(2**63), 2**63 - 1),
        'B': (0, 2**8 - 1),
        'H': (0, 2**16 - 1),
        'I': (0, 2**32 - 1),
        'Q': (0, 2**64 - 1),
    }
    
    def __init__(self, data: array, mask: array | None = None, dtype_name: str = None):
        """
        Parameters
        ----------
        data : array.array
            Contiguous numeric data
        mask : array.array of 'B' or None
            Null mask (1 = null, 0 = valid), same length as data
        dtype_name : str, optional
            Sized type name (e.g., 'int32', 'float64')
        """
        self._data = data
        self._mask = mask
        self._typecode = data.typecode
        self._dtype_name = dtype_name
    
    @classmethod
    def from_iterable(cls, values: Iterable[Any], dtype_name: str) -> ArrayStorage:
        """Create from Python iterable using sized type name."""
        typecode = cls._TYPECODE_MAP.get(dtype_name)
        if typecode is None:
            raise ValueError(f"Cannot use ArrayStorage for dtype {dtype_name}")
        
        # Check bounds for integer types
        bounds = cls._INT_BOUNDS.get(typecode)
        
        data_list = []
        mask_list = []
        has_nulls = False
        
        for v in values:
            if v is None:
                has_nulls = True
                mask_list.append(1)
                data_list.append(0)  # sentinel value (ignored when masked)
            else:
                # Bounds check for integers
                if bounds is not None:
                    min_val, max_val = bounds
                    if not (min_val <= v <= max_val):
                        raise OverflowError(f"Value {v} exceeds {dtype_name} range [{min_val}, {max_val}]")
                
                mask_list.append(0)
                data_list.append(v)
        
        data = array(typecode, data_list)
        mask = array('B', mask_list) if has_nulls else None
        
        return cls(data, mask, dtype_name)
    
    def __len__(self) -> int:
        return len(self._data)
    
    def __getitem__(self, i: int) -> Any:
        if self._mask and self._mask[i]:
            return None
        return self._data[i]
    
    def __iter__(self) -> Iterator[Any]:
        if self._mask:
            for i in range(len(self._data)):
                yield None if self._mask[i] else self._data[i]
        else:
            yield from self._data
    
    def is_null(self, i: int) -> bool:
        return bool(self._mask and self._mask[i])
    
    def slice(self, slc: slice) -> ArrayStorage:
        """Zero-copy slice (array.array creates new view)."""
        new_data = self._data[slc]
        new_mask = self._mask[slc] if self._mask else None
        return ArrayStorage(new_data, new_mask)
    
    def to_tuple(self) -> tuple:
        return tuple(self)
    
    def set(self, i: int, value: Any) -> ArrayStorage:
        """Copy-on-write update."""
        new_data = array(self._typecode, self._data)
        new_mask = array('B', self._mask) if self._mask else None
        
        if value is None:
            if new_mask is None:
                new_mask = array('B', [0] * len(new_data))
            new_mask[i] = 1
        else:
            new_data[i] = value
            if new_mask:
                new_mask[i] = 0
        
        return ArrayStorage(new_data, new_mask)


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
    
    def __getitem__(self, i: int) -> Any:
        return self._data[i]
    
    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)
    
    def is_null(self, i: int) -> bool:
        return self._data[i] is None
    
    def slice(self, slc: slice) -> TupleStorage:
        return TupleStorage(self._data[slc])
    
    def to_tuple(self) -> tuple:
        return self._data
    
    def set(self, i: int, value: Any) -> TupleStorage:
        """Copy-on-write update."""
        new_data = list(self._data)
        new_data[i] = value
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
    
    def __getitem__(self, i: int) -> Any:
        self._ensure_materialized()
        return self._materialized[i]
    
    def __iter__(self) -> Iterator[Any]:
        if self._materialized:
            return iter(self._materialized)
        return iter(self._source)
    
    def is_null(self, i: int) -> bool:
        self._ensure_materialized()
        return self._materialized.is_null(i)
    
    def slice(self, slc: slice) -> Storage:
        self._ensure_materialized()
        return self._materialized.slice(slc)
    
    def to_tuple(self) -> tuple:
        self._ensure_materialized()
        return self._materialized.to_tuple()
    
    def set(self, i: int, value: Any) -> Storage:
        self._ensure_materialized()
        return self._materialized.set(i, value)


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
        Whether nulls are present
    
    Returns
    -------
    Storage
        Appropriate storage backend
    """
    # Try array.array for numeric types
    if dtype_kind in (int, float, bool):
        try:
            return ArrayStorage.from_iterable(values, dtype_kind)
        except (ValueError, TypeError):
            pass
    
    # Fallback to tuple for everything else
    return TupleStorage.from_iterable(values)
