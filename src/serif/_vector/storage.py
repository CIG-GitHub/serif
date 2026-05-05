"""
Storage backends for Vector data.
"""

from __future__ import annotations
from array import array
from typing import Any
from typing import Iterator
from collections.abc import Iterable
from .nullable import ByteMask


class ArrayStorage:
    """
    Contiguous numeric storage using array.array.

    None cannot live in array.array, so nulls are tracked with a separate
    byte mask (1=null, 0=valid). mask=None means no nulls present.
    Let array.array raise on bad typecodes or overflow — not duplicated here.
    """

    __slots__ = ('_data', '_mask')

    def __init__(self, data: array, mask: ByteMask | None = None):
        self._data = data
        self._mask = mask

    @classmethod
    def from_iterable(cls, values: Iterable[Any], typecode: str, nullable: bool) -> ArrayStorage:
        data_list = []
        null_flags = []
        has_nulls = False
        for val in values:
            if val is None:
                has_nulls = True
                null_flags.append(True)
                data_list.append(0)  # sentinel — position is masked
            else:
                null_flags.append(False)
                data_list.append(val)

        data = array(typecode, data_list)  # raises TypeError/OverflowError on bad values
        mask = ByteMask.from_iterable(null_flags) if has_nulls else None
        return cls(data, mask)

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> Any:
        if self._mask is not None and self._mask.is_null(idx):
            return None
        return self._data[idx]

    def __iter__(self) -> Iterator[Any]:
        if self._mask is not None:
            for i in range(len(self._data)):
                yield None if self._mask.is_null(i) else self._data[i]
        else:
            yield from self._data

    def is_null(self, idx: int) -> bool:
        return self._mask is not None and self._mask.is_null(idx)

    def slice(self, slc: slice) -> ArrayStorage:
        new_data = self._data[slc]
        new_mask = self._mask[slc] if self._mask is not None else None
        return ArrayStorage(new_data, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def set(self, idx: int, value: Any) -> ArrayStorage:
        """Copy-on-write. Let array.array raise on overflow."""
        new_data = array(self._data.typecode, self._data)

        if value is None:
            mask = self._mask if self._mask is not None else ByteMask.from_size(len(new_data))
            new_mask = mask.mark_null(idx)
        else:
            new_data[idx] = value
            new_mask = self._mask.mark_valid(idx) if self._mask is not None else None

        return ArrayStorage(new_data, new_mask)


class TupleStorage:
    """
    General-purpose storage using a Python tuple.

    None is stored inline — tuples hold anything, no sentinel or mask needed.
    """

    __slots__ = ('_data',)

    def __init__(self, data: tuple):
        self._data = data

    @classmethod
    def from_iterable(cls, values: Iterable[Any], nullable: bool = False) -> TupleStorage:
        if isinstance(values, tuple):
            return cls(values)
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

    def __bool__(self) -> bool:
        return len(self._data) > 0

    def to_tuple(self) -> tuple:
        return self._data

    def set(self, idx: int, value: Any) -> TupleStorage:
        lst = list(self._data)
        lst[idx] = value
        return TupleStorage(tuple(lst))
