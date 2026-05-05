"""
Storage backends for Vector data.
"""

from __future__ import annotations
from array import array
from typing import Any
from typing import Iterator
from collections.abc import Iterable


class ArrayStorage:
    """
    Contiguous numeric storage using array.array.

    None cannot live in array.array, so nulls are tracked with a separate
    byte mask (1=null, 0=valid). mask=None means no nulls present.
    Let array.array raise on bad typecodes or overflow — not duplicated here.
    """

    __slots__ = ('_data', '_mask')

    def __init__(self, data: array, mask: array | None = None):
        self._data = data
        self._mask = mask

    @classmethod
    def from_iterable(cls, values: Iterable[Any], typecode: str, nullable: bool) -> ArrayStorage:
        data_list = []
        mask_list = []
        has_nulls = False

        for val in values:
            if val is None:
                has_nulls = True
                mask_list.append(1)
                data_list.append(0)  # sentinel — position is masked
            else:
                mask_list.append(0)
                data_list.append(val)

        data = array(typecode, data_list)  # raises TypeError/OverflowError on bad values
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
            for i in range(len(self._data)):
                yield None if self._mask[i] else self._data[i]
        else:
            yield from self._data

    def is_null(self, idx: int) -> bool:
        return bool(self._mask and self._mask[idx])

    def slice(self, slc: slice) -> ArrayStorage:
        new_data = self._data[slc]
        new_mask = self._mask[slc] if self._mask else None
        return ArrayStorage(new_data, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def set(self, idx: int, value: Any) -> ArrayStorage:
        """Copy-on-write. Let array.array raise on overflow."""
        new_data = array(self._data.typecode, self._data)
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
