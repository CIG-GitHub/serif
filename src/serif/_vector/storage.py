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


# ---------------------------------------------------------------------------
# StringStorage — Arrow-style contiguous UTF-8 string buffer
# ---------------------------------------------------------------------------

class StringStorage:
    """
    Contiguous UTF-8 string storage: one bytes buffer + uint32 offsets array.

    Layout
    ------
    _buf:     bytes          — all string data concatenated (UTF-8)
    _offsets: array('I')    — length n+1; string i lives at buf[off[i]:off[i+1]]
    _mask:    ByteMask|None — null flags (1=valid, 0=null per new convention)

    Accessing string i:  buf[offsets[i]:offsets[i+1]].decode('utf-8')

    Benefits over TupleStorage for string columns
    ---------------------------------------------
    - Construction: one b''.join() pass instead of N .encode() calls living as
      separate heap objects.
    - Memory: ~4 bytes/string (offset) + raw UTF-8 bytes vs ~57 bytes/string
      Python str object overhead.
    - Parquet integration: raw BYTE_ARRAY bytes slot directly into _buf;
      decode is deferred until a value is actually accessed.
    - Multi-byte / emoji / non-ASCII: fully handled — offsets are byte positions,
      bytes.decode('utf-8') handles all of UTF-8 correctly.

    Limit: total buffer size must fit in uint32 (~4 GB per column).
    """

    __slots__ = ('_buf', '_offsets', '_mask')

    def __init__(self, buf: bytes, offsets: array, mask: ByteMask | None = None):
        self._buf     = buf
        self._offsets = offsets  # array('I'), length = n+1
        self._mask    = mask

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_iterable(cls, values: Iterable[Any]) -> StringStorage:
        """
        Build from any iterable of str | None values.
        One pass: encodes and concatenates all strings, builds offset array.
        """
        buf_parts:  list[bytes] = []
        offsets:    list[int]   = [0]
        null_flags: list[bool]  = []
        has_nulls = False

        for val in values:
            if val is None:
                has_nulls = True
                null_flags.append(True)
                offsets.append(offsets[-1])     # zero-length sentinel for null
            else:
                encoded = val.encode('utf-8')
                buf_parts.append(encoded)
                null_flags.append(False)
                offsets.append(offsets[-1] + len(encoded))

        buf  = b''.join(buf_parts)
        arr  = array('I', offsets)
        mask = ByteMask.from_iterable(null_flags) if has_nulls else None
        return cls(buf, arr, mask)

    @classmethod
    def from_raw(cls, buf: bytes, offsets: array,
                 mask: ByteMask | None = None) -> StringStorage:
        """
        Wrap pre-built components with zero copying.
        Used by the Parquet reader which builds _buf directly from page bytes.
        offsets must already be an array('I') of length n+1.
        """
        return cls(buf, offsets, mask)

    # ------------------------------------------------------------------
    # Core interface (matches ArrayStorage / TupleStorage)
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._offsets) - 1

    def __bool__(self) -> bool:
        return len(self._offsets) > 1

    def __getitem__(self, idx: int) -> Any:
        if self._mask is not None and self._mask.is_null(idx):
            return None
        return self._buf[self._offsets[idx]:self._offsets[idx + 1]].decode('utf-8')

    def __iter__(self) -> Iterator[Any]:
        buf     = self._buf
        offsets = self._offsets
        mask    = self._mask
        for i in range(len(offsets) - 1):
            if mask is not None and mask.is_null(i):
                yield None
            else:
                yield buf[offsets[i]:offsets[i + 1]].decode('utf-8')

    def is_null(self, idx: int) -> bool:
        return self._mask is not None and self._mask.is_null(idx)

    def slice(self, slc: slice) -> StringStorage:
        """Return a new StringStorage covering the slice."""
        n       = len(self)
        indices = range(n)[slc]

        if not indices:
            return StringStorage(b'', array('I', [0]), None)

        buf_parts:  list[bytes] = []
        new_offs:   list[int]   = [0]
        null_flags: list[bool]  = []
        has_nulls = False

        buf     = self._buf
        offsets = self._offsets
        mask    = self._mask

        for i in indices:
            is_null = mask is not None and mask.is_null(i)
            null_flags.append(is_null)
            if is_null:
                has_nulls = True
                new_offs.append(new_offs[-1])   # zero advance
            else:
                chunk = buf[offsets[i]:offsets[i + 1]]
                buf_parts.append(chunk)
                new_offs.append(new_offs[-1] + len(chunk))

        new_buf  = b''.join(buf_parts)
        new_arr  = array('I', new_offs)
        new_mask = ByteMask.from_iterable(null_flags) if has_nulls else None
        return StringStorage(new_buf, new_arr, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def set(self, idx: int, value: Any) -> StringStorage:
        """Copy-on-write point mutation.  O(n) — string mutation is rare."""
        lst      = list(self)       # decodes all strings
        lst[idx] = value
        return StringStorage.from_iterable(lst)
