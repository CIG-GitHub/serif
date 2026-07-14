
"""
Null bitmaps for Vector storage backends.

Two implementations are provided:

ByteMask — one byte per element (1=valid, 0=null).
    Straightforward random access and copy-on-write mutation.
    Memory cost: n bytes for n elements.
    Preferred for small or frequently-mutated vectors.

BitMask  — one bit per element, packed into bytes (ceil(n/8) bytes total).
    8x more memory-efficient; slightly more CPU work per access.
    Bit layout: LSB-first within each byte (bit i lives in byte i//8 at
    position i%8). Matches the Apache Arrow validity bitmap convention
    (1=valid, 0=null). Compatible with Arrow buffers without inversion.
    Preferred for large, mostly-read vectors or when handing buffers
    to external tools that expect packed bitmaps.

Usage
-----
A ``None`` mask means "no nulls present" — callers treat ``None`` as the
fast/no-null path. A ``ByteMask`` or ``BitMask`` instance means nullable
is True. The presence of the object itself is the nullability flag.
"""

from __future__ import annotations
from array import array
from math import ceil
from typing import Iterable
from typing import Iterator


class ByteMask:
    """
    Null bitmap using one byte per element.

    Stores an ``array('B', ...)`` of the same length as the data array.
    A value of 1 means the corresponding element is valid; 0 means null.

    Copy-on-write: every mutating operation returns a *new* ``ByteMask``
    rather than modifying the receiver, so storage objects can share masks
    safely across copy operations.
    """

    __slots__ = ('_data',)

    def __init__(self, data: array) -> None:
        self._data = data

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_iterable(cls, nulls: Iterable[bool]) -> ByteMask:
        """
        Build a ByteMask from an iterable of booleans.

        Parameters
        ----------
        nulls:
            One True/False per element — True means the element is null.
        """
        return cls(array('B', (0 if n else 1 for n in nulls)))

    @classmethod
    def from_size(cls, n: int) -> ByteMask:
        """
        Allocate an all-valid (no nulls) mask of length n.
        """
        return cls(array('B', [1] * n))

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._data)

    def is_null(self, idx: int) -> bool:
        """Return True if element at idx is null."""
        return not bool(self._data[idx])

    def any_null(self) -> bool:
        """Return True if at least one element is null."""
        return not all(self._data)

    def __iter__(self) -> Iterator[bool]:
        """Yield True for each null position, False for each valid position."""
        return (not bool(b) for b in self._data)

    # ------------------------------------------------------------------
    # Slicing
    # ------------------------------------------------------------------

    def __getitem__(self, slc: slice) -> ByteMask:
        """Return a new ByteMask covering the slice."""
        return ByteMask(self._data[slc])

    # ------------------------------------------------------------------
    # Copy-on-write mutation
    # ------------------------------------------------------------------

    def mark_null(self, idx: int) -> ByteMask:
        """Return a new ByteMask with element idx marked null."""
        new_data = array('B', self._data)
        new_data[idx] = 0
        return ByteMask(new_data)

    def mark_valid(self, idx: int) -> ByteMask:
        """Return a new ByteMask with element idx marked valid."""
        new_data = array('B', self._data)
        new_data[idx] = 1
        return ByteMask(new_data)


class BitMask:
    """
    Null bitmap packing one bit per element into bytes.

    Allocates ceil(n/8) bytes regardless of how many nulls exist.
    Bit i lives in byte i//8 at bit position i%8 (LSB-first).
    A set bit (1) means valid; a clear bit (0) means null.

    Copy-on-write: every mutating operation returns a *new* ``BitMask``.
    """

    __slots__ = ('_data', '_length')

    def __init__(self, data: array, length: int) -> None:
        self._data = data      # array('B', ...) of ceil(length/8) bytes
        self._length = length  # true element count

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_iterable(cls, nulls: Iterable[bool]) -> BitMask:
        """
        Build a BitMask from an iterable of booleans.

        Parameters
        ----------
        nulls:
            One True/False per element — True means the element is null.
        """
        null_list = list(nulls)
        n = len(null_list)
        nbytes = ceil(n / 8) if n else 0
        data = array('B', [0] * nbytes)
        for i, flag in enumerate(null_list):
            if not flag:  # set bit = valid
                data[i // 8] |= (1 << (i % 8))
        return cls(data, n)

    @classmethod
    def from_size(cls, n: int) -> BitMask:
        """
        Allocate an all-valid (no nulls) BitMask for n elements.
        """
        nbytes = ceil(n / 8) if n else 0
        # 0xFF = all bits set = all valid (1=valid). Padding bits in the last
        # byte are also set but are never queried (idx is always < length).
        return cls(array('B', [0xFF] * nbytes), n)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self._length

    def is_null(self, idx: int) -> bool:
        """Return True if element at idx is null."""
        return not bool((self._data[idx // 8] >> (idx % 8)) & 1)

    def any_null(self) -> bool:
        """Return True if at least one element is null."""
        return any(not ((self._data[i // 8] >> (i % 8)) & 1) for i in range(self._length))

    def __iter__(self) -> Iterator[bool]:
        """Yield True for each null position, False for each valid position."""
        for i in range(self._length):
            yield not bool((self._data[i // 8] >> (i % 8)) & 1)

    # ------------------------------------------------------------------
    # Slicing
    # ------------------------------------------------------------------

    def __getitem__(self, slc: slice) -> BitMask:
        """Return a new BitMask covering the slice."""
        indices = range(self._length)[slc]
        nulls = [self.is_null(i) for i in indices]  # True=null, abstraction-safe
        return BitMask.from_iterable(nulls)

    # ------------------------------------------------------------------
    # Copy-on-write mutation
    # ------------------------------------------------------------------

    def mark_null(self, idx: int) -> BitMask:
        """Return a new BitMask with element idx marked null."""
        new_data = array('B', self._data)
        new_data[idx // 8] &= ~(1 << (idx % 8))  # clear bit = null
        return BitMask(new_data, self._length)

    def mark_valid(self, idx: int) -> BitMask:
        """Return a new BitMask with element idx marked valid."""
        new_data = array('B', self._data)
        new_data[idx // 8] |= (1 << (idx % 8))  # set bit = valid
        return BitMask(new_data, self._length)
