
"""
Null bitmap for Vector storage backends.

BitMask — one BIT per element, packed 8-to-a-byte in the Apache Arrow
    validity-bitmap layout: bit i lives at byte ``i >> 3``, bit position
    ``i & 7`` (least-significant-bit first within each byte). A set bit (1)
    means the element is VALID; a clear bit (0) means NULL. The buffer is
    ceil(n / 8) bytes; the element count n is tracked separately because the
    final byte's high bits are padding that carries no element.

    Memory cost: n / 8 bytes for n elements — an eighth of a byte-per-element
    mask. And because the layout is byte-for-byte identical to Arrow's
    validity bitmap, a pyarrow validity buffer drops straight in (and serif's
    buffer hands straight back out) with no unpacking or inversion. That
    zero-copy interop is the whole reason for packing at the bit level.

Usage
-----
A ``None`` mask means "no nulls present" — callers treat ``None`` as the
fast/no-null path. A ``BitMask`` instance means nullable is True. The
presence of the object itself is the nullability flag.

Convention note: 1=valid / 0=null matches Arrow so bitmaps interoperate
without inversion. ``from_iterable`` still takes NULL flags (True=null) to
match the storage backends that build it.
"""

from __future__ import annotations
from typing import Iterable
from typing import Iterator


class BitMask:
    """
    Null bitmap using one packed bit per element (Arrow validity layout).

    Stores a ``bytearray`` of ceil(n/8) bytes plus the element count n.
    Element i is valid iff ``_buf[i >> 3] >> (i & 7) & 1``; 1=valid, 0=null.

    Copy-on-write: every mutating operation returns a *new* ``BitMask``
    rather than modifying the receiver, so storage objects can share masks
    safely across copy operations.
    """

    __slots__ = ('_buf', '_length')

    def __init__(self, buf: bytearray, length: int) -> None:
        self._buf    = buf
        self._length = length

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
        flags = nulls if isinstance(nulls, (list, tuple)) else list(nulls)
        n   = len(flags)
        buf = bytearray((n + 7) // 8)          # zero-filled → all bits start null
        for i, is_null in enumerate(flags):
            if not is_null:
                buf[i >> 3] |= 1 << (i & 7)     # valid → set the bit
        return cls(buf, n)

    @classmethod
    def from_size(cls, n: int) -> BitMask:
        """Allocate an all-valid (no nulls) mask of length n."""
        return cls(bytearray(b'\xff' * ((n + 7) // 8)), n)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self._length

    def is_null(self, idx: int) -> bool:
        """Return True if element at idx is null.

        Negative indices count from the end, and out-of-range indices raise
        IndexError — callers (e.g. ArrayStorage.__getitem__) rely on both, as
        the byte-per-element predecessor gave them for free via array
        indexing. The packed buffer is ceil(n/8) bytes, so it cannot lean on
        the buffer to raise: the bound is checked against the element count.
        """
        if idx < 0:
            idx += self._length
        if not 0 <= idx < self._length:
            raise IndexError('mask index out of range')
        return not (self._buf[idx >> 3] >> (idx & 7)) & 1

    def any_null(self) -> bool:
        """Return True if at least one element is null."""
        return any(self.is_null(i) for i in range(self._length))

    def __iter__(self) -> Iterator[bool]:
        """Yield True for each null position, False for each valid position."""
        buf = self._buf
        for i in range(self._length):
            yield not (buf[i >> 3] >> (i & 7)) & 1

    # ------------------------------------------------------------------
    # Slicing
    # ------------------------------------------------------------------

    def __getitem__(self, slc: slice) -> BitMask:
        """Return a new BitMask covering the slice.

        Bits are repacked by element index: a byte-level slice of the packed
        buffer would misalign every element whose bit position shifts, so the
        slice is rebuilt one element at a time.
        """
        indices = range(*slc.indices(self._length))
        return BitMask.from_iterable(self.is_null(i) for i in indices)

    # ------------------------------------------------------------------
    # Copy-on-write mutation
    # ------------------------------------------------------------------

    def mark_null(self, idx: int) -> BitMask:
        """Return a new BitMask with element idx marked null."""
        if idx < 0:
            idx += self._length
        if not 0 <= idx < self._length:
            raise IndexError('mask index out of range')
        new_buf = bytearray(self._buf)
        new_buf[idx >> 3] &= ~(1 << (idx & 7)) & 0xFF
        return BitMask(new_buf, self._length)

    def mark_valid(self, idx: int) -> BitMask:
        """Return a new BitMask with element idx marked valid."""
        if idx < 0:
            idx += self._length
        if not 0 <= idx < self._length:
            raise IndexError('mask index out of range')
        new_buf = bytearray(self._buf)
        new_buf[idx >> 3] |= 1 << (idx & 7)
        return BitMask(new_buf, self._length)
