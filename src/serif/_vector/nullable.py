
"""
Null bitmap for Vector storage backends.

ByteMask — one byte per element (1=valid, 0=null).
    Straightforward random access and copy-on-write mutation.
    Memory cost: n bytes for n elements.

Usage
-----
A ``None`` mask means "no nulls present" — callers treat ``None`` as the
fast/no-null path. A ``ByteMask`` instance means nullable is True. The
presence of the object itself is the nullability flag.

(A packed 1-bit-per-element BitMask matching the Apache Arrow validity
convention existed here previously; it was removed as dead code — resurrect
from git history if Arrow buffer interop ever lands.)
"""

from __future__ import annotations
from array import array
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
