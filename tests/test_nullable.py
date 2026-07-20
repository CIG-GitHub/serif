"""
BitMask — the bit-packed null bitmap (Apache Arrow validity layout).

The layout is a contract, not an implementation detail: bit i lives at byte
i >> 3, position i & 7 (LSB-first), 1=valid / 0=null. pyarrow interop depends
on it being byte-for-byte identical to Arrow's validity bitmap, so the exact
packed bytes are asserted here — not just the observable is_null behavior.

BitMask is exercised through the Vector API elsewhere (test_storage_protocol,
test_null_semantics); this file pins the primitive directly.
"""

import pytest

from serif import Vector
from serif._vector.nullable import BitMask


# ---------------------------------------------------------------------------
# Arrow layout contract — exact packed bytes
# ---------------------------------------------------------------------------

def test_from_iterable_packs_lsb_first_one_is_valid():
    # elements: valid, null, valid → bits 1,0,1 → byte 0b00000101 == 5
    m = BitMask.from_iterable([False, True, False])
    assert bytes(m._buf) == bytes([0b00000101])
    assert len(m) == 3


def test_multi_byte_layout():
    # 10 elements, null at index 0 and 9, valid elsewhere.
    nulls = [i in (0, 9) for i in range(10)]
    m = BitMask.from_iterable(nulls)
    # byte 0: bit0=0 (null), bits1..7=1  -> 0b11111110 == 0xFE
    # byte 1: bit0(elem8)=1, bit1(elem9)=0, padding 0 -> 0b00000001 == 0x01
    assert bytes(m._buf) == bytes([0xFE, 0x01])
    assert len(m) == 10


def test_buffer_length_is_ceil_n_over_8():
    assert len(BitMask.from_iterable([False] * 8)._buf) == 1
    assert len(BitMask.from_iterable([False] * 9)._buf) == 2
    assert len(BitMask.from_iterable([False] * 17)._buf) == 3


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def test_is_null_and_iter():
    nulls = [False, True, False, True, True, False, False, True, False]
    m = BitMask.from_iterable(nulls)
    assert [m.is_null(i) for i in range(len(m))] == nulls
    assert list(m) == nulls          # __iter__ yields True at null positions


def test_is_null_negative_index():
    # Negative indices count from the end. The byte-per-element predecessor
    # got this for free via Python array indexing; the packed version must
    # normalize, or -1 reads a padding bit of the last byte instead.
    m = BitMask.from_iterable([False, True, False])   # last element is valid
    assert m.is_null(-1) is False
    assert m.is_null(-2) is True
    assert m.is_null(-3) is False


def test_is_null_out_of_range_raises():
    # The packed buffer is ceil(n/8) bytes, so an out-of-range index lands on
    # a padding bit rather than overrunning — is_null must bounds-check so
    # ArrayStorage.__getitem__ still raises IndexError past the end.
    m = BitMask.from_iterable([False, True, False])
    with pytest.raises(IndexError):
        m.is_null(3)
    with pytest.raises(IndexError):
        m.is_null(-4)


def test_any_null():
    assert BitMask.from_iterable([False, False, False]).any_null() is False
    assert BitMask.from_iterable([False, True, False]).any_null() is True


def test_from_size_is_all_valid():
    m = BitMask.from_size(5)
    assert len(m) == 5
    assert m.any_null() is False
    assert all(not m.is_null(i) for i in range(5))


# ---------------------------------------------------------------------------
# Slicing — bits repacked by element index (not a byte slice)
# ---------------------------------------------------------------------------

def test_slice_byte_aligned():
    nulls = [False, True, False, True, True, False, False, True]
    m = BitMask.from_iterable(nulls)[0:4]
    assert list(m) == [False, True, False, True]
    assert len(m) == 4


def test_slice_non_aligned_offset():
    # Slicing from index 1 shifts every bit — a raw byte slice would misread.
    nulls = [False, True, False, True, True, False, False, True]
    m = BitMask.from_iterable(nulls)[1:6]
    assert list(m) == [True, False, True, True, False]


def test_slice_with_step():
    nulls = [False, True, False, True, True, False, False, True]
    m = BitMask.from_iterable(nulls)[::2]
    assert list(m) == [False, False, True, False]


# ---------------------------------------------------------------------------
# Copy-on-write mutation
# ---------------------------------------------------------------------------

def test_mark_null_is_copy_on_write():
    original = BitMask.from_size(4)
    marked = original.mark_null(2)
    assert marked.is_null(2) is True
    assert original.is_null(2) is False       # receiver untouched
    assert marked._buf is not original._buf


def test_mark_valid_is_copy_on_write():
    original = BitMask.from_iterable([True, True, True])
    marked = original.mark_valid(1)
    assert marked.is_null(1) is False
    assert original.is_null(1) is True
    assert marked._buf is not original._buf


# ---------------------------------------------------------------------------
# Integration — a nullable Vector rides on a BitMask; is_na() reads it
# ---------------------------------------------------------------------------

def test_vector_is_na_reads_bitmask():
    v = Vector([1, None, 3, None, 5])
    assert isinstance(v._storage._mask, BitMask)
    assert list(v.is_na()) == [False, True, False, True, False]
