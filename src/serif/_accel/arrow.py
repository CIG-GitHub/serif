"""
OPTIONAL pyarrow bridge — zero-copy wrap/unwrap between storage buffers
and arrow arrays.

serif stays zero-dependency: this module only activates when pyarrow
happens to be installed, and nothing anywhere requires it. It is the
second opportunistic backend (the package docstring holds the shared
doctrine), and it exists for the work numpy structurally cannot
accelerate. numpy's model is fixed-width lanes — variable-width UTF-8
is invisible to it, which is why string GATHERING is numpy-accelerated
(pure offset arithmetic, mask.py) while string CONTENT still walks a
per-element decode loop. Arrow's compute kernels are the vectorized
engine for that content, and serif's storage layout was built
arrow-shaped so they run on serif's buffers directly:

  * StringStorage (validity BitMask, uint32 offsets, UTF-8 buffer) IS
    the arrow StringArray layout: pa.Array.from_buffers over py_buffer
    views wraps it with ZERO copies — the inverse of the parquet
    reader's memcpys (io/_arrow.py), minus even the memcpy.

  * BitMask IS the arrow validity bitmap (packed LSB-first, 1=valid),
    byte for byte, in both directions.

  * One asymmetry: arrow string offsets are SIGNED int32; StringStorage
    offsets are uint32 (buffers up to 4 GiB). A buffer of 2 GiB or more
    would read back as negative offsets, so the wrap DECLINES at 2**31.
    Offsets are monotonic, so one comparison on the last one decides.

Coming back, arrow's bit-packed bool results become BoolStorage's 0/1
bytes: one unpack pass — a single numpy C call when numpy is present,
the reader's bit loop when not. When BoolStorage goes bit-packed (the
0.1.7 bitpacking work), bool_storage() collapses to the same memcpy
bitmask() already is; the seam is deliberately that one function.

Buffer lifetime and mutation: py_buffer holds a reference to the object
it wraps, so a returned arrow array keeps serif's buffers alive on its
own. The storage protocol is read-only (storage.py), and batch() writes
land only on private_copy()'d buffers, so a wrapped buffer can never
change underneath a live arrow array.

_USE_ARROW is a private switch for tests/benchmarks, not API.
"""

from __future__ import annotations

import operator as _op

try:
    import pyarrow as _pa
    import pyarrow.compute as _pc
except ImportError:            # pyarrow not installed — every call declines
    _pa = None
    _pc = None

from . import _np
from .._vector.nullable import BitMask
from .._vector.storage import BoolStorage, StringStorage

_USE_ARROW = _pa is not None

_I32_MAX = 2**31 - 1

_CMP_KERNELS = {
    _op.eq: 'equal',
    _op.ne: 'not_equal',
    _op.lt: 'less',
    _op.le: 'less_equal',
    _op.gt: 'greater',
    _op.ge: 'greater_equal',
}


def string_array(storage):
    """StringStorage → pa.StringArray over the SAME buffers; None to DECLINE.

    Declines: pyarrow absent (or switched off), not a StringStorage, empty
    (nothing to accelerate — the pure path is trivial), or a data buffer
    too large for arrow's signed int32 offsets.
    """
    if not _USE_ARROW or not isinstance(storage, StringStorage):
        return None
    n = len(storage)
    if n == 0:
        return None
    offsets = storage._offsets
    if offsets[-1] > _I32_MAX:
        return None
    mask = storage._mask
    validity = _pa.py_buffer(mask._buf) if mask is not None else None
    return _pa.Array.from_buffers(
        _pa.string(), n,
        [validity, _pa.py_buffer(offsets), _pa.py_buffer(storage._buf)],
        -1 if mask is not None else 0)   # -1: arrow counts nulls lazily


def bitmask(arr):
    """Arrow validity bitmap → BitMask; None when the array has no nulls.

    The same near-zero-copy trim as the reader's _bit_mask (io/_arrow.py):
    identical layouts, one memcpy, no bit twiddling. Callers guarantee
    offset == 0 (bool_storage declines non-zero offsets before calling).
    """
    if arr.null_count == 0:
        return None
    n   = len(arr)
    buf = bytearray(memoryview(arr.buffers()[0])[:(n + 7) // 8])
    return BitMask(buf, n)


def compare_strings(storage, rhs, op_func):
    """Comparison on string buffers → BoolStorage, or None to DECLINE.

    StringStorage vs str scalar only (vector-vector is the next step).
    This sits in the BIT-IDENTICAL tier (ops.py's contract): UTF-8 byte
    order IS codepoint order, so arrow's bytewise compare and Python's
    str compare agree on all six operators — equality because UTF-8 is
    injective, ordering because UTF-8 sorts bytewise in codepoint order.

    The rhs guard is exact (`type(rhs) is str`): a str SUBCLASS may
    override comparison, and the pure path would honor it — subclasses
    decline. None rhs declines too (the pure path yields all-null, with
    the warning already emitted upstream). Null lanes never compare:
    arrow propagates input validity straight to the result, exactly the
    pure path's `None if x is None` — no sentinel ever leaks.
    """
    if type(rhs) is not str:
        return None
    kernel = _CMP_KERNELS.get(op_func)
    if kernel is None:
        return None
    arr = string_array(storage)
    if arr is None:
        return None
    return bool_storage(getattr(_pc, kernel)(arr, rhs))


def bool_storage(arr):
    """pa.BooleanArray (a kernel result) → BoolStorage; None to DECLINE.

    Declines on a non-zero offset: kernel outputs are freshly allocated
    at offset 0; anything else (a slice) would misalign every bit against
    the bytewise trims here and in bitmask().

    Arrow's values are bit-packed, BoolStorage's are 0/1 bytes — one
    unpack pass (numpy C call, or the reader's bit loop without numpy).
    Bits under null lanes pass through as-is: masked-lane bytes are
    unobservable garbage, the same contract the pure path's 0-sentinels
    rely on (ops.py).
    """
    if arr.offset != 0:
        return None
    n = len(arr)
    if n == 0:
        data = bytearray()
    else:
        bit_buf = arr.buffers()[1]
        if _np is not None:
            bits = _np.frombuffer(bit_buf, dtype=_np.uint8)
            data = bytearray(
                _np.unpackbits(bits, count=n, bitorder='little').tobytes())
        else:
            mv   = memoryview(bit_buf)
            data = bytearray(n)
            for i in range(n):
                data[i] = (mv[i >> 3] >> (i & 7)) & 1
    return BoolStorage.from_raw(data, bitmask(arr))
