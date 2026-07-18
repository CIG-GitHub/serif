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

Second job, same doctrine: CHECKED integer arithmetic (binop_ints).
Not because numpy can't do int64 math — because it can't do it SAFELY
without predicting: np.int64 wraps silently, so the numpy tier runs a
bounds pass and declines whenever overflow is POSSIBLE. Arrow's
*_checked kernels detect ACTUAL overflow instead, so the decline is
exact, null lanes never compute, and the bounds pass's over-declines
get a second chance before falling to pure.

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

import array as _pyarray

from . import _np
from .._vector.nullable import BitMask
from .._vector.storage import ArrayStorage, BoolStorage, StringStorage

_USE_ARROW = _pa is not None

_I32_MAX = 2**31 - 1
_I64_MAX = 2**63 - 1
_I64_MIN = -2**63

_ARITH_KERNELS = {
    _op.add: 'add_checked',
    _op.sub: 'subtract_checked',
    _op.mul: 'multiply_checked',
}

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


def int64_array(storage):
    """ArrayStorage('q') → pa.Int64Array over the SAME buffers; None to
    DECLINE (pyarrow absent/off, wrong storage or typecode, empty).
    array('q') is 8-byte little-endian lanes and BitMask is the validity
    bitmap — the wrap is two py_buffer views, zero copies."""
    if not _USE_ARROW or not isinstance(storage, ArrayStorage):
        return None
    if storage._data.typecode != 'q':
        return None
    n = len(storage)
    if n == 0:
        return None
    mask = storage._mask
    validity = _pa.py_buffer(mask._buf) if mask is not None else None
    return _pa.Array.from_buffers(
        _pa.int64(), n,
        [validity, _pa.py_buffer(storage._data)],
        -1 if mask is not None else 0)   # -1: arrow counts nulls lazily


def int64_storage(arr):
    """pa.Int64Array (a kernel result) → ArrayStorage; None to DECLINE
    (non-zero offset — same rationale as bool_storage). One memcpy for
    the lanes, one for the validity trim. Lanes under null are whatever
    the kernel left there: unobservable garbage, the pure path's
    0-sentinel contract (ops.py)."""
    if arr.offset != 0:
        return None
    n = len(arr)
    data = _pyarray.array('q')
    if n:
        data.frombytes(memoryview(arr.buffers()[1])[:n * 8])
    return ArrayStorage(data, bitmask(arr))


def binop_ints(lhs_storage, rhs, op_func, result_kind):
    """Checked int64 add/sub/mul on buffers → ArrayStorage, or None to
    DECLINE.

    The numpy tier must PREDICT overflow: a bounds pass over operand
    extremes declines whenever some cross-lane combination COULD leave
    int64 — even if no actual lane pair does (ops.py). Arrow's *_checked
    kernels DETECT it instead: compute once, raise on the first lane
    that actually overflows. So this tier declines exactly when the
    pure path would promote, and rescues the vector-vector cases the
    bounds pass gives up on. (Scalar rhs bounds are exact — a scalar's
    min IS its max — so numpy never over-declines those; scalar rescue
    only matters when numpy is absent.)

    Null lanes never compute in arrow, so a huge value under the other
    side's null cannot overflow — same as the pure path, and one more
    over-decline the bounds pass (which sees sentinel zeros and null-
    lane values alike) cannot avoid. Result validity is arrow's
    valid_a & valid_b, the pure path's null-propagation exactly.

    floordiv/mod are ABSENT from the kernel map by doctrine, not
    omission: arrow's integer division truncates toward zero, Python
    floors — semantics, not transport. The division family is division's
    commit; this one is pure add/sub/mul.
    """
    if result_kind is not int:
        return None
    kernel = _ARITH_KERNELS.get(op_func)
    if kernel is None:
        return None
    arr = int64_array(lhs_storage)
    if arr is None:
        return None
    if isinstance(rhs, ArrayStorage):
        other = int64_array(rhs)
        if other is None:
            return None
    elif type(rhs) is int and _I64_MIN <= rhs <= _I64_MAX:
        # bool is an int subclass — `type is int` keeps it out, like
        # ops.py. Out-of-range scalars decline: the pure path promotes.
        other = _pa.scalar(rhs, type=_pa.int64())
    else:
        return None
    try:
        out = getattr(_pc, kernel)(arr, other)
    except _pa.ArrowInvalid:
        return None   # actual overflow — the pure path promotes past int64
    return int64_storage(out)


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

    StringStorage vs str scalar, or StringStorage vs StringStorage (the
    caller has already length-checked). This sits in the BIT-IDENTICAL
    tier (ops.py's contract): UTF-8 byte order IS codepoint order, so
    arrow's bytewise compare and Python's str compare agree on all six
    operators — equality because UTF-8 is injective, ordering because
    UTF-8 sorts bytewise in codepoint order.

    The scalar guard is exact (`type(rhs) is str`): a str SUBCLASS may
    override comparison, and the pure path would honor it — subclasses
    decline. (StringStorage needs no such guard: it never holds anything
    but real str.) None rhs declines too (the pure path yields all-null,
    with the warning already emitted upstream). Null lanes never
    compare: arrow propagates input validity straight to the result —
    either side's null nulls the lane, exactly the pure path's
    `None if (x is None or y is None)` — so no sentinel ever leaks.
    """
    kernel = _CMP_KERNELS.get(op_func)
    if kernel is None:
        return None
    if type(rhs) is str:
        other = rhs
    elif isinstance(rhs, StringStorage):
        other = string_array(rhs)
        if other is None:
            return None
    else:
        return None
    arr = string_array(storage)
    if arr is None:
        return None
    return bool_storage(getattr(_pc, kernel)(arr, other))


def group_strings(storage):
    """Single-key bucketing for string columns: {(str,): row_indices}
    with keys in FIRST-APPEARANCE order and row indices ascending within
    each bucket — the same dict the pure loops build and group.py's
    group_indices returns for int64, buckets held as numpy arrays for
    the same zero-conversion flow into the downstream gathers. None to
    DECLINE.

    A two-backend composition, so BOTH switches gate it: arrow's hash
    kernel turns UTF-8 content into dense int codes (dictionary_encode
    builds its dictionary in first-appearance order, so no reorder pass
    is needed), then the codes ride the same argsort/bincount/split math
    as group.py. Only the final wrap — one dict entry and one decoded
    str per DISTINCT key — runs in Python.

    Nullable declines for group.py's reason: None is a legitimate pure-
    path group key that the buffer math cannot carry.
    """
    from . import _USE_NUMPY
    if not _USE_NUMPY or _np is None:
        return None
    if not isinstance(storage, StringStorage) or storage._mask is not None:
        return None
    arr = string_array(storage)
    if arr is None:
        return None
    enc    = arr.dictionary_encode()
    codes  = enc.indices.to_numpy(zero_copy_only=True)
    k      = len(enc.dictionary)
    order  = _np.argsort(codes, kind='stable')   # rows grouped by code,
                                                 # ascending within group
    counts = _np.bincount(codes, minlength=k)
    groups = _np.split(order, _np.cumsum(counts)[:-1])
    keys   = enc.dictionary.to_pylist()          # Python strs, one per distinct
    return {(keys[c],): groups[c] for c in range(k)}


def join_probe_strings(left_storage, right_storage,
                       expect_left_unique, expect_right_unique,
                       keep_unmatched_left, keep_unmatched_right):
    """Vectorized join probe for string key columns; None to DECLINE.

    Same two-backend composition as group_strings (both switches gate):
    arrow encodes, numpy probes. BOTH key columns encode through ONE
    dictionary — concat then dictionary_encode — so every distinct
    string on either side gets its own code, and the codes feed
    join.probe_codes exactly as int64 keys would. Encoding the sides
    separately would be wrong, not just awkward: strings absent from
    the other side would share one "missing" code, and two DISTINCT
    unmatched left keys would falsely trip expect_left_unique.

    Nullable keys decline: the pure loop joins None keys like any other
    value ((None,) == (None,) matches), and codes cannot carry that.

    Cardinality tags come back holding a duplicate CODE; the string it
    encodes — the key the pure loop's error text reports — goes out.
    """
    from . import _USE_NUMPY
    if not _USE_NUMPY or _np is None:
        return None
    if not (isinstance(left_storage, StringStorage)
            and isinstance(right_storage, StringStorage)):
        return None
    if left_storage._mask is not None or right_storage._mask is not None:
        return None
    left_arr  = string_array(left_storage)
    right_arr = string_array(right_storage)
    if left_arr is None or right_arr is None:
        return None

    from .join import probe_codes
    enc   = _pa.concat_arrays([left_arr, right_arr]).dictionary_encode()
    codes = enc.indices.to_numpy(zero_copy_only=True)
    n_l   = len(left_storage)
    result = probe_codes(codes[:n_l], codes[n_l:],
                         expect_left_unique, expect_right_unique,
                         keep_unmatched_left, keep_unmatched_right)
    if result[0] == 'ok':
        return result
    tag, (code,), count = result
    return (tag, (enc.dictionary[code].as_py(),), count)


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
