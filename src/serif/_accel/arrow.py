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

Second job, same doctrine: CHECKED numeric kernels (binop_ints,
div_floats). Not because numpy can't do the math — because it can't do
it SAFELY without preparing: np.int64 wraps silently, so the numpy tier
predicts overflow with a bounds pass and declines whenever it is
POSSIBLE; numpy division executes every lane, so null-lane divisors
must be neutralized to 1 (a copy) and zeros scanned for (a pass) first.
Arrow's *_checked kernels detect instead: they skip null lanes, compute
once, and raise on the first lane that actually overflows or divides by
zero — so int over-declines get a second chance before pure, and
division skips the preparation passes entirely (which is why truediv
tries arrow FIRST — see api.py's _accel_binop).

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
_U64 = 2**64

_NUMERIC_PA_TYPES = ({'q': _pa.int64(), 'd': _pa.float64()}
                     if _pa is not None else {})

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


def numeric_array(storage):
    """ArrayStorage('q'/'d') → pa Int64Array/DoubleArray over the SAME
    buffers; None to DECLINE (pyarrow absent/off, wrong storage or
    typecode, empty). Both typecodes are 8-byte little-endian lanes and
    BitMask is the validity bitmap — the wrap is two py_buffer views,
    zero copies."""
    if not _USE_ARROW or not isinstance(storage, ArrayStorage):
        return None
    pa_type = _NUMERIC_PA_TYPES.get(storage._data.typecode)
    if pa_type is None:
        return None
    n = len(storage)
    if n == 0:
        return None
    mask = storage._mask
    validity = _pa.py_buffer(mask._buf) if mask is not None else None
    return _pa.Array.from_buffers(
        pa_type, n,
        [validity, _pa.py_buffer(storage._data)],
        -1 if mask is not None else 0)   # -1: arrow counts nulls lazily


def int64_array(storage):
    """numeric_array narrowed to int lanes — binop_ints' checked kernels
    are an INT overflow story, so both operands must be 'q'."""
    if isinstance(storage, ArrayStorage) and storage._data.typecode == 'q':
        return numeric_array(storage)
    return None


def numeric_storage(arr):
    """pa Int64Array/DoubleArray (a kernel result) → ArrayStorage; None
    to DECLINE (non-zero offset — same rationale as bool_storage — or a
    lane type serif doesn't store). One memcpy for the lanes, one for
    the validity trim. Lanes under null are whatever the kernel left
    there: unobservable garbage, the pure path's 0-sentinel contract
    (ops.py)."""
    if arr.offset != 0:
        return None
    if _pa.types.is_int64(arr.type):
        typecode = 'q'
    elif _pa.types.is_float64(arr.type):
        typecode = 'd'
    else:
        return None
    n = len(arr)
    data = _pyarray.array(typecode)
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
    return numeric_storage(out)


def div_floats(lhs_storage, rhs, op_func, result_kind):
    """Checked float-producing division on buffers → ArrayStorage, or
    None to DECLINE. Tried BEFORE the numpy tier (base.py): both wraps
    are zero-copy, and this kernel simply does less work — numpy must
    neutralize null-lane divisors to 1 (a copy) and scan for zeros (a
    pass) before it dares divide; arrow's divide_checked never executes
    null lanes and raises on the first REAL zero divisor, which is the
    exact decline point anyway (the pure path raises ZeroDivisionError).
    Identical results, fewer passes.

    Bit-identical tier: one IEEE division per lane, int operands
    converted to float64 exactly as Python converts them lane-wise.
    Two exclusions, one per operand shape:

      * int / int declines — Python true-divides integers EXACTLY at
        any magnitude; float64 transport is only exact through 2**53.
        The numpy tier owns the guarded small-value case; past it, pure
        is the only exact engine.

      * floordiv/mod are not division kernels here AT ALL (see
        binop_ints: arrow truncates toward zero, Python floors —
        semantics, not transport).

    Zero scalars decline immediately; a huge int scalar (outside int64)
    declines because the pure path promotes.
    """
    if op_func is not _op.truediv or result_kind is not float:
        return None
    arr = numeric_array(lhs_storage)
    if arr is None:
        return None
    lhs_is_int = lhs_storage._data.typecode == 'q'
    if isinstance(rhs, ArrayStorage):
        other = numeric_array(rhs)
        if other is None:
            return None
        rhs_is_int = rhs._data.typecode == 'q'
    elif type(rhs) is float:
        if rhs == 0.0:
            return None            # pure raises ZeroDivisionError
        other = _pa.scalar(rhs, _pa.float64())
        rhs_is_int = False
    elif type(rhs) is int:
        if rhs == 0 or not (_I64_MIN <= rhs <= _I64_MAX):
            return None
        other = _pa.scalar(rhs, _pa.int64())
        rhs_is_int = True
    else:
        return None
    if lhs_is_int and rhs_is_int:
        return None
    try:
        out = _pc.divide_checked(arr, other)
    except (_pa.ArrowInvalid, _pa.ArrowNotImplementedError):
        # ArrowInvalid: a zero divisor actually divided — pure raises.
        # NotImplemented: this pyarrow lacks the kernel/type combo — the
        # decline widens transport back to the numpy/pure tiers.
        return None
    return numeric_storage(out)


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


def join_probe_strings_hash(left_storage, right_storage,
                            expect_left_unique, expect_right_unique,
                            keep_unmatched_left, keep_unmatched_right):
    """O(n) right-unique string probe over shared Arrow dictionary codes.

    String joins already must hash/decode their content. This path removes the
    subsequent code sort for the right-unique case; duplicate and unsupported
    cases decline to join_probe_strings and its established diagnostics.
    """
    from . import _USE_NUMPY
    if not _USE_ARROW or not _USE_NUMPY or _np is None:
        return None
    if not expect_right_unique:
        return None
    if not (isinstance(left_storage, StringStorage)
            and isinstance(right_storage, StringStorage)):
        return None
    if left_storage._mask is not None or right_storage._mask is not None:
        return None
    left_arr = string_array(left_storage)
    right_arr = string_array(right_storage)
    if left_arr is None or right_arr is None:
        return None

    # Right first makes its code lane a convenient prefix. One shared
    # dictionary is essential: independently encoded absent values could
    # otherwise collide when the left codes probe the right lookup.
    enc = _pa.concat_arrays([right_arr, left_arr]).dictionary_encode()
    codes = enc.indices.to_numpy(zero_copy_only=True)
    n_right = len(right_storage)
    right_codes = codes[:n_right]
    left_codes = codes[n_right:]
    n_codes = len(enc.dictionary)

    from .join import probe_unique_codes
    return probe_unique_codes(
        left_codes, right_codes, n_codes,
        expect_left_unique, expect_right_unique,
        keep_unmatched_left, keep_unmatched_right)


def grouped_sums(key_storage, value_storages):
    """Hash-group one key and sum one or more numeric value columns.

    Return ``(keys, value_columns)`` in first-appearance group order, or
    None to decline. This is deliberately narrower than Arrow's group-by
    surface: dense int/string keys and numeric values only. The caller uses
    it only for recognized bound ``Vector.sum`` aggregations.

    Arrow's int64 hash sum is a modular carrier, not Serif's answer. For
    each value column we also ask for count/min/max and apply reduce.py's
    residue proof independently to every group. If one group is ambiguous,
    the entire operation declines to the ordinary per-group path.
    """
    if not _USE_ARROW:
        return None

    if (isinstance(key_storage, ArrayStorage)
            and key_storage._data.typecode == 'q'
            and key_storage._mask is None):
        key_arr = int64_array(key_storage)
    elif (isinstance(key_storage, StringStorage)
          and key_storage._mask is None):
        key_arr = string_array(key_storage)
    else:
        return None
    if key_arr is None:
        return None

    value_arrays = []
    for storage in value_storages:
        arr = numeric_array(storage)
        if arr is None:
            return None
        value_arrays.append(arr)

    key_name = '__serif_group_key'
    value_names = [f'__serif_value_{i}' for i in range(len(value_arrays))]
    table = _pa.Table.from_arrays(
        [key_arr, *value_arrays], names=[key_name, *value_names])
    specs = []
    for name in value_names:
        specs.extend([
            (name, 'sum'),
            (name, 'count'),
            (name, 'min'),
            (name, 'max'),
        ])
    try:
        # Threaded grouping has no stable-output guarantee. Single-threaded
        # grouping retains encounter order, Serif's public group-order rule.
        grouped = table.group_by(key_name, use_threads=False).aggregate(specs)
    except (_pa.ArrowInvalid, _pa.ArrowNotImplementedError):
        return None

    keys = grouped[key_name].to_pylist()
    outputs = []
    for storage, name in zip(value_storages, value_names):
        wrapped = grouped[f'{name}_sum'].to_pylist()
        counts = grouped[f'{name}_count'].to_pylist()
        mins = grouped[f'{name}_min'].to_pylist()
        maxs = grouped[f'{name}_max'].to_pylist()

        if storage._data.typecode == 'q':
            values = []
            for residue, count, mn, mx in zip(wrapped, counts, mins, maxs):
                n = int(count)
                if n == 0:
                    values.append(0)
                    continue
                mn = int(mn)
                mx = int(mx)
                if n * (mx - mn) >= _U64:
                    return None
                residue = int(residue)
                spread_sum = (residue - n * mn) % _U64
                values.append(n * mn + spread_sum)
            outputs.append(values)
        else:
            # Existing accelerated float sum already accepts backend
            # reduction-order differences. All-null groups retain Python
            # sum's identity rather than Arrow's null scalar.
            outputs.append([
                0 if count == 0 else float(value)
                for value, count in zip(wrapped, counts)
            ])

    return keys, outputs


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
