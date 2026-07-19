"""
OPTIONAL pyarrow-accelerated Parquet reading — EXPERIMENT.

serif stays zero-dependency: this module only imports when pyarrow happens
to be installed, and nothing anywhere requires it. numpy is OPTIONAL — it
accelerates two steps (the decimal byte-swap and the bool bit-unpack), but
pyarrow >= 25 no longer pulls it in, so when it is absent those steps fall
back to pure Python: identical results, just slower. The doctrine being
prototyped:

  * python in → python out. Whatever backend decodes the file, the Table
    that comes back surfaces concrete Python values (int, float, str,
    bool, date, datetime, None) — never pyarrow or numpy scalars.
    Enforced by the conformance tests in tests/test_parquet_arrow.py.

  * Accelerators may widen TRANSPORT, never SEMANTICS. pyarrow can decode
    codecs/encodings the pure reader can't (snappy, dictionary, V2 pages)
    — same values, better transport, fine. But types serif rejects
    (nanosecond timestamps, unsigned 32/64-bit ints, decimal256) must still
    reject: any column type outside the supported set declines the WHOLE
    file back to the pure reader, whose errors are the ones users see.

Supported: everything serif's own writer emits — int64, float64, string,
bool, date32, timestamp[us/ms], decimal128 — so every serif-written file
takes this path. Foreign types whose values the pure reader also surfaces
widen EXACTLY in C first (see _target_type): small/32-bit ints → int64,
float32 → float64, large_string → string, date64 → date32, and
dictionary-encoded columns decode to their value type.

Why this is fast: serif's storage layout is deliberately arrow-shaped.
  * strings   — arrow string arrays are (validity, int32 offsets, UTF-8
                buffer); StringStorage.from_raw takes exactly that. Two
                buffer copies, ZERO decode calls.
  * numerics  — data buffer → array.frombytes: one memcpy, no boxing.
  * validity  — arrow validity bitmaps and BitMask share the identical
                layout (one packed bit per element, LSB-first, 1=valid),
                so arrow's validity buffer IS a BitMask buffer: one memcpy,
                no bit twiddling, no per-row Python loop, no numpy needed.
  * bools     — arrow bit-packed values → BoolStorage's 0/1 bytes: one
                unpack pass (a single numpy C call when available), no
                interned-bool boxing.
  * objects   — date/datetime columns are TupleStorage of Python objects
                either way; arrow's C-implemented to_pylist() is the
                fastest available boxer, with None already in place.
"""

from __future__ import annotations

import array as _pyarray

try:
    import numpy as _np
except ImportError:            # pyarrow >= 25 no longer pulls numpy in
    _np = None
import pyarrow as _pa
import pyarrow.compute as _pc
import pyarrow.parquet as _pq

from datetime import date as _date, datetime as _datetime
from decimal import Decimal as _Decimal
from itertools import chain as _chain, islice as _islice

from .._vector import Vector
from .._vector.dtype import Schema as _Schema
from .._vector.nullable import BitMask
from .._vector.storage import ArrayStorage, StringStorage, DecimalStorage, BoolStorage


def _target_type(t):
    """
    Map an arrow column type to the type to DECODE AS; None means DECLINE.

    Identity for the types serif's writer emits. A handful of foreign types
    additionally widen to a supported type — legal because the widening is
    EXACT and the pure reader accepts the same parquet columns and surfaces
    the same Python values (transport, not semantics):

      * int8/16/32, uint8/16 → int64 — all surface as Python int either way
      * float32 → float64            — every float32 is exact in float64
      * large_string → string        — same bytes, narrower offsets (a
                                       column past 2 GB fails the cast and
                                       declines via try_read's guard)
      * date64 → date32              — parquet DATE is days; day-aligned
      * dictionary<supported>        — decodes to its value type: parquet
                                       dictionary pages are compression,
                                       not a categorical claim, and the
                                       pure reader surfaces plain values

    Types serif REJECTS (nanosecond timestamps, uint32/64, decimal256)
    return None: the whole file declines, and the pure reader's refusal is
    what users see, with pyarrow installed or not.
    """
    if _pa.types.is_dictionary(t):
        vt = t.value_type
        return _target_type(vt) if not _pa.types.is_dictionary(vt) else None
    if (_pa.types.is_int64(t) or _pa.types.is_float64(t)
            or _pa.types.is_string(t) or _pa.types.is_boolean(t)
            or _pa.types.is_date32(t)):
        return t
    if (_pa.types.is_int8(t) or _pa.types.is_int16(t) or _pa.types.is_int32(t)
            or _pa.types.is_uint8(t) or _pa.types.is_uint16(t)):
        return _pa.int64()
    if _pa.types.is_float32(t):
        return _pa.float64()
    if _pa.types.is_large_string(t):
        return _pa.string()
    if _pa.types.is_date64(t):
        return _pa.date32()
    # Nanos decline: the pure reader raises its truncation-refusal error,
    # exactly as it would with pyarrow absent. Semantics stay serif's.
    if _pa.types.is_timestamp(t) and t.unit in ('us', 'ms'):
        return t
    # decimal128 only; decimal256 declines (serif has no 256-bit backend).
    if _pa.types.is_decimal(t) and t.bit_width == 128:
        return t
    return None


def try_read(path):
    """
    Read `path` via pyarrow if every column type is supported.

    Returns a Table, or None to DECLINE — whole-file fallback to the pure
    reader. Declining covers unsupported column types (semantics stay
    serif's: the pure reader raises its own loud errors) and any pyarrow
    parse failure (so corrupt files surface serif's messages, not arrow's).
    """
    from ..table import Table

    # Decline must be CHEAP: check the schema from the footer metadata
    # BEFORE decoding any data.
    try:
        pf = _pq.ParquetFile(path)
    except Exception:
        return None

    schema  = pf.schema_arrow
    targets = [_target_type(field.type) for field in schema]
    if not all(t is not None for t in targets):
        return None

    try:
        table = pf.read()
    except Exception:
        return None

    # Arrow-layer failures during conversion (cast overflow, string concat
    # past 2 GB, …) decline like any other pyarrow failure — the pure
    # reader's errors surface. Serif bugs (IndexError etc.) stay loud.
    try:
        cols = []
        for i, (field, target) in enumerate(zip(schema, targets)):
            chunked = table.column(i)
            if chunked.type != target:
                # C-level normalization: dictionary decode / exact widen.
                chunked = chunked.cast(target)
            if chunked.num_chunks == 1:
                arr = chunked.chunk(0)
            elif chunked.num_chunks == 0:
                arr = _pa.array([], type=target)
            else:
                arr = _pa.concat_arrays(chunked.chunks)
            if arr.offset != 0:
                # The buffer math below assumes offset 0. Fresh reads always
                # are; decline rather than risk a misread.
                return None
            cols.append(_to_vector(arr, field.name, field.nullable))
        return Table._from_columns_nocopy(cols)
    except _pa.ArrowException:
        return None


def try_read_column(path, column_index, row_groups, mask_segments):
    """Decode one projected Parquet column in bounded Arrow batches.

    Returns Serif Vector pieces, or None to decline to the pure reader.
    """
    try:
        pf = _pq.ParquetFile(path)
    except Exception:
        return None

    schema = pf.schema_arrow
    targets = [_target_type(field.type) for field in schema]
    if not all(target is not None for target in targets):
        return None

    field = schema.field(column_index)
    # ParquetFile projection is name-based; duplicate names are legal in
    # Serif, so decline rather than risk selecting the wrong occurrence.
    if schema.names.count(field.name) != 1:
        return None

    mask_iter = None
    if mask_segments and mask_segments[0] is not None:
        mask_iter = iter(_chain.from_iterable(mask_segments))

    try:
        vectors = []
        for batch in pf.iter_batches(
                row_groups=row_groups, columns=[field.name]):
            arr = batch.column(0)
            target = targets[column_index]
            if arr.type != target:
                arr = arr.cast(target)
            if mask_iter is not None:
                selected = list(_islice(mask_iter, len(arr)))
                if len(selected) != len(arr):
                    return None
                arr = _pc.filter(
                    arr,
                    _pa.array(selected, type=_pa.bool_()),
                    null_selection_behavior='drop',
                )
            if arr.offset != 0:
                arr = arr.slice(0)
            vectors.append(_to_vector(arr, field.name, field.nullable))
        return vectors
    except _pa.ArrowException:
        return None


def _bit_mask(arr):
    """
    Arrow validity bitmap → serif BitMask, near-zero-copy.

    Arrow and BitMask use the identical layout: one packed bit per element,
    least-significant-bit-first, 1=valid/0=null. So arrow's validity buffer
    is already a valid BitMask buffer — copy it once and trim to the ceil(n/8)
    bytes BitMask holds (arrow pads the buffer to a wider alignment; the
    trailing bytes and any bits past n are never read but we drop them so the
    buffer length matches what BitMask.from_iterable would have produced).
    No numpy needed on this path, with or without it installed.

    Callers must pass an array at offset 0 (try_read declines otherwise); a
    non-zero offset would misalign every bit against this bytewise copy.
    """
    if arr.null_count == 0:
        return None
    n   = len(arr)
    buf = bytearray(memoryview(arr.buffers()[0])[:(n + 7) // 8])
    return BitMask(buf, n)


def _to_vector(arr, name, nullable):
    # arr is already normalized to a _target_type (try_read casts), so its
    # own type — not the file schema's — is what decodes here.
    n = len(arr)
    t = arr.type

    if _pa.types.is_string(t):
        dtype = _Schema(str, nullable)
        if n == 0:
            return Vector._from_iterable_known_dtype([], dtype, name=name)
        # Arrow pads buffers to a wider alignment; trim each memoryview to
        # the live bytes BEFORE copying so every buffer costs one memcpy.
        bufs = arr.buffers()  # [validity, int32 offsets, utf-8 data]
        offs = _pyarray.array('I')
        offs.frombytes(memoryview(bufs[1])[:(n + 1) * 4])  # same layout as int32
        data_buf = bufs[2]
        raw = memoryview(data_buf)[:offs[-1]].tobytes() if data_buf is not None else b''
        storage = StringStorage.from_raw(raw, offs, _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=name)

    if _pa.types.is_int64(t) or _pa.types.is_float64(t):
        kind, typecode = (int, 'q') if _pa.types.is_int64(t) else (float, 'd')
        dtype = _Schema(kind, nullable)
        if n == 0:
            return Vector._from_iterable_known_dtype([], dtype, name=name)
        data = _pyarray.array(typecode)
        # Trim the padding before the copy: one memcpy, no boxing.
        data.frombytes(memoryview(arr.buffers()[1])[:n * 8])
        storage = ArrayStorage(data, _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=name)

    if _pa.types.is_decimal(t):
        # Arrow decimal128 is little-endian; DecimalStorage is big-endian
        # (Parquet-native), so each 16-byte row is byte-reversed. With numpy
        # that's one vectorised reshape + reverse over a zero-copy view of
        # arrow's buffer (all C); without numpy a per-row slice-reverse loop
        # yields the identical big-endian bytes.
        scale     = t.scale
        precision = t.precision
        dtype     = _Schema(_Decimal, nullable)
        if n == 0:
            return Vector._from_storage(
                DecimalStorage(bytearray(), scale, precision, None),
                dtype, name=name)
        le = memoryview(arr.buffers()[1])[:n * 16]
        if _np is not None:
            le_rows  = _np.frombuffer(le, dtype=_np.uint8).reshape(n, 16)
            be_bytes = le_rows[:, ::-1].tobytes()  # reverse each row → big-endian
        else:
            le_buf = le.tobytes()
            be = bytearray(n * 16)
            for i in range(n):
                be[i * 16:(i + 1) * 16] = le_buf[i * 16:(i + 1) * 16][::-1]
            be_bytes = bytes(be)
        storage = DecimalStorage.from_raw_be(be_bytes, scale, precision,
                                             _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=name)

    if _pa.types.is_boolean(t):
        # Arrow bools are bit-packed; BoolStorage is byte-packed. One
        # unpack pass — a single C call with numpy, a bit loop without
        # (same optional-numpy trade as the decimal byte-swap).
        dtype = _Schema(bool, nullable)
        if n == 0:
            return Vector._from_iterable_known_dtype([], dtype, name=name)
        bit_buf = arr.buffers()[1]
        if _np is not None:
            bits = _np.frombuffer(bit_buf, dtype=_np.uint8)  # zero-copy view
            data = bytearray(
                _np.unpackbits(bits, count=n, bitorder='little').tobytes())
        else:
            mv   = memoryview(bit_buf)
            data = bytearray(n)
            for i in range(n):
                data[i] = (mv[i >> 3] >> (i & 7)) & 1
        storage = BoolStorage.from_raw(data, _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=name)

    # date32 / timestamp → TupleStorage of Python objects, the same
    # backend the pure reader builds. arrow's to_pylist() boxes in C with
    # None already at the null positions.
    if _pa.types.is_date32(t):
        kind = _date
    else:  # timestamp[us/ms], per _target_type
        kind = _datetime
        if t.tz is not None:
            # The pure reader surfaces naive datetimes (µs since epoch,
            # no zone). Cast tz-aware columns to naive in C so both paths
            # agree; same underlying instant, same values out.
            arr = arr.cast(_pa.timestamp(t.unit))
    dtype = _Schema(kind, nullable)
    return Vector._from_iterable_known_dtype(arr.to_pylist(), dtype, name=name)
