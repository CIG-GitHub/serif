"""
OPTIONAL pyarrow-accelerated Parquet reading — EXPERIMENT.

serif stays zero-dependency: this module only imports when pyarrow happens
to be installed, and nothing anywhere requires it. numpy is OPTIONAL — it
accelerates two steps (validity-bitmap unpacking and the decimal byte-swap),
but pyarrow >= 25 no longer pulls it in, so when it is absent those steps
fall back to pure Python: identical results, just slower. The doctrine
being prototyped:

  * python in → python out. Whatever backend decodes the file, the Table
    that comes back surfaces concrete Python values (int, float, str,
    bool, date, datetime, None) — never pyarrow or numpy scalars.
    Enforced by the conformance tests in tests/test_parquet_arrow.py.

  * Accelerators may widen TRANSPORT, never SEMANTICS. pyarrow can decode
    codecs/encodings the pure reader can't (snappy, dictionary, V2 pages)
    — same values, better transport, fine. But types serif rejects
    (DECIMAL, nanosecond timestamps, unsigned 64s) must still reject: any
    column type outside the supported set declines the WHOLE file back to
    the pure reader, whose errors are the ones users see.

Supported: everything serif's own writer emits — int64, float64, string,
bool, date32, timestamp[us/ms]. Every serif-written file takes this path.

Why this is fast: serif's storage layout is deliberately arrow-shaped.
  * strings   — arrow string arrays are (validity, int32 offsets, UTF-8
                buffer); StringStorage.from_raw takes exactly that. Two
                buffer copies, ZERO decode calls.
  * numerics  — data buffer → array.frombytes: one memcpy, no boxing.
  * validity  — arrow validity bitmaps and BitMask share the identical
                layout (one packed bit per element, LSB-first, 1=valid),
                so arrow's validity buffer IS a BitMask buffer: one memcpy,
                no bit twiddling, no per-row Python loop, no numpy needed.
  * objects   — bool/date/datetime columns are TupleStorage of Python
                objects either way; arrow's C-implemented to_pylist() is
                the fastest available boxer, with None already in place.
"""

from __future__ import annotations

import array as _pyarray

try:
    import numpy as _np
except ImportError:            # pyarrow >= 25 no longer pulls numpy in
    _np = None
import pyarrow as _pa
import pyarrow.parquet as _pq

from datetime import date as _date, datetime as _datetime
from decimal import Decimal as _Decimal

from .._vector import Vector
from .._vector.dtype import Schema as _Schema
from .._vector.nullable import BitMask
from .._vector.storage import ArrayStorage, StringStorage, DecimalStorage


def _supported(t) -> bool:
    if (_pa.types.is_int64(t) or _pa.types.is_float64(t)
            or _pa.types.is_string(t) or _pa.types.is_boolean(t)
            or _pa.types.is_date32(t)):
        return True
    # Nanos decline: the pure reader raises its truncation-refusal error,
    # exactly as it would with pyarrow absent. Semantics stay serif's.
    if _pa.types.is_timestamp(t) and t.unit in ('us', 'ms'):
        return True
    # decimal128 only; decimal256 declines (serif has no 256-bit backend).
    if _pa.types.is_decimal(t) and t.bit_width == 128:
        return True
    return False


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

    schema = pf.schema_arrow
    if not all(_supported(field.type) for field in schema):
        return None

    try:
        table = pf.read()
    except Exception:
        return None

    cols = []
    for i, field in enumerate(schema):
        chunked = table.column(i)
        if chunked.num_chunks == 1:
            arr = chunked.chunk(0)
        elif chunked.num_chunks == 0:
            arr = _pa.array([], type=field.type)
        else:
            arr = _pa.concat_arrays(chunked.chunks)
        if arr.offset != 0:
            # The buffer math below assumes offset 0. Fresh reads always
            # are; decline rather than risk a misread.
            return None
        cols.append(_to_vector(arr, field))
    return Table._from_columns_nocopy(cols)


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


def _to_vector(arr, field):
    n = len(arr)
    t = field.type

    if _pa.types.is_string(t):
        dtype = _Schema(str, field.nullable)
        if n == 0:
            return Vector._from_iterable_known_dtype([], dtype, name=field.name)
        bufs = arr.buffers()  # [validity, int32 offsets, utf-8 data]
        offs = _pyarray.array('I')
        offs.frombytes(memoryview(bufs[1]))  # same byte layout as int32
        if len(offs) > n + 1:
            offs = offs[:n + 1]              # arrow pads buffers to 64 bytes
        data_buf = bufs[2]
        raw = bytes(memoryview(data_buf))[:offs[-1]] if data_buf is not None else b''
        storage = StringStorage.from_raw(raw, offs, _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=field.name)

    if _pa.types.is_int64(t) or _pa.types.is_float64(t):
        kind, typecode = (int, 'q') if _pa.types.is_int64(t) else (float, 'd')
        dtype = _Schema(kind, field.nullable)
        if n == 0:
            return Vector._from_iterable_known_dtype([], dtype, name=field.name)
        data = _pyarray.array(typecode)
        data.frombytes(memoryview(arr.buffers()[1]))  # one memcpy, no boxing
        if len(data) > n:
            data = data[:n]                            # strip 64-byte padding
        storage = ArrayStorage(data, _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=field.name)

    if _pa.types.is_decimal(t):
        # Arrow decimal128 is little-endian; DecimalStorage is big-endian
        # (Parquet-native), so each 16-byte row is byte-reversed. With numpy
        # that's one vectorised reshape + reverse (all C); without numpy a
        # per-row slice-reverse loop yields the identical big-endian bytes.
        scale     = t.scale
        precision = t.precision
        dtype     = _Schema(_Decimal, field.nullable)
        if n == 0:
            return Vector._from_storage(
                DecimalStorage(bytearray(), scale, precision, None),
                dtype, name=field.name)
        le_buf = bytes(memoryview(arr.buffers()[1]))[:n * 16]
        if _np is not None:
            le_rows  = _np.frombuffer(le_buf, dtype=_np.uint8).reshape(n, 16)
            be_bytes = le_rows[:, ::-1].tobytes()  # reverse each row → big-endian
        else:
            be = bytearray(n * 16)
            for i in range(n):
                be[i * 16:(i + 1) * 16] = le_buf[i * 16:(i + 1) * 16][::-1]
            be_bytes = bytes(be)
        storage = DecimalStorage.from_raw_be(be_bytes, scale, precision,
                                             _bit_mask(arr))
        return Vector._from_storage(storage, dtype, name=field.name)

    # bool / date32 / timestamp → TupleStorage of Python objects, the same
    # backend the pure reader builds. arrow's to_pylist() boxes in C with
    # None already at the null positions.
    if _pa.types.is_boolean(t):
        kind = bool
    elif _pa.types.is_date32(t):
        kind = _date
    else:  # timestamp[us/ms], per _supported
        kind = _datetime
        if t.tz is not None:
            # The pure reader surfaces naive datetimes (µs since epoch,
            # no zone). Cast tz-aware columns to naive in C so both paths
            # agree; same underlying instant, same values out.
            arr = arr.cast(_pa.timestamp(t.unit))
    dtype = _Schema(kind, field.nullable)
    return Vector._from_iterable_known_dtype(arr.to_pylist(), dtype, name=field.name)
