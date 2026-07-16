"""
Parquet read/write for serif Tables.

This is a deliberately minimal, zero-dependency "round-trippable subset":

  WRITE — produces valid Parquet 2.x (PLAIN encoding, UNCOMPRESSED).
           Any Parquet reader (pyarrow, DuckDB, pandas, Spark) can consume it.

  READ  — reads Parquet written by this module; also handles PLAIN-encoded,
           UNCOMPRESSED or GZIP-compressed files from other tools.
           Snappy raises with an informative error. Anything the reader
           cannot decode FAITHFULLY raises rather than misreads: DataPage V2,
           dictionary/RLE value encodings, and type annotations with no
           faithful Python value (DECIMAL, TIME, unsigned 32/64-bit ints,
           nanosecond timestamps) — never silent wrong values.

Supported column types (write & read):
    bool      → BOOLEAN  (bit-packed)
    int       → INT64    (ArrayStorage 'q' only; TupleStorage raises — ints
                          may be arbitrarily wide, which doesn't fit INT64)
    float     → DOUBLE
    str       → BYTE_ARRAY + ConvertedType=UTF8
    date      → INT32    + ConvertedType=DATE (days since 1970-01-01)
    datetime  → INT64    + ConvertedType=TIMESTAMP_MICROS (µs since epoch)

object (mixed-type) columns and unsupported physical types raise SerifTypeError.
External files with dictionary encoding or nested schemas are not supported.
"""

from __future__ import annotations

import array as _pyarray
import struct as _struct
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
from decimal import Decimal as _Decimal, ROUND_HALF_EVEN as _ROUND_HALF_EVEN

from ..errors import SerifTypeError, SerifValueError
from .._vector import Vector
from .._vector.storage import ArrayStorage, StringStorage, DecimalStorage

# ---------------------------------------------------------------------------
# Optional pyarrow acceleration (read side) — EXPERIMENT
# ---------------------------------------------------------------------------
# serif stays zero-dependency: pyarrow is never required. When it happens to
# be installed, _arrow.py may decode a supported file faster and must hand
# back an IDENTICAL Table (python in → python out; see
# tests/test_parquet_arrow.py). Accelerators may widen TRANSPORT (codecs,
# encodings), never SEMANTICS (types serif rejects still reject — the
# accelerator declines and this module's errors surface).
# _USE_ARROW is a private switch for tests/benchmarks, not API.
try:
    from . import _arrow as _arrow_accel
except ImportError:            # pyarrow not installed
    _arrow_accel = None

_USE_ARROW = _arrow_accel is not None

# ---------------------------------------------------------------------------
# File-level constants
# ---------------------------------------------------------------------------

_MAGIC = b'PAR1'

# ---------------------------------------------------------------------------
# Parquet enum constants  (from parquet.thrift)
# ---------------------------------------------------------------------------

# Type (physical)
_T_BOOLEAN              = 0
_T_INT32                = 1
_T_INT64                = 2
_T_FLOAT                = 4
_T_DOUBLE               = 5
_T_BYTE_ARRAY           = 6
_T_FIXED_LEN_BYTE_ARRAY = 7

# FieldRepetitionType
_REP_REQUIRED = 0
_REP_OPTIONAL = 1

# ConvertedType
_CT_UTF8             = 0
_CT_DATE             = 6
_CT_DECIMAL          = 5
_CT_TIMESTAMP_MILLIS = 9
_CT_TIMESTAMP_MICROS = 10

# LogicalType union field ids (SchemaElement field 10). Newer writers use
# these instead of (or alongside) ConvertedType; TIMESTAMP_NANOS and UUID
# exist ONLY here, so the reader must look or it misreads them as raw ints.
_LT_STRING    = 1
_LT_TIMESTAMP = 8

_LT_NAMES = {1: 'STRING', 2: 'MAP', 3: 'LIST', 4: 'ENUM', 5: 'DECIMAL',
             6: 'DATE', 7: 'TIME', 8: 'TIMESTAMP', 10: 'INTEGER',
             11: 'UNKNOWN', 12: 'JSON', 13: 'BSON', 14: 'UUID',
             15: 'FLOAT16'}

# TimeUnit union field ids (TimestampType field 2)
_TU_MILLIS = 1
_TU_MICROS = 2
_TU_NANOS  = 3

# Encoding
_ENC_PLAIN = 0
_ENC_RLE   = 3

# CompressionCodec
_CODEC_UNCOMPRESSED = 0
_CODEC_GZIP         = 2
_CODEC_SNAPPY       = 1

# PageType
_PAGE_DATA       = 0
_PAGE_DICTIONARY = 2
_PAGE_DATA_V2    = 3

# ---------------------------------------------------------------------------
# Thrift compact binary — type codes
# ---------------------------------------------------------------------------

_TC_BOOL_TRUE  = 1
_TC_BOOL_FALSE = 2
_TC_BYTE       = 3
_TC_I16        = 4
_TC_I32        = 5
_TC_I64        = 6
_TC_DOUBLE_TC  = 7
_TC_BINARY     = 8
_TC_LIST       = 9
_TC_SET        = 10
_TC_MAP        = 11
_TC_STRUCT     = 12

# ---------------------------------------------------------------------------
# Thrift compact binary — encode helpers
# ---------------------------------------------------------------------------

def _varint_encode(n: int) -> bytes:
    """Encode a non-negative integer as a little-endian varint."""
    buf = bytearray()
    while n > 0x7F:
        buf.append((n & 0x7F) | 0x80)
        n >>= 7
    buf.append(n & 0x7F)
    return bytes(buf)


def _zigzag32(n: int) -> int:
    return ((n << 1) ^ (n >> 31)) & 0xFFFFFFFF


def _zigzag64(n: int) -> int:
    return ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF


def _enc_i32(n: int) -> bytes:
    return _varint_encode(_zigzag32(n))


def _enc_i64(n: int) -> bytes:
    return _varint_encode(_zigzag64(n))


def _enc_str(s: str) -> bytes:
    b = s.encode('utf-8')
    return _varint_encode(len(b)) + b


def _enc_list_hdr(count: int, elem_type: int) -> bytes:
    """
    Thrift compact list header.
    count < 15  → 1 byte: (count << 4) | elem_type
    count >= 15 → 1 byte: 0xF0 | elem_type, then count as varint
    """
    if count < 15:
        return bytes([(count << 4) | elem_type])
    return bytes([0xF0 | elem_type]) + _varint_encode(count)


class _ThriftWriter:
    """Incremental Thrift compact binary struct builder."""

    __slots__ = ('_buf', '_last_fid')

    def __init__(self) -> None:
        self._buf: bytearray = bytearray()
        self._last_fid: int = 0

    # -- private ----------------------------------------------------------

    def _fhdr(self, field_id: int, type_code: int) -> bytes:
        delta = field_id - self._last_fid
        self._last_fid = field_id
        if 1 <= delta <= 15:
            return bytes([(delta << 4) | type_code])
        # Long form: type byte (low 4 bits only) then zigzag i16 field id
        return bytes([type_code]) + _enc_i32(field_id)

    # -- public field writers ---------------------------------------------

    def bool_(self, fid: int, value: bool) -> None:
        code = _TC_BOOL_TRUE if value else _TC_BOOL_FALSE
        self._buf.extend(self._fhdr(fid, code))
        # Boolean value is encoded in the type code; no separate byte.

    def i32(self, fid: int, value: int) -> None:
        self._buf.extend(self._fhdr(fid, _TC_I32))
        self._buf.extend(_enc_i32(value))

    def i64(self, fid: int, value: int) -> None:
        self._buf.extend(self._fhdr(fid, _TC_I64))
        self._buf.extend(_enc_i64(value))

    def string(self, fid: int, value: str) -> None:
        self._buf.extend(self._fhdr(fid, _TC_BINARY))
        self._buf.extend(_enc_str(value))

    def struct(self, fid: int, inner: bytes) -> None:
        """Write a struct field. `inner` must already end with a stop byte."""
        self._buf.extend(self._fhdr(fid, _TC_STRUCT))
        self._buf.extend(inner)

    def list_i32(self, fid: int, values: list) -> None:
        self._buf.extend(self._fhdr(fid, _TC_LIST))
        self._buf.extend(_enc_list_hdr(len(values), _TC_I32))
        for v in values:
            self._buf.extend(_enc_i32(v))

    def list_str(self, fid: int, values: list) -> None:
        self._buf.extend(self._fhdr(fid, _TC_LIST))
        self._buf.extend(_enc_list_hdr(len(values), _TC_BINARY))
        for s in values:
            self._buf.extend(_enc_str(s))

    def list_struct(self, fid: int, structs: list) -> None:
        """Write a list of pre-serialised struct byte strings."""
        self._buf.extend(self._fhdr(fid, _TC_LIST))
        self._buf.extend(_enc_list_hdr(len(structs), _TC_STRUCT))
        for s in structs:
            self._buf.extend(s)

    def stop(self) -> bytes:
        """Append stop byte and return the finished struct bytes."""
        self._buf.append(0x00)
        return bytes(self._buf)


# ---------------------------------------------------------------------------
# Thrift compact binary — decode helpers
# ---------------------------------------------------------------------------

def _varint_decode(data, pos: int):
    """Decode unsigned varint. Returns (value, new_pos)."""
    result = shift = 0
    while True:
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7


def _zzdec(n: int) -> int:
    """Zigzag decode."""
    return (n >> 1) ^ -(n & 1)


def _dec_i32(data, pos):
    n, pos = _varint_decode(data, pos)
    return _zzdec(n), pos


def _dec_i64(data, pos):
    n, pos = _varint_decode(data, pos)
    return _zzdec(n), pos


def _dec_str(data, pos):
    length, pos = _varint_decode(data, pos)
    return bytes(data[pos:pos + length]).decode('utf-8'), pos + length


def _skip_field(data, pos: int, type_code: int) -> int:
    """Skip past a Thrift field value. Returns new pos."""
    if type_code in (_TC_BOOL_TRUE, _TC_BOOL_FALSE):
        pass  # value encoded in type byte
    elif type_code == _TC_BYTE:
        # i8 is one raw byte, not a varint. IntType.bitWidth is the notable
        # user — skipping it as zero-width would desync the footer parse.
        pos += 1
    elif type_code in (_TC_I16, _TC_I32):
        _, pos = _varint_decode(data, pos)
    elif type_code == _TC_I64:
        _, pos = _varint_decode(data, pos)
    elif type_code == _TC_DOUBLE_TC:
        pos += 8
    elif type_code == _TC_BINARY:
        length, pos = _varint_decode(data, pos)
        pos += length
    elif type_code == _TC_STRUCT:
        pos = _skip_struct(data, pos)
    elif type_code in (_TC_LIST, _TC_SET):
        pos = _skip_list_or_set(data, pos)
    elif type_code == _TC_MAP:
        pos = _skip_map(data, pos)
    return pos


def _skip_struct(data, pos: int) -> int:
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            return pos
        tc = b & 0x0F
        delta = (b >> 4) & 0x0F
        if delta:
            last += delta
        else:
            # Long-form header: field id follows as zigzag varint. Consume it
            # exactly once — decoding it twice desyncs the whole footer parse.
            last, pos = _dec_i32(data, pos)
        pos = _skip_field(data, pos, tc)


def _skip_list_or_set(data, pos: int) -> int:
    b = data[pos]; pos += 1
    et = b & 0x0F
    count = (b >> 4) & 0x0F
    if count == 0x0F:
        count, pos = _varint_decode(data, pos)
    for _ in range(count):
        pos = _skip_field(data, pos, et)
    return pos


def _skip_map(data, pos: int) -> int:
    n, pos = _varint_decode(data, pos)
    if n == 0:
        return pos
    types = data[pos]; pos += 1
    kt = (types >> 4) & 0x0F
    vt = types & 0x0F
    for _ in range(n):
        pos = _skip_field(data, pos, kt)
        pos = _skip_field(data, pos, vt)
    return pos


# ---------------------------------------------------------------------------
# Parquet struct encoding
# ---------------------------------------------------------------------------

def _enc_schema_element(name: str, phys_type, conv_type, rep_type,
                         num_children=None) -> bytes:
    """
    SchemaElement fields:
      1: type (i32)            — absent for root group
      3: repetition_type (i32)
      4: name (string)
      5: num_children (i32)   — only for root/groups
      6: converted_type (i32) — optional
    """
    w = _ThriftWriter()
    if phys_type is not None:
        w.i32(1, phys_type)
    w.i32(3, rep_type)
    w.string(4, name)
    if num_children is not None:
        w.i32(5, num_children)
    if conv_type is not None:
        w.i32(6, conv_type)
    return w.stop()


def _enc_decimal_schema_element(name: str, rep_type: int,
                                 scale: int, precision: int) -> bytes:
    """
    SchemaElement for DECIMAL columns (FIXED_LEN_BYTE_ARRAY + DECIMAL annotation).

    Fields written (must be in ascending id order for Thrift compact):
      1: type          = FIXED_LEN_BYTE_ARRAY
      2: type_length   = 16  (decimal128)
      3: repetition_type
      4: name
      6: converted_type = DECIMAL
      7: scale
      8: precision
    """
    w = _ThriftWriter()
    w.i32(1, _T_FIXED_LEN_BYTE_ARRAY)
    w.i32(2, 16)
    w.i32(3, rep_type)
    w.string(4, name)
    w.i32(6, _CT_DECIMAL)
    w.i32(7, scale)
    w.i32(8, precision)
    return w.stop()


def _enc_data_page_header(num_values: int) -> bytes:
    """
    DataPageHeader fields:
      1: num_values (i32)
      2: encoding (i32)                = PLAIN
      3: definition_level_encoding (i32) = RLE
      4: repetition_level_encoding (i32) = RLE
    """
    w = _ThriftWriter()
    w.i32(1, num_values)
    w.i32(2, _ENC_PLAIN)
    w.i32(3, _ENC_RLE)
    w.i32(4, _ENC_RLE)
    return w.stop()


def _enc_page_header(uncompressed: int, compressed: int,
                      data_page_hdr: bytes) -> bytes:
    """
    PageHeader fields:
      1: type (i32)                  = DATA_PAGE
      2: uncompressed_page_size (i32)
      3: compressed_page_size (i32)
      5: data_page_header (struct)
    """
    w = _ThriftWriter()
    w.i32(1, _PAGE_DATA)
    w.i32(2, uncompressed)
    w.i32(3, compressed)
    w.struct(5, data_page_hdr)
    return w.stop()


def _enc_column_metadata(phys_type: int, conv_type, col_name: str,
                          codec: int, num_values: int,
                          total_uncompressed: int, total_compressed: int,
                          data_page_offset: int, nullable: bool) -> bytes:
    """
    ColumnMetaData fields:
      1: type (i32)
      2: encodings (list<i32>)
      3: path_in_schema (list<string>)
      4: codec (i32)
      5: num_values (i64)
      6: total_uncompressed_size (i64)
      7: total_compressed_size (i64)
      9: data_page_offset (i64)
    """
    # Document encodings used: always PLAIN for data; RLE for def levels if nullable
    encodings = [_ENC_PLAIN, _ENC_RLE] if nullable else [_ENC_PLAIN]
    w = _ThriftWriter()
    w.i32(1, phys_type)
    w.list_i32(2, encodings)
    w.list_str(3, [col_name])
    w.i32(4, codec)
    w.i64(5, num_values)
    w.i64(6, total_uncompressed)
    w.i64(7, total_compressed)
    w.i64(9, data_page_offset)
    return w.stop()


def _enc_column_chunk(meta_bytes: bytes, file_offset: int) -> bytes:
    """
    ColumnChunk fields:
      2: file_offset (i64)   — offset of first data page
      3: meta_data (struct)  — inline ColumnMetaData
    """
    w = _ThriftWriter()
    w.i64(2, file_offset)
    w.struct(3, meta_bytes)
    return w.stop()


def _enc_row_group(col_chunks: list, total_bytes: int, num_rows: int) -> bytes:
    """
    RowGroup fields:
      1: columns (list<ColumnChunk>)
      2: total_byte_size (i64)
      3: num_rows (i64)
    """
    w = _ThriftWriter()
    w.list_struct(1, col_chunks)
    w.i64(2, total_bytes)
    w.i64(3, num_rows)
    return w.stop()


def _enc_file_metadata(schema_elems: list, row_groups: list,
                        num_rows: int) -> bytes:
    """
    FileMetaData fields:
      1: version (i32)    = 2
      2: schema (list<SchemaElement>)
      3: num_rows (i64)
      4: row_groups (list<RowGroup>)
      6: created_by (string)
    """
    w = _ThriftWriter()
    w.i32(1, 2)
    w.list_struct(2, schema_elems)
    w.i64(3, num_rows)
    w.list_struct(4, row_groups)
    w.string(6, 'serif')
    return w.stop()


# ---------------------------------------------------------------------------
# Definition level encoding / decoding
# ---------------------------------------------------------------------------

def _encode_def_levels(null_flags: list) -> bytes:
    """
    Encode definition levels for a nullable column using RLE/bit-packing.

    serif uses a "null mask" convention: True = null.
    Parquet def_level convention:        1 = present, 0 = null.

    Layout: [4-byte LE length][run-header varint][packed bytes]
    bit_width = 1  (max def level = 1)
    """
    n = len(null_flags)
    if n == 0:
        return _struct.pack('<I', 0)

    num_groups = (n + 7) // 8
    packed = bytearray(num_groups)

    for i, is_null in enumerate(null_flags):
        if not is_null:
            # def_level = 1 (present): set the bit at position i
            packed[i >> 3] |= 1 << (i & 7)

    # Bit-packed run header: (num_groups << 1) | 1
    run_hdr = _varint_encode((num_groups << 1) | 1)
    encoded = run_hdr + bytes(packed)
    return _struct.pack('<I', len(encoded)) + encoded


def _decode_def_levels(data, pos: int, nrows: int):
    """
    Decode RLE/bit-packed definition levels.

    Returns (null_flags: list[bool], pos_after_levels).
    null_flags[i] = True  → value at row i is null.
    """
    length = _struct.unpack_from('<I', data, pos)[0]
    pos += 4
    end = pos + length
    null_flags: list = []

    while pos < end and len(null_flags) < nrows:
        header, pos = _varint_decode(data, pos)

        if header & 1:
            # Bit-packed: header >> 1 = number of 8-value groups
            num_groups = header >> 1
            for _ in range(num_groups):
                if pos >= end:
                    break
                byte = data[pos]; pos += 1
                for bit in range(8):
                    if len(null_flags) < nrows:
                        # def_level == 0 → null
                        null_flags.append(((byte >> bit) & 1) == 0)
        else:
            # RLE: header >> 1 = count, next byte = value
            count = header >> 1
            if pos >= end:
                break
            value = data[pos]; pos += 1
            is_null = (value == 0)
            remaining = nrows - len(null_flags)
            null_flags.extend([is_null] * min(count, remaining))

    return null_flags, end  # jump past entire encoded block


# ---------------------------------------------------------------------------
# PLAIN value encoding
# ---------------------------------------------------------------------------

_EPOCH_ORD  = _date(1970, 1, 1).toordinal()
_EPOCH_DT   = _datetime(1970, 1, 1)


def _encode_plain(values: list, kind: type, col_name: str,
                   decimal_scale: int = None) -> bytes:
    """Encode a list of non-null values as PLAIN bytes."""
    if not values:
        return b''

    if kind is bool:
        n = len(values)
        packed = bytearray((n + 7) // 8)
        for i, v in enumerate(values):
            if v:
                packed[i >> 3] |= 1 << (i & 7)
        return bytes(packed)

    if kind is int:
        # Values come from ArrayStorage('q') so are guaranteed i64-safe.
        import array as _array
        return _array.array('q', values).tobytes()

    if kind is float:
        import array as _array
        return _array.array('d', values).tobytes()

    if kind is str:
        parts = []
        for s in values:
            b = s.encode('utf-8')
            parts.append(_struct.pack('<I', len(b)))
            parts.append(b)
        return b''.join(parts)

    if kind is _date:
        import array as _array
        days = _array.array('i', [v.toordinal() - _EPOCH_ORD for v in values])
        return days.tobytes()

    if kind is _datetime:
        import array as _array

        def _micros(v):
            # Integer math on the timedelta components. total_seconds() is a
            # float and loses microsecond exactness once the magnitude gets
            # large (float64 spacing reaches 1µs within a few hundred years
            # of the epoch).
            d = v - _EPOCH_DT
            return (d.days * 86_400_000_000) + (d.seconds * 1_000_000) + d.microseconds

        us = _array.array('q', [_micros(v) for v in values])
        return us.tobytes()

    if kind is _Decimal:
        # Each value encodes as a 16-byte big-endian two's complement integer
        # (Parquet FIXED_LEN_BYTE_ARRAY DECIMAL, type_length=16).
        multiplier = _Decimal(10) ** decimal_scale
        buf = bytearray()
        for val in values:
            unscaled = int(
                (val * multiplier).to_integral_value(rounding=_ROUND_HALF_EVEN)
            )
            buf.extend(unscaled.to_bytes(16, 'big', signed=True))
        return bytes(buf)

    raise SerifTypeError(
        f"Column '{col_name}': unsupported type '{kind.__name__}' for Parquet PLAIN encoding"
    )


# ---------------------------------------------------------------------------
# PLAIN value decoding
# ---------------------------------------------------------------------------

def _decode_plain(data, pos: int, kind: type, phys_type: int,
                  count: int, conv_type=None):
    """
    Decode `count` non-null PLAIN-encoded values.
    Returns (values: list, new_pos).
    """
    if count == 0:
        return [], pos

    if kind is bool:
        byte_count = (count + 7) // 8
        raw = data[pos:pos + byte_count]
        pos += byte_count
        values = [bool((raw[i >> 3] >> (i & 7)) & 1) for i in range(count)]
        return values, pos

    if kind is int:
        if phys_type == _T_INT32:
            size = count * 4
            values = list(_struct.unpack_from(f'<{count}i', data, pos))
            return values, pos + size
        else:  # INT64 — frombytes: direct memcpy, zero boxing
            size = count * 8
            a = _pyarray.array('q')
            a.frombytes(data[pos:pos + size])
            return a, pos + size

    if kind is float:
        if phys_type == _T_FLOAT:
            size = count * 4
            values = list(_struct.unpack_from(f'<{count}f', data, pos))
            return values, pos + size
        else:  # DOUBLE — frombytes: direct memcpy, zero boxing
            size = count * 8
            a = _pyarray.array('d')
            a.frombytes(data[pos:pos + size])
            return a, pos + size

    if kind is str:
        values = []
        for _ in range(count):
            length = _struct.unpack_from('<I', data, pos)[0]
            pos += 4
            values.append(bytes(data[pos:pos + length]).decode('utf-8'))
            pos += length
        return values, pos

    if kind is _date:
        size = count * 4
        days_list = _struct.unpack_from(f'<{count}i', data, pos)
        values = [_date.fromordinal(_EPOCH_ORD + d) for d in days_list]
        return values, pos + size

    if kind is _datetime:
        size = count * 8
        raw = _struct.unpack_from(f'<{count}q', data, pos)
        if conv_type == _CT_TIMESTAMP_MILLIS:
            # Milliseconds since epoch
            values = [_EPOCH_DT + _timedelta(milliseconds=ms) for ms in raw]
        else:
            # Microseconds since epoch (our default)
            values = [_EPOCH_DT + _timedelta(microseconds=us) for us in raw]
        return values, pos + size

    raise SerifTypeError(
        f"Unsupported Parquet physical type {phys_type} (kind={kind}) for PLAIN decoding"
    )


# ---------------------------------------------------------------------------
# Column type resolution
# ---------------------------------------------------------------------------

def _col_parquet_type(col, col_name: str):
    """
    Inspect a Vector column and return
    (phys_type, conv_type_or_None, rep_type, decimal_scale_or_None, decimal_precision_or_None).

    Raises SerifTypeError for unsupported or ambiguous types.
    """
    schema = col.schema()
    if schema is None:
        raise SerifTypeError(
            f"Column '{col_name}': cannot write untyped (empty) column to Parquet. "
            "The column must contain at least one non-None value so its type can be inferred."
        )

    kind = schema.kind
    rep  = _REP_OPTIONAL if schema.nullable else _REP_REQUIRED

    if kind is bool:
        return _T_BOOLEAN, None, rep, None, None

    if kind is int:
        st = col._storage
        if not (isinstance(st, ArrayStorage) and st._data.typecode == 'q'):
            raise SerifTypeError(
                f"Column '{col_name}': int columns must be backed by ArrayStorage('q') "
                "(64-bit) to write as Parquet INT64. "
                "TupleStorage int values may exceed INT64 range. "
                "Create the column via arithmetic or an explicit typed Vector."
            )
        return _T_INT64, None, rep, None, None

    if kind is float:
        return _T_DOUBLE, None, rep, None, None

    if kind is str:
        return _T_BYTE_ARRAY, _CT_UTF8, rep, None, None

    if kind is _date:
        return _T_INT32, _CT_DATE, rep, None, None

    if kind is _datetime:
        return _T_INT64, _CT_TIMESTAMP_MICROS, rep, None, None

    if kind is _Decimal:
        st = col._storage
        if isinstance(st, DecimalStorage):
            scale     = st._scale
            precision = st._precision
        else:
            # TupleStorage: infer scale from the max decimal places across values
            non_nulls = [v for v in st if v is not None]
            if not non_nulls:
                raise SerifTypeError(
                    f"Column '{col_name}': empty Decimal column has no values "
                    "to determine scale and precision from."
                )
            scale     = max(-v.as_tuple().exponent for v in non_nulls
                           if v.as_tuple().exponent < 0) if any(
                               v.as_tuple().exponent < 0 for v in non_nulls) else 0
            # Digits needed: significant digits + any extra scale padding
            precision = max(
                len(v.as_tuple().digits) + max(0, scale + v.as_tuple().exponent)
                for v in non_nulls
            )
            precision = max(precision, 1)
        return _T_FIXED_LEN_BYTE_ARRAY, _CT_DECIMAL, rep, scale, precision

    if kind is object:
        raise SerifTypeError(
            f"Column '{col_name}': object (mixed-type) columns cannot be written to Parquet. "
            "Cast to a single homogeneous type first."
        )

    raise SerifTypeError(
        f"Column '{col_name}': unsupported type '{kind.__name__}' for Parquet. "
        "Supported: bool, int (ArrayStorage 'q'), float, str, date, datetime, Decimal."
    )


# ---------------------------------------------------------------------------
# write_parquet
# ---------------------------------------------------------------------------

def write_parquet(table, path: str) -> None:
    """
    Write a Table to a Parquet file (PLAIN encoding, UNCOMPRESSED).

    Parameters
    ----------
    table : Table
    path  : str | path-like

    Raises
    ------
    SerifTypeError
        If any column has an unsupported type (object, complex, etc.) or
        if an int column is not backed by ArrayStorage('q').
    """
    from ..table import Table as _Table
    if not isinstance(table, _Table):
        raise SerifTypeError("write_parquet expects a Table")

    nrows = len(table)
    ncols = len(table._storage)

    if ncols == 0:
        _write_empty_parquet(path, nrows)
        return

    # ------------------------------------------------------------------
    # 1. Validate all columns up front (fail before touching the file)
    # ------------------------------------------------------------------
    col_infos = []
    for idx, col in enumerate(table._storage):
        name = col._name if col._name is not None else f'col_{idx}'
        phys, conv, rep, dec_scale, dec_prec = _col_parquet_type(col, name)
        col_infos.append({
            'col':               col,
            'name':              name,
            'phys_type':         phys,
            'conv_type':         conv,
            'rep_type':          rep,
            'nullable':          rep == _REP_OPTIONAL,
            'kind':              col.schema().kind,
            'decimal_scale':     dec_scale,
            'decimal_precision': dec_prec,
        })

    # ------------------------------------------------------------------
    # 2. Build the file in a bytearray; track offsets as we go
    # ------------------------------------------------------------------
    buf = bytearray(_MAGIC)

    page_records = []

    for info in col_infos:
        col      = info['col']
        kind     = info['kind']
        nullable = info['nullable']
        n        = len(col)

        data_tuple = col._storage.to_tuple()

        # Separate nullability.
        # to_tuple() already returns None at null positions for both
        # ArrayStorage (via BitMask) and TupleStorage (inline None).
        if nullable:
            null_flags = [v is None for v in data_tuple]
            non_null   = [v for v in data_tuple if v is not None]
        else:
            null_flags = None
            non_null   = list(data_tuple)

        # Encode PLAIN values (non-null only)
        value_bytes = _encode_plain(non_null, kind, info['name'],
                                    info.get('decimal_scale'))

        # Build page body: [def_levels?][value_bytes]
        if nullable:
            page_body = _encode_def_levels(null_flags) + value_bytes
        else:
            page_body = value_bytes

        uncompressed = len(page_body)

        # Build page: PageHeader + body
        dph        = _enc_data_page_header(n)
        ph         = _enc_page_header(uncompressed, uncompressed, dph)
        full_page  = ph + page_body
        total_size = len(full_page)   # header + body, as required by spec

        # Record offset of this page's first byte BEFORE appending
        page_offset = len(buf)
        buf.extend(full_page)

        page_records.append({
            **info,
            'data_page_offset':  page_offset,
            'num_values':        n,
            'total_size':        total_size,
            'uncompressed_body': uncompressed,
        })

    # ------------------------------------------------------------------
    # 3. Build schema: root element + one leaf per column
    # ------------------------------------------------------------------
    schema_elems = [
        _enc_schema_element('schema', None, None, _REP_REQUIRED, num_children=ncols)
    ]
    for r in page_records:
        if r['kind'] is _Decimal:
            schema_elems.append(
                _enc_decimal_schema_element(r['name'], r['rep_type'],
                                             r['decimal_scale'], r['decimal_precision'])
            )
        else:
            schema_elems.append(
                _enc_schema_element(r['name'], r['phys_type'], r['conv_type'], r['rep_type'])
            )

    # ------------------------------------------------------------------
    # 4. Build column chunks and row group
    # ------------------------------------------------------------------
    col_chunk_bytes = []
    total_rg_bytes  = 0

    for r in page_records:
        meta = _enc_column_metadata(
            phys_type        = r['phys_type'],
            conv_type        = r['conv_type'],
            col_name         = r['name'],
            codec            = _CODEC_UNCOMPRESSED,
            num_values       = r['num_values'],
            total_uncompressed = r['total_size'],
            total_compressed   = r['total_size'],
            data_page_offset = r['data_page_offset'],
            nullable         = r['nullable'],
        )
        chunk = _enc_column_chunk(meta, r['data_page_offset'])
        col_chunk_bytes.append(chunk)
        total_rg_bytes += r['total_size']

    rg     = _enc_row_group(col_chunk_bytes, total_rg_bytes, nrows)
    footer = _enc_file_metadata(schema_elems, [rg], nrows)

    buf.extend(footer)
    buf.extend(_struct.pack('<I', len(footer)))
    buf.extend(_MAGIC)

    with open(path, 'wb') as f:
        f.write(buf)


def _write_empty_parquet(path: str, nrows: int) -> None:
    """Write a valid Parquet file with zero columns."""
    schema_elems = [
        _enc_schema_element('schema', None, None, _REP_REQUIRED, num_children=0)
    ]
    footer = _enc_file_metadata(schema_elems, [], nrows)
    buf = bytearray(_MAGIC)
    buf.extend(footer)
    buf.extend(_struct.pack('<I', len(footer)))
    buf.extend(_MAGIC)
    with open(path, 'wb') as f:
        f.write(buf)


# ---------------------------------------------------------------------------
# Thrift struct parsers for reading
# ---------------------------------------------------------------------------

def _parse_list(data, pos: int, elem_parser):
    """
    Parse a Thrift compact list.
    elem_parser(data, pos) → (item, new_pos)
    """
    b     = data[pos]; pos += 1
    count = (b >> 4) & 0x0F
    if count == 0x0F:
        count, pos = _varint_decode(data, pos)
    items = []
    for _ in range(count):
        item, pos = elem_parser(data, pos)
        items.append(item)
    return items, pos


def _parse_list_i32(data, pos: int):
    b     = data[pos]; pos += 1
    count = (b >> 4) & 0x0F
    if count == 0x0F:
        count, pos = _varint_decode(data, pos)
    vals = []
    for _ in range(count):
        v, pos = _dec_i32(data, pos)
        vals.append(v)
    return vals, pos


def _parse_list_str(data, pos: int):
    b     = data[pos]; pos += 1
    count = (b >> 4) & 0x0F
    if count == 0x0F:
        count, pos = _varint_decode(data, pos)
    vals = []
    for _ in range(count):
        s, pos = _dec_str(data, pos)
        vals.append(s)
    return vals, pos


# -- LogicalType (SchemaElement field 10) --

def _parse_timestamp_unit(data, pos: int):
    """
    TimestampType{1: bool isAdjustedToUTC, 2: TimeUnit unit}. TimeUnit is a
    union of EMPTY structs, so the inner field id IS the unit
    (1=MILLIS, 2=MICROS, 3=NANOS).
    """
    unit = None
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if fid == 2 and tc == _TC_STRUCT:
            inner_last = 0
            while True:
                ib = data[pos]; pos += 1
                if ib == 0:
                    break
                itc    = ib & 0x0F
                idelta = (ib >> 4) & 0x0F
                ifid   = (inner_last + idelta) if idelta else None
                if ifid is None:
                    ifid, pos = _dec_i32(data, pos)
                inner_last = ifid
                unit = ifid
                pos = _skip_field(data, pos, itc)
        else:
            pos = _skip_field(data, pos, tc)
    return unit, pos


def _parse_logical_type(data, pos: int):
    """
    SchemaElement.logicalType is a Thrift union of structs — exactly one
    field is present, and its field id says WHICH logical type this is.
    Payload structs are skipped, except TIMESTAMP's, whose unit changes how
    INT64 values decode.

    Returns ({'fid': int|None, 'timestamp_unit': int|None}, new_pos).
    """
    r = {'fid': None, 'timestamp_unit': None}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        r['fid'] = fid
        if fid == _LT_TIMESTAMP and tc == _TC_STRUCT:
            r['timestamp_unit'], pos = _parse_timestamp_unit(data, pos)
        else:
            pos = _skip_field(data, pos, tc)
    return r, pos


# -- SchemaElement --

def _parse_schema_element(data, pos: int):
    r = {'type': None, 'type_length': None, 'repetition_type': None, 'name': None,
         'num_children': None, 'converted_type': None, 'scale': None, 'precision': None,
         'logical_type': None}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1  and tc == _TC_I32:    r['type'],            pos = _dec_i32(data, pos)
        elif fid == 2  and tc == _TC_I32:    r['type_length'],     pos = _dec_i32(data, pos)
        elif fid == 3  and tc == _TC_I32:    r['repetition_type'], pos = _dec_i32(data, pos)
        elif fid == 4  and tc == _TC_BINARY: r['name'],            pos = _dec_str(data, pos)
        elif fid == 5  and tc == _TC_I32:    r['num_children'],    pos = _dec_i32(data, pos)
        elif fid == 6  and tc == _TC_I32:    r['converted_type'],  pos = _dec_i32(data, pos)
        elif fid == 7  and tc == _TC_I32:    r['scale'],           pos = _dec_i32(data, pos)
        elif fid == 8  and tc == _TC_I32:    r['precision'],       pos = _dec_i32(data, pos)
        elif fid == 10 and tc == _TC_STRUCT: r['logical_type'],    pos = _parse_logical_type(data, pos)
        else: pos = _skip_field(data, pos, tc)
    return r, pos


# -- ColumnMetaData --

def _parse_column_metadata(data, pos: int):
    r = {
        'type': None, 'encodings': [], 'path_in_schema': [],
        'codec': _CODEC_UNCOMPRESSED, 'num_values': 0,
        'total_uncompressed_size': 0, 'total_compressed_size': 0,
        'data_page_offset': 0, 'dictionary_page_offset': None,
    }
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1  and tc == _TC_I32:  r['type'],                    pos = _dec_i32(data, pos)
        elif fid == 2  and tc == _TC_LIST:  r['encodings'],               pos = _parse_list_i32(data, pos)
        elif fid == 3  and tc == _TC_LIST:  r['path_in_schema'],          pos = _parse_list_str(data, pos)
        elif fid == 4  and tc == _TC_I32:  r['codec'],                    pos = _dec_i32(data, pos)
        elif fid == 5  and tc == _TC_I64:  r['num_values'],               pos = _dec_i64(data, pos)
        elif fid == 6  and tc == _TC_I64:  r['total_uncompressed_size'],  pos = _dec_i64(data, pos)
        elif fid == 7  and tc == _TC_I64:  r['total_compressed_size'],    pos = _dec_i64(data, pos)
        elif fid == 9  and tc == _TC_I64:  r['data_page_offset'],         pos = _dec_i64(data, pos)
        elif fid == 11 and tc == _TC_I64:  r['dictionary_page_offset'],   pos = _dec_i64(data, pos)
        else: pos = _skip_field(data, pos, tc)
    return r, pos


# -- ColumnChunk --

def _parse_column_chunk(data, pos: int):
    r = {'file_offset': 0, 'meta_data': None}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 2 and tc == _TC_I64:    r['file_offset'], pos = _dec_i64(data, pos)
        elif fid == 3 and tc == _TC_STRUCT:  r['meta_data'],   pos = _parse_column_metadata(data, pos)
        else: pos = _skip_field(data, pos, tc)
    return r, pos


# -- RowGroup --

def _parse_row_group(data, pos: int):
    r = {'columns': [], 'total_byte_size': 0, 'num_rows': 0}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1 and tc == _TC_LIST:
            r['columns'], pos = _parse_list(data, pos, _parse_column_chunk)
        elif fid == 2 and tc == _TC_I64: r['total_byte_size'], pos = _dec_i64(data, pos)
        elif fid == 3 and tc == _TC_I64: r['num_rows'],        pos = _dec_i64(data, pos)
        else: pos = _skip_field(data, pos, tc)
    return r, pos


# -- FileMetaData --

def _parse_file_metadata(data, pos: int):
    r = {'version': None, 'schema': [], 'num_rows': 0, 'row_groups': []}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1 and tc == _TC_I32:
            r['version'],    pos = _dec_i32(data, pos)
        elif fid == 2 and tc == _TC_LIST:
            r['schema'],     pos = _parse_list(data, pos, _parse_schema_element)
        elif fid == 3 and tc == _TC_I64:
            r['num_rows'],   pos = _dec_i64(data, pos)
        elif fid == 4 and tc == _TC_LIST:
            r['row_groups'], pos = _parse_list(data, pos, _parse_row_group)
        else:
            pos = _skip_field(data, pos, tc)
    return r, pos


# -- PageHeader --

def _parse_data_page_header(data, pos: int):
    r = {'num_values': 0, 'encoding': _ENC_PLAIN,
         'definition_level_encoding': _ENC_RLE,
         'repetition_level_encoding': _ENC_RLE}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1 and tc == _TC_I32: r['num_values'],                   pos = _dec_i32(data, pos)
        elif fid == 2 and tc == _TC_I32: r['encoding'],                      pos = _dec_i32(data, pos)
        elif fid == 3 and tc == _TC_I32: r['definition_level_encoding'],     pos = _dec_i32(data, pos)
        elif fid == 4 and tc == _TC_I32: r['repetition_level_encoding'],     pos = _dec_i32(data, pos)
        else: pos = _skip_field(data, pos, tc)
    return r, pos


def _parse_page_header(data, pos: int):
    r = {'type': None, 'uncompressed_page_size': 0,
         'compressed_page_size': 0, 'data_page_header': None}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else None
        if fid is None:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1 and tc == _TC_I32:    r['type'],                  pos = _dec_i32(data, pos)
        elif fid == 2 and tc == _TC_I32:    r['uncompressed_page_size'], pos = _dec_i32(data, pos)
        elif fid == 3 and tc == _TC_I32:    r['compressed_page_size'],   pos = _dec_i32(data, pos)
        elif fid == 5 and tc == _TC_STRUCT: r['data_page_header'],       pos = _parse_data_page_header(data, pos)
        else: pos = _skip_field(data, pos, tc)
    return r, pos


# ---------------------------------------------------------------------------
# Schema element → Python type
# ---------------------------------------------------------------------------

_PHYS_TO_KIND = {
    _T_BOOLEAN:    bool,
    _T_INT32:      int,
    _T_INT64:      int,
    _T_FLOAT:      float,
    _T_DOUBLE:     float,
    _T_BYTE_ARRAY: str,
}

_CONV_TO_KIND = {
    _CT_UTF8:             str,
    _CT_DATE:             _date,
    _CT_TIMESTAMP_MILLIS: _datetime,
    _CT_TIMESTAMP_MICROS: _datetime,
    11: int, 12: int,                     # UINT_8 / UINT_16 — fit their
                                          # physical INT32 without sign issues
    15: int, 16: int, 17: int, 18: int,   # INT_8 .. INT_64
}

# ConvertedType codes the reader refuses: decoding any of these as raw
# physical values yields plausible-looking WRONG numbers (a DECIMAL's
# unscaled int, a UINT_64's sign-flipped negatives). Names for the error.
_CT_UNSUPPORTED = {
    1: 'MAP', 2: 'MAP_KEY_VALUE', 3: 'LIST', 4: 'ENUM',
    7: 'TIME_MILLIS', 8: 'TIME_MICROS', 13: 'UINT_32', 14: 'UINT_64',
    19: 'JSON', 20: 'BSON', 21: 'INTERVAL',
}


def _elem_to_kind(elem: dict):
    """
    Returns (kind, phys_type, conv_type). conv_type may be synthesized from
    the logical type (TIMESTAMP unit) so the decoder picks the right scale.

    Raises SerifTypeError for any annotation the reader can't decode
    faithfully — falling back to raw physical values would be a silent
    misread, and serif pukes instead.
    """
    name = elem.get('name')
    phys = elem.get('type')
    conv = elem.get('converted_type')
    lt   = elem.get('logical_type')

    if phys is None:
        raise SerifTypeError(
            f"Schema element '{name}' is a group (no physical type); "
            "nested schemas are not supported."
        )

    if conv is not None:
        if conv == _CT_DECIMAL:
            # DECIMAL: phys must be FIXED_LEN_BYTE_ARRAY (16 bytes = decimal128).
            # scale and precision live in the SchemaElement fields 7 and 8.
            if elem.get('type') != _T_FIXED_LEN_BYTE_ARRAY or elem.get('type_length') != 16:
                raise SerifTypeError(
                    f"Column '{name}': DECIMAL with physical type other than "
                    "FIXED_LEN_BYTE_ARRAY(16) is not supported (received "
                    f"type={elem.get('type')}, type_length={elem.get('type_length')}). "
                    "Only decimal128 (precision ≤ 38) columns can be read."
                )
            return _Decimal, _T_FIXED_LEN_BYTE_ARRAY, _CT_DECIMAL
        kind = _CONV_TO_KIND.get(conv)
        if kind is None:
            label = _CT_UNSUPPORTED.get(conv, f'code {conv}')
            raise SerifTypeError(
                f"Column '{name}': unsupported Parquet converted type {label}; "
                f"decoding it as raw physical values would be silently wrong. "
                f"Cannot read this column."
            )
        return kind, phys, conv

    if lt is not None and lt.get('fid') is not None:
        fid = lt['fid']
        if fid == _LT_STRING:
            return str, phys, _CT_UTF8
        if fid == 6:  # DATE
            return _date, phys, _CT_DATE
        if fid == _LT_TIMESTAMP:
            unit = lt.get('timestamp_unit')
            if unit == _TU_MILLIS:
                return _datetime, phys, _CT_TIMESTAMP_MILLIS
            if unit == _TU_MICROS:
                return _datetime, phys, _CT_TIMESTAMP_MICROS
            raise SerifTypeError(
                f"Column '{name}': TIMESTAMP(NANOS) is not supported — Python "
                f"datetimes hold microseconds, and truncating nanoseconds "
                f"would silently change the data. Re-write the file with "
                f"microsecond timestamps."
            )
        label = _LT_NAMES.get(fid, f'id {fid}')
        raise SerifTypeError(
            f"Column '{name}': unsupported Parquet logical type {label}; "
            f"decoding it as raw physical values would be silently wrong. "
            f"Cannot read this column."
        )

    kind = _PHYS_TO_KIND.get(phys)
    if kind is None:
        raise SerifTypeError(
            f"Column '{name}': unsupported Parquet physical type {phys}. "
            "Cannot read this column."
        )
    return kind, phys, conv


# ---------------------------------------------------------------------------
# Page decompression
# ---------------------------------------------------------------------------

def _decompress(page_bytes: bytes, codec: int, col_name: str) -> bytes:
    if codec == _CODEC_UNCOMPRESSED:
        return page_bytes
    if codec == _CODEC_GZIP:
        import zlib
        try:
            # wbits=47 = auto-detect gzip or zlib
            return zlib.decompress(page_bytes, wbits=47)
        except zlib.error:
            return zlib.decompress(page_bytes, wbits=15)
    if codec == _CODEC_SNAPPY:
        raise SerifValueError(
            f"Column '{col_name}': Snappy-compressed Parquet requires the "
            "'python-snappy' package. This reader supports UNCOMPRESSED (0) "
            "and GZIP (2) only."
        )
    raise SerifValueError(
        f"Column '{col_name}': unsupported compression codec {codec}. "
        "This reader supports UNCOMPRESSED (0) and GZIP (2) only."
    )


# ---------------------------------------------------------------------------
# Column chunk reader
# ---------------------------------------------------------------------------

def _decode_str_raw(page_body: bytes, body_pos: int,
                    null_flags, non_null_count: int) -> StringStorage:
    """
    Build a StringStorage directly from PLAIN BYTE_ARRAY page data.

    PLAIN BYTE_ARRAY format:  [4-byte LE length][utf-8 bytes] repeated.
    We scan the length prefixes to build the offset array and copy raw bytes
    into a contiguous buffer — zero .decode() calls.  Strings are decoded
    lazily only when StringStorage.__getitem__ is called.
    """
    buf_parts: list = []
    partial_offs: list = [0]   # byte offsets for the non-null values only
    pos = body_pos

    for _ in range(non_null_count):
        length = _struct.unpack_from('<I', page_body, pos)[0]
        pos += 4
        buf_parts.append(page_body[pos:pos + length])
        partial_offs.append(partial_offs[-1] + length)
        pos += length

    raw_buf = b''.join(buf_parts)

    if null_flags is None:
        # Non-nullable: partial_offs IS the full offset array
        return StringStorage.from_raw(raw_buf, _pyarray.array('I', partial_offs), None)

    # Nullable: expand partial_offs to cover every position (including nulls).
    # Null positions get a zero-length entry (duplicate offset).
    from .._vector.nullable import BitMask
    full_offs = [0]
    null_list = []
    has_nulls = False
    raw_idx   = 0

    for is_null in null_flags:
        if is_null:
            has_nulls = True
            null_list.append(True)
            full_offs.append(full_offs[-1])   # zero advance
        else:
            null_list.append(False)
            seg_len = partial_offs[raw_idx + 1] - partial_offs[raw_idx]
            full_offs.append(full_offs[-1] + seg_len)
            raw_idx += 1

    mask = BitMask.from_iterable(null_list) if has_nulls else None
    return StringStorage.from_raw(raw_buf, _pyarray.array('I', full_offs), mask)


def _decode_decimal_raw(page_body: bytes, body_pos: int,
                         null_flags, non_null_count: int,
                         scale: int, precision: int) -> DecimalStorage:
    """
    Build a DecimalStorage from PLAIN FIXED_LEN_BYTE_ARRAY page data.

    Each non-null value is exactly 16 bytes, big-endian two's complement —
    Parquet DECIMAL native format matches DecimalStorage._buf directly.
    """
    from .._vector.nullable import BitMask

    non_null_bytes = bytes(page_body[body_pos:body_pos + non_null_count * 16])

    if null_flags is None:
        return DecimalStorage.from_raw_be(non_null_bytes, scale, precision, None)

    # Expand: insert 16 zero bytes at each null position so _buf[i*16:(i+1)*16]
    # is always a valid (if sentinel) address regardless of nullability.
    buf       = bytearray()
    null_list: list[bool] = []
    has_nulls = False
    raw_idx   = 0

    for is_null in null_flags:
        if is_null:
            has_nulls = True
            null_list.append(True)
            buf.extend(b'\x00' * 16)
        else:
            buf.extend(non_null_bytes[raw_idx * 16:(raw_idx + 1) * 16])
            null_list.append(False)
            raw_idx += 1

    mask = BitMask.from_iterable(null_list) if has_nulls else None
    return DecimalStorage(buf, scale, precision, mask)


def _concat_decimal_storages(a: DecimalStorage, b: DecimalStorage) -> DecimalStorage:
    """Concatenate two DecimalStorages (multiple row groups)."""
    from .._vector.nullable import BitMask
    new_buf = a._buf + b._buf
    if a._mask is None and b._mask is None:
        new_mask = None
    else:
        a_flags = [a._mask.is_null(i) if a._mask else False for i in range(len(a))]
        b_flags = [b._mask.is_null(i) if b._mask else False for i in range(len(b))]
        all_flags = a_flags + b_flags
        new_mask = BitMask.from_iterable(all_flags) if any(all_flags) else None
    return DecimalStorage(new_buf, a._scale, a._precision, new_mask)


def _concat_string_storages(a: StringStorage, b: StringStorage) -> StringStorage:
    """Concatenate two StringStorages (multiple row groups)."""
    shift    = len(a._buf)
    new_buf  = a._buf + b._buf
    a_len    = len(a)
    b_len    = len(b)

    # a's offsets + b's offsets each shifted by len(a._buf)
    new_offs = _pyarray.array('I', a._offsets)
    for i in range(1, b_len + 1):
        new_offs.append(b._offsets[i] + shift)

    # Combine null masks
    if a._mask is None and b._mask is None:
        new_mask = None
    else:
        from .._vector.nullable import BitMask
        a_flags = [a._mask.is_null(i) if a._mask else False for i in range(a_len)]
        b_flags = [b._mask.is_null(i) if b._mask else False for i in range(b_len)]
        all_flags = a_flags + b_flags
        new_mask = BitMask.from_iterable(all_flags) if any(all_flags) else None

    return StringStorage.from_raw(new_buf, new_offs, new_mask)


def _read_column_chunk(file_data, cm: dict, kind: type, phys_type: int,
                        conv_type, is_optional: bool, col_name: str,
                        decimal_scale: int = None,
                        decimal_precision: int = None) -> list:
    """
    Read all data pages for one column chunk.
    Returns a flat list (None at null positions for optional columns).
    """
    codec          = cm.get('codec', _CODEC_UNCOMPRESSED)
    num_values     = cm['num_values']
    data_page_off  = cm['data_page_offset']

    # If the file has a dictionary page it sits before data_page_offset;
    # starting from data_page_offset skips it implicitly.
    pos = data_page_off

    values    = None   # None = not yet initialised; may become list or array.array
    remaining = num_values

    while remaining > 0:
        ph, pos = _parse_page_header(file_data, pos)

        page_type        = ph['type']
        compressed_size  = ph['compressed_page_size']
        dph              = ph.get('data_page_header') or {}
        page_num_values  = dph.get('num_values', remaining)
        encoding         = dph.get('encoding', _ENC_PLAIN)

        page_body = bytes(file_data[pos:pos + compressed_size])
        pos      += compressed_size

        if page_type == _PAGE_DICTIONARY:
            # Dictionary page — we can't decode RLE_DICTIONARY data pages,
            # but we skip the dict page itself.
            continue

        if page_type == _PAGE_DATA_V2:
            # V2 pages carry their def levels outside the (possibly
            # compressed) body and their header in field 8, which this
            # parser doesn't read. Skipping them would leave `remaining`
            # undecremented and walk the reader into garbage bytes.
            raise SerifValueError(
                f"Column '{col_name}': DataPage V2 pages are not supported "
                "by this reader. Re-write the file with data page version 1 "
                "(the default in most tools)."
            )

        if page_type != _PAGE_DATA:
            continue

        if encoding != _ENC_PLAIN:
            # RLE (3) here would mean RLE-encoded *values* (legal for
            # booleans) — decoding them as PLAIN bit-packing yields
            # well-formed garbage, so it raises like the dictionary
            # encodings do.
            raise SerifValueError(
                f"Column '{col_name}': unsupported data page value encoding "
                f"{encoding}. This reader supports PLAIN (0) only; dictionary "
                "(PLAIN_DICTIONARY=2 / RLE_DICTIONARY=8) and RLE-encoded "
                "boolean (RLE=3) pages cannot be read by this module."
            )

        page_body = _decompress(page_body, codec, col_name)

        body_pos   = 0
        null_flags = None

        if is_optional:
            null_flags, body_pos = _decode_def_levels(page_body, body_pos, page_num_values)

        non_null_count = (
            sum(1 for f in null_flags if not f)
            if null_flags is not None
            else page_num_values
        )

        if kind is str:
            # String fast path: build StringStorage directly from raw bytes.
            # No .decode() calls — offsets built from length prefixes, raw
            # UTF-8 bytes copied into a single contiguous buffer.
            page_storage = _decode_str_raw(page_body, body_pos, null_flags, non_null_count)
            if values is None:
                values = page_storage
            elif isinstance(values, StringStorage):
                values = _concat_string_storages(values, page_storage)
            else:
                values = list(values) if not isinstance(values, list) else values
                values.extend(page_storage)
        elif kind is _Decimal:
            # Decimal fast path: build DecimalStorage directly from raw bytes.
            # Each value is exactly 16 bytes big-endian — no boxing, no loop.
            page_storage = _decode_decimal_raw(
                page_body, body_pos, null_flags, non_null_count,
                decimal_scale, decimal_precision)
            if values is None:
                values = page_storage
            elif isinstance(values, DecimalStorage):
                values = _concat_decimal_storages(values, page_storage)
            else:
                values = list(values) if not isinstance(values, list) else values
                values.extend(page_storage)
        else:
            page_values, _ = _decode_plain(
                page_body, body_pos, kind, phys_type, non_null_count, conv_type
            )

            if null_flags is not None:
                it = iter(page_values)
                page_result = [None if f else next(it) for f in null_flags]
            else:
                page_result = page_values  # may be array.array (DOUBLE/INT64 fast path)

            # Accumulate — keep array.array alive to avoid boxing
            if values is None:
                values = (page_result
                          if isinstance(page_result, _pyarray.array)
                          else list(page_result))
            elif isinstance(values, _pyarray.array) and isinstance(page_result, _pyarray.array):
                values += page_result          # pure array concatenation, no boxing
            else:
                if isinstance(values, _pyarray.array):     # first page was fast, rest aren't
                    values = list(values)
                values.extend(page_result)
        remaining -= page_num_values

    return values if values is not None else []


# ---------------------------------------------------------------------------
# read_parquet
# ---------------------------------------------------------------------------

def read_parquet(path: str):
    """
    Read a Parquet file into a Table.

    Supports PLAIN encoding, UNCOMPRESSED and GZIP compression.
    Does not support: Snappy, LZ4, Zstd; dictionary encoding; nested schemas.
    Multiple row groups are concatenated in order.

    Parameters
    ----------
    path : str

    Returns
    -------
    Table
    """
    from ..table import Table
    from .._vector.dtype import Schema as _Schema

    if _USE_ARROW and _arrow_accel is not None:
        result = _arrow_accel.try_read(path)
        if result is not None:
            return result
        # Declined (unsupported column type, parse error, …): fall through
        # to the pure reader, whose errors are the ones users should see.

    with open(path, 'rb') as f:
        raw = f.read()

    data = memoryview(raw)
    n    = len(data)

    if n < 12:
        raise SerifValueError(f"'{path}' is too small to be a valid Parquet file")

    if bytes(data[:4]) != _MAGIC or bytes(data[n - 4:n]) != _MAGIC:
        raise SerifValueError(f"'{path}' is not a valid Parquet file (bad magic bytes)")

    footer_len   = _struct.unpack_from('<I', data, n - 8)[0]
    footer_start = n - 8 - footer_len

    if footer_start < 4 or footer_len <= 0:
        raise SerifValueError(
            f"'{path}': invalid Parquet footer length {footer_len}"
        )

    footer_bytes = bytes(data[footer_start:footer_start + footer_len])
    try:
        file_meta, _ = _parse_file_metadata(footer_bytes, 0)
    except (IndexError, _struct.error) as e:
        raise SerifValueError(
            f"'{path}': truncated or corrupt Parquet footer") from e

    schema_elems = file_meta.get('schema', [])
    row_groups   = file_meta.get('row_groups', [])

    if not schema_elems:
        raise SerifValueError(f"'{path}': empty or missing Parquet schema")

    # schema_elems[0] = root group element; [1:] = leaf columns
    leaf_schemas = [s for s in schema_elems[1:] if s.get('type') is not None]

    if not leaf_schemas:
        return Table(())

    # Resolve Python type for each leaf schema element
    col_meta = []
    for s in leaf_schemas:
        kind, phys_type, conv_type = _elem_to_kind(s)
        col_meta.append({
            'name':              s['name'],
            'kind':              kind,
            'phys_type':         phys_type,
            'conv_type':         conv_type,
            'is_optional':       s.get('repetition_type') == _REP_OPTIONAL,
            'decimal_scale':     s.get('scale'),
            'decimal_precision': s.get('precision'),
        })

    # Accumulate values per schema-leaf POSITION (not name): duplicate
    # column names are legal in both Parquet and serif (invariant #6), and
    # a name-keyed accumulator would silently merge them.
    col_values = [None] * len(col_meta)
    name_to_indices = {}
    for i, m in enumerate(col_meta):
        name_to_indices.setdefault(m['name'], []).append(i)

    for rg in row_groups:
        # Within a row group, the nth chunk bearing a name maps to the nth
        # schema leaf with that name, in order.
        seen_count = {}
        for cc in rg.get('columns', []):
            cm = cc.get('meta_data')
            if cm is None:
                continue

            path_parts = cm.get('path_in_schema', [])
            col_name   = path_parts[-1] if path_parts else None
            candidates = name_to_indices.get(col_name)
            if not candidates:
                continue
            nth = seen_count.get(col_name, 0)
            seen_count[col_name] = nth + 1
            if nth >= len(candidates):
                continue
            col_idx = candidates[nth]
            meta_entry = col_meta[col_idx]


            try:
                chunk_values = _read_column_chunk(
                    data,
                    cm,
                    kind              = meta_entry['kind'],
                    phys_type         = meta_entry['phys_type'],
                    conv_type         = meta_entry['conv_type'],
                    is_optional       = meta_entry['is_optional'],
                    col_name          = col_name,
                    decimal_scale     = meta_entry.get('decimal_scale'),
                    decimal_precision = meta_entry.get('decimal_precision'),
                )
            except (IndexError, _struct.error) as e:
                raise SerifValueError(
                    f"'{path}': truncated or corrupt Parquet data for "
                    f"column '{col_name}'") from e
            existing = col_values[col_idx]
            if existing is None:
                col_values[col_idx] = chunk_values
            elif isinstance(existing, StringStorage) and isinstance(chunk_values, StringStorage):
                col_values[col_idx] = _concat_string_storages(existing, chunk_values)
            elif isinstance(existing, DecimalStorage) and isinstance(chunk_values, DecimalStorage):
                col_values[col_idx] = _concat_decimal_storages(existing, chunk_values)
            elif isinstance(existing, _pyarray.array) and isinstance(chunk_values, _pyarray.array):
                col_values[col_idx] = existing + chunk_values
            else:
                if isinstance(existing, _pyarray.array):
                    col_values[col_idx] = list(existing)
                col_values[col_idx].extend(chunk_values)

    result_cols = []
    for col_idx, m in enumerate(col_meta):
        raw   = col_values[col_idx] or []
        dtype = _Schema(m['kind'], m['is_optional'])

        if isinstance(raw, StringStorage):
            # Str fast path: StringStorage already built with raw bytes,
            # no .decode() has occurred yet.
            col = Vector._from_storage(raw, dtype, name=m['name'])
        elif isinstance(raw, DecimalStorage):
            # Decimal fast path: DecimalStorage built directly from raw bytes.
            col = Vector._from_storage(raw, dtype, name=m['name'])
        elif isinstance(raw, _pyarray.array):
            # Non-nullable float or int: storage is already a packed C array.
            # Wrap it directly — zero extra iteration, zero boxing.
            storage = ArrayStorage(raw, None)
            col     = Vector._from_storage(storage, dtype, name=m['name'])
        else:
            col = Vector._from_iterable_known_dtype(raw, dtype, name=m['name'])

        result_cols.append(col)

    return Table._from_columns_nocopy(result_cols)
