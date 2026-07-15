"""
Parquet read/write for serif Tables.

This is a deliberately minimal, zero-dependency "round-trippable subset":

  WRITE — produces valid Parquet 2.x (PLAIN encoding, UNCOMPRESSED).
           Any Parquet reader (pyarrow, DuckDB, pandas, Spark) can consume it.

  READ  — reads Parquet written by this module; also handles PLAIN-encoded,
           UNCOMPRESSED or GZIP-compressed files from other tools.
           Snappy raises with an informative error.

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

from ..errors import SerifTypeError, SerifValueError
from .._vector import Vector
from .._vector.storage import ArrayStorage

# ---------------------------------------------------------------------------
# File-level constants
# ---------------------------------------------------------------------------

_MAGIC = b'PAR1'

# ---------------------------------------------------------------------------
# Parquet enum constants  (from parquet.thrift)
# ---------------------------------------------------------------------------

# Type (physical)
_T_BOOLEAN    = 0
_T_INT32      = 1
_T_INT64      = 2
_T_FLOAT      = 4
_T_DOUBLE     = 5
_T_BYTE_ARRAY = 6

# FieldRepetitionType
_REP_REQUIRED = 0
_REP_OPTIONAL = 1

# ConvertedType
_CT_UTF8             = 0
_CT_DATE             = 6
_CT_TIMESTAMP_MILLIS = 9
_CT_TIMESTAMP_MICROS = 10

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

# ---------------------------------------------------------------------------
# Thrift compact binary — type codes
# ---------------------------------------------------------------------------

_TC_BOOL_TRUE  = 1
_TC_BOOL_FALSE = 2
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
        last = last + delta if delta else (_dec_i32(data, pos)[0], (pos := _dec_i32(data, pos)[1]))[0]
        if delta == 0:
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


def _read_struct_fields(data, pos: int):
    """
    Generator: yields (field_id, type_code, pos_after_header).
    Caller must consume the field value and advance pos themselves — but
    since we can't do that here, we instead return a simple flat reader.

    Actually: return a dict-building function instead.
    """
    raise NotImplementedError  # Use _parse_* functions below


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


def _enc_data_page_header(num_values: int, nullable: bool) -> bytes:
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


def _encode_plain(values: list, kind: type, col_name: str) -> bytes:
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
        us = _array.array('q', [
            int((v - _EPOCH_DT).total_seconds() * 1_000_000) for v in values
        ])
        return us.tobytes()

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
    Inspect a Vector column and return (phys_type, conv_type_or_None, rep_type).
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
        return _T_BOOLEAN, None, rep

    if kind is int:
        st = col._storage
        if not (isinstance(st, ArrayStorage) and st._data.typecode == 'q'):
            raise SerifTypeError(
                f"Column '{col_name}': int columns must be backed by ArrayStorage('q') "
                "(64-bit) to write as Parquet INT64. "
                "TupleStorage int values may exceed INT64 range. "
                "Create the column via arithmetic or an explicit typed Vector."
            )
        return _T_INT64, None, rep

    if kind is float:
        return _T_DOUBLE, None, rep

    if kind is str:
        return _T_BYTE_ARRAY, _CT_UTF8, rep

    if kind is _date:
        return _T_INT32, _CT_DATE, rep

    if kind is _datetime:
        return _T_INT64, _CT_TIMESTAMP_MICROS, rep

    if kind is object:
        raise SerifTypeError(
            f"Column '{col_name}': object (mixed-type) columns cannot be written to Parquet. "
            "Cast to a single homogeneous type first."
        )

    raise SerifTypeError(
        f"Column '{col_name}': unsupported type '{kind.__name__}' for Parquet. "
        "Supported: bool, int (ArrayStorage 'q'), float, str, date, datetime."
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
        phys, conv, rep = _col_parquet_type(col, name)
        col_infos.append({
            'col':       col,
            'name':      name,
            'phys_type': phys,
            'conv_type': conv,
            'rep_type':  rep,
            'nullable':  rep == _REP_OPTIONAL,
            'kind':      col.schema().kind,
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
        # ArrayStorage (via ByteMask) and TupleStorage (inline None).
        if nullable:
            null_flags = [v is None for v in data_tuple]
            non_null   = [v for v in data_tuple if v is not None]
        else:
            null_flags = None
            non_null   = list(data_tuple)

        # Encode PLAIN values (non-null only)
        value_bytes = _encode_plain(non_null, kind, info['name'])

        # Build page body: [def_levels?][value_bytes]
        if nullable:
            page_body = _encode_def_levels(null_flags) + value_bytes
        else:
            page_body = value_bytes

        uncompressed = len(page_body)

        # Build page: PageHeader + body
        dph        = _enc_data_page_header(n, nullable)
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

def _parse_struct(data, pos: int, field_handlers: dict):
    """
    Generic Thrift compact struct reader.

    field_handlers: {field_id: callable(data, pos, type_code) → (value, new_pos)}
    Returns (result_dict, new_pos) where result_dict has the same keys.
    """
    result = {}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        if delta:
            fid = last + delta
        else:
            fid, pos = _dec_i32(data, pos)
        last = fid

        handler = field_handlers.get(fid)
        if handler is None:
            pos = _skip_field(data, pos, tc)
        else:
            val, pos = handler(data, pos, tc)
            result[fid] = val

    return result, pos


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


# -- SchemaElement --

def _parse_schema_element(data, pos: int):
    r = {'type': None, 'repetition_type': None, 'name': None,
         'num_children': None, 'converted_type': None}
    last = 0
    while True:
        b = data[pos]; pos += 1
        if b == 0:
            break
        tc    = b & 0x0F
        delta = (b >> 4) & 0x0F
        fid   = (last + delta) if delta else _dec_i32(data, pos)[0]
        if not delta:
            fid, pos = _dec_i32(data, pos)
        last = fid
        if   fid == 1 and tc == _TC_I32:    r['type'],            pos = _dec_i32(data, pos)
        elif fid == 3 and tc == _TC_I32:    r['repetition_type'], pos = _dec_i32(data, pos)
        elif fid == 4 and tc == _TC_BINARY: r['name'],            pos = _dec_str(data, pos)
        elif fid == 5 and tc == _TC_I32:    r['num_children'],    pos = _dec_i32(data, pos)
        elif fid == 6 and tc == _TC_I32:    r['converted_type'],  pos = _dec_i32(data, pos)
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
}


def _elem_to_kind(elem: dict):
    """
    Returns (kind, phys_type, conv_type).
    Raises SerifTypeError for unreadable types.
    """
    phys = elem.get('type')
    conv = elem.get('converted_type')

    if phys is None:
        raise SerifTypeError(
            f"Schema element '{elem.get('name')}' is a group (no physical type); "
            "nested schemas are not supported."
        )

    kind = _CONV_TO_KIND.get(conv) or _PHYS_TO_KIND.get(phys)
    if kind is None:
        raise SerifTypeError(
            f"Column '{elem.get('name')}': unsupported Parquet physical type {phys} "
            f"(converted_type={conv}). Cannot read this column."
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

def _read_column_chunk(file_data, cm: dict, kind: type, phys_type: int,
                        conv_type, is_optional: bool, col_name: str) -> list:
    """
    Read all data pages for one column chunk.
    Returns a flat list (None at null positions for optional columns).
    """
    codec          = cm.get('codec', _CODEC_UNCOMPRESSED)
    num_values     = cm['num_values']
    data_page_off  = cm['data_page_offset']
    dict_page_off  = cm.get('dictionary_page_offset')

    # Skip dictionary page if present (we don't support dictionary encoding,
    # but we can skip over it gracefully if the data pages are PLAIN)
    pos = data_page_off

    # If the file has a dictionary page before the data page, it would be at
    # dict_page_off. We start reading from data_page_off so it's fine.

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

        if page_type != _PAGE_DATA:
            continue

        if encoding not in (_ENC_PLAIN, _ENC_RLE):
            raise SerifValueError(
                f"Column '{col_name}': unsupported data page encoding {encoding}. "
                "This reader supports PLAIN (0) only. "
                "Files using dictionary (RLE_DICTIONARY=8) encoding cannot be read "
                "by this module."
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
    file_meta, _ = _parse_file_metadata(footer_bytes, 0)

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
            'name':        s['name'],
            'kind':        kind,
            'phys_type':   phys_type,
            'conv_type':   conv_type,
            'is_optional': s.get('repetition_type') == _REP_OPTIONAL,
        })

    # Accumulate values per column across all row groups
    col_values = {m['name']: None for m in col_meta}

    for rg in row_groups:
        for cc in rg.get('columns', []):
            cm = cc.get('meta_data')
            if cm is None:
                continue

            path_parts = cm.get('path_in_schema', [])
            col_name   = path_parts[-1] if path_parts else None
            if col_name not in col_values:
                continue

            meta_entry = next((m for m in col_meta if m['name'] == col_name), None)
            if meta_entry is None:
                continue

            chunk_values = _read_column_chunk(
                data,
                cm,
                kind       = meta_entry['kind'],
                phys_type  = meta_entry['phys_type'],
                conv_type  = meta_entry['conv_type'],
                is_optional= meta_entry['is_optional'],
                col_name   = col_name,
            )
            existing = col_values[col_name]
            if existing is None:
                col_values[col_name] = chunk_values
            elif isinstance(existing, _pyarray.array) and isinstance(chunk_values, _pyarray.array):
                col_values[col_name] = existing + chunk_values
            else:
                if isinstance(existing, _pyarray.array):
                    col_values[col_name] = list(existing)
                col_values[col_name].extend(chunk_values)

    result_cols = []
    for m in col_meta:
        raw   = col_values[m['name']] or []
        dtype = _Schema(m['kind'], m['is_optional'])

        if isinstance(raw, _pyarray.array):
            # Non-nullable float or int: storage is already a packed C array.
            # Wrap it directly — zero extra iteration, zero boxing.
            storage = ArrayStorage(raw, None)
            col     = Vector._from_storage(storage, dtype, name=m['name'])
        else:
            col = Vector._from_iterable_known_dtype(raw, dtype, name=m['name'])

        result_cols.append(col)

    return Table._from_columns_nocopy(result_cols)
