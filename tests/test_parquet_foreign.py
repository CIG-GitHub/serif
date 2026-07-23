"""
Parquet reader vs FOREIGN files.

serif's own writer emits PLAIN + UNCOMPRESSED + one row group + INT64/DOUBLE
physicals only. Files from other tools (pyarrow, DuckDB, Spark) legally
contain things the reader claims to support but the round-trip suite never
exercises: GZIP pages, INT32/FLOAT physicals, TIMESTAMP_MILLIS, multiple row
groups, and metadata fields we don't know about (statistics etc.) that must
be skipped correctly.

serif is zero-dependency, so these tests hand-assemble such files with the
module's own low-level Thrift encoders rather than importing pyarrow. That
covers the reader paths; genuine pyarrow-written fixture files can be added
on top later without changing any of these tests.
"""

import gzip
import struct
from array import array
from datetime import datetime, timedelta

import pytest

from serif import Table, read_parquet, write_parquet
from serif.errors import SerifTypeError, SerifValueError
from serif.io.parquet import (
    _MAGIC,
    _CODEC_GZIP,
    _CODEC_SNAPPY,
    _CODEC_UNCOMPRESSED,
    _CT_TIMESTAMP_MILLIS,
    _CT_UTF8,
    _ENC_PLAIN,
    _REP_OPTIONAL,
    _REP_REQUIRED,
    _T_BOOLEAN,
    _T_BYTE_ARRAY,
    _T_DOUBLE,
    _T_FLOAT,
    _T_INT32,
    _T_INT64,
    _ThriftWriter,
    _enc_column_chunk,
    _enc_column_metadata,
    _enc_data_page_header,
    _enc_file_metadata,
    _enc_page_header,
    _enc_row_group,
    _enc_schema_element,
    _encode_def_levels,
)

_EPOCH = datetime(1970, 1, 1)


@pytest.fixture(autouse=True)
def _force_pure_reader(monkeypatch):
    """This suite tests serif's OWN decoder. With pyarrow installed, the
    optional accelerator would route supported files around it (legal
    transport widening — e.g. it reads DataPage V2 fine), silently changing
    what these tests exercise. Pin the pure path so coverage is
    deterministic regardless of environment."""
    import serif.io.parquet as parquet_mod
    monkeypatch.setattr(parquet_mod, '_USE_ARROW', False)


# ---------------------------------------------------------------------------
# File assembly helpers
# ---------------------------------------------------------------------------

def _single_column_file(path, *, phys, conv=None, optional=False, page_body,
                        uncompressed_size=None, num_values,
                        codec=_CODEC_UNCOMPRESSED, meta_builder=None,
                        page_header=None, schema_leaf=None):
    """Assemble a one-column, one-row-group Parquet file byte by byte."""
    page_body = bytes(page_body)
    if uncompressed_size is None:
        uncompressed_size = len(page_body)

    buf = bytearray(_MAGIC)
    if page_header is None:
        dph = _enc_data_page_header(num_values)
        page_header = _enc_page_header(uncompressed_size, len(page_body), dph)
    page = page_header + page_body
    offset = len(buf)
    buf += page

    if meta_builder is None:
        meta = _enc_column_metadata(phys, conv, 'x', codec, num_values,
                                    len(page), len(page), offset, optional)
    else:
        meta = meta_builder(offset, len(page))

    chunk = _enc_column_chunk(meta, offset)
    rg = _enc_row_group([chunk], len(page), num_values)

    rep = _REP_OPTIONAL if optional else _REP_REQUIRED
    if schema_leaf is None:
        schema_leaf = _enc_schema_element('x', phys, conv, rep)
    schema = [
        _enc_schema_element('schema', None, None, _REP_REQUIRED, num_children=1),
        schema_leaf,
    ]
    footer = _enc_file_metadata(schema, [rg], num_values)
    buf += footer
    buf += struct.pack('<I', len(footer))
    buf += _MAGIC
    path.write_bytes(bytes(buf))


def _string_page(values):
    """PLAIN BYTE_ARRAY page body for an OPTIONAL column: def levels + data."""
    null_flags = [v is None for v in values]
    parts = []
    for v in values:
        if v is not None:
            b = v.encode('utf-8')
            parts.append(struct.pack('<I', len(b)) + b)
    return _encode_def_levels(null_flags) + b''.join(parts)


# ---------------------------------------------------------------------------
# Physical / converted types serif's writer never emits
# ---------------------------------------------------------------------------

def test_int32_physical_reads_as_int(tmp_path):
    p = tmp_path / "i32.parquet"
    _single_column_file(p, phys=_T_INT32, num_values=2,
                        page_body=struct.pack('<2i', -5, 7))
    t = read_parquet(str(p))
    assert list(t['x']) == [-5, 7]
    assert t['x'].schema().kind is int


def test_float32_physical_reads_as_float(tmp_path):
    p = tmp_path / "f32.parquet"
    _single_column_file(p, phys=_T_FLOAT, num_values=2,
                        page_body=struct.pack('<2f', 1.5, -2.25))
    t = read_parquet(str(p))
    assert list(t['x']) == [1.5, -2.25]
    assert t['x'].schema().kind is float


def test_timestamp_millis_reads_as_datetime(tmp_path):
    dt = datetime(2024, 1, 2, 3, 4, 5, 678000)
    ms = (dt - _EPOCH) // timedelta(milliseconds=1)
    p = tmp_path / "ms.parquet"
    _single_column_file(p, phys=_T_INT64, conv=_CT_TIMESTAMP_MILLIS,
                        num_values=1, page_body=array('q', [ms]).tobytes())
    t = read_parquet(str(p))
    assert list(t['x']) == [dt]


def test_signed_int_converted_type_reads_as_int(tmp_path):
    p = tmp_path / "i32conv.parquet"
    _single_column_file(p, phys=_T_INT32, conv=17,  # INT_32
                        num_values=2, page_body=struct.pack('<2i', -5, 7))
    t = read_parquet(str(p))
    assert list(t['x']) == [-5, 7]


# ---------------------------------------------------------------------------
# Annotations that must RAISE, not misread. Decoding these as raw physical
# values yields plausible-looking wrong numbers — serif pukes instead.
# ---------------------------------------------------------------------------

def test_decimal_converted_type_raises(tmp_path):
    # DECIMAL(9,2) stored as INT32: raw 12345 means 123.45. Reading it as
    # the int 12345 would be silently wrong.
    p = tmp_path / "dec.parquet"
    _single_column_file(p, phys=_T_INT32, conv=5,  # DECIMAL
                        num_values=1, page_body=struct.pack('<i', 12345))
    with pytest.raises(SerifTypeError, match='DECIMAL'):
        read_parquet(str(p))


def test_uint64_converted_type_raises(tmp_path):
    # UINT_64 values with the high bit set decode as negative INT64s.
    p = tmp_path / "u64.parquet"
    _single_column_file(p, phys=_T_INT64, conv=14,  # UINT_64
                        num_values=1, page_body=array('q', [-1]).tobytes())
    with pytest.raises(SerifTypeError, match='UINT_64'):
        read_parquet(str(p))


# ---------------------------------------------------------------------------
# LogicalType (SchemaElement field 10) — the newer annotation union. Some
# types (nanosecond timestamps, UUID) exist ONLY here; without parsing it
# the reader would surface them as raw physical values.
# ---------------------------------------------------------------------------

def _leaf_with_logical_type(name, phys, lt_fid, ts_unit=None):
    """SchemaElement annotated ONLY via logicalType — no converted_type."""
    w = _ThriftWriter()
    w.i32(1, phys)
    w.i32(3, _REP_REQUIRED)
    w.string(4, name)
    lt = _ThriftWriter()
    if ts_unit is not None:
        ts = _ThriftWriter()
        ts.bool_(1, True)                # isAdjustedToUTC
        unit = _ThriftWriter()
        unit.struct(ts_unit, b'\x00')    # TimeUnit union: empty struct at fid=unit
        ts.struct(2, unit.stop())
        lt.struct(lt_fid, ts.stop())
    else:
        lt.struct(lt_fid, b'\x00')       # empty payload struct
    w.struct(10, lt.stop())
    return w.stop()


def test_logical_timestamp_micros_reads_as_datetime(tmp_path):
    dt = datetime(2024, 6, 1, 12, 30, 45, 123456)
    us = (dt - _EPOCH) // timedelta(microseconds=1)
    leaf = _leaf_with_logical_type('x', _T_INT64, 8, ts_unit=2)  # TIMESTAMP(MICROS)
    p = tmp_path / "lt_us.parquet"
    _single_column_file(p, phys=_T_INT64, num_values=1,
                        page_body=array('q', [us]).tobytes(), schema_leaf=leaf)
    t = read_parquet(str(p))
    assert list(t['x']) == [dt]


def test_logical_timestamp_nanos_raises(tmp_path):
    # Python datetimes hold microseconds; truncating nanos would silently
    # change the data.
    leaf = _leaf_with_logical_type('x', _T_INT64, 8, ts_unit=3)  # TIMESTAMP(NANOS)
    p = tmp_path / "lt_ns.parquet"
    _single_column_file(p, phys=_T_INT64, num_values=1,
                        page_body=array('q', [1]).tobytes(), schema_leaf=leaf)
    with pytest.raises(SerifTypeError, match='NANOS'):
        read_parquet(str(p))


def test_unknown_logical_type_raises(tmp_path):
    leaf = _leaf_with_logical_type('x', _T_INT64, 14)  # UUID
    p = tmp_path / "lt_uuid.parquet"
    _single_column_file(p, phys=_T_INT64, num_values=1,
                        page_body=array('q', [1]).tobytes(), schema_leaf=leaf)
    with pytest.raises(SerifTypeError, match='UUID'):
        read_parquet(str(p))


# ---------------------------------------------------------------------------
# Page formats that must raise, not walk into garbage
# ---------------------------------------------------------------------------

def test_data_page_v2_raises(tmp_path):
    # V2 pages don't decrement `remaining` if skipped — the reader would
    # parse the bytes after the page as page headers.
    raw = array('q', [1, 2]).tobytes()
    w = _ThriftWriter()
    w.i32(1, 3)              # PageType DATA_PAGE_V2
    w.i32(2, len(raw))
    w.i32(3, len(raw))
    p = tmp_path / "v2.parquet"
    _single_column_file(p, phys=_T_INT64, num_values=2, page_body=raw,
                        page_header=w.stop())
    with pytest.raises(SerifValueError, match='V2'):
        list(read_parquet(str(p))['x'])


def test_rle_value_encoding_raises(tmp_path):
    # encoding=RLE in DataPageHeader field 2 means RLE-encoded VALUES
    # (legal for booleans); decoding them as PLAIN bit-packing would yield
    # well-formed garbage booleans.
    dph = _ThriftWriter()
    dph.i32(1, 8)            # num_values
    dph.i32(2, 3)            # encoding = RLE
    dph.i32(3, 3)
    dph.i32(4, 3)
    body = b'\x00'
    ph = _enc_page_header(len(body), len(body), dph.stop())
    p = tmp_path / "rle.parquet"
    _single_column_file(p, phys=_T_BOOLEAN, num_values=8, page_body=body,
                        page_header=ph)
    with pytest.raises(SerifValueError, match='RLE'):
        list(read_parquet(str(p))['x'])


def test_truncated_footer_raises_serif_error(tmp_path):
    # A garbage footer must surface as SerifValueError, not IndexError.
    p = tmp_path / "trunc.parquet"
    footer = b'\x1c'  # struct-field header promising content that isn't there
    p.write_bytes(_MAGIC + footer + struct.pack('<I', len(footer)) + _MAGIC)
    with pytest.raises(SerifValueError, match='footer'):
        read_parquet(str(p))


# ---------------------------------------------------------------------------
# Compression codecs
# ---------------------------------------------------------------------------

def test_gzip_compressed_column(tmp_path):
    raw = array('d', [1.5, 2.5, -3.25]).tobytes()
    p = tmp_path / "gz.parquet"
    _single_column_file(p, phys=_T_DOUBLE, num_values=3,
                        page_body=gzip.compress(raw),
                        uncompressed_size=len(raw), codec=_CODEC_GZIP)
    t = read_parquet(str(p))
    assert list(t['x']) == [1.5, 2.5, -3.25]


def test_snappy_raises_informative_error(tmp_path):
    raw = array('d', [1.0]).tobytes()
    p = tmp_path / "snappy.parquet"
    _single_column_file(p, phys=_T_DOUBLE, num_values=1,
                        page_body=raw, codec=_CODEC_SNAPPY)
    with pytest.raises(SerifValueError, match='[Ss]nappy'):
        list(read_parquet(str(p))['x'])


# ---------------------------------------------------------------------------
# Unknown metadata fields must be skipped, including long-form field headers
# ---------------------------------------------------------------------------

def test_unknown_struct_field_with_long_form_header_is_skipped(tmp_path):
    """Foreign writers put Statistics (and newer fields) in ColumnMetaData.
    The parser must skip structs it doesn't know — including fields whose
    id delta exceeds 15, which Thrift encodes in long form. A long-form id
    inside a skipped struct used to desync the whole footer parse."""
    values = array('q', [1, 2, 3]).tobytes()

    def meta_with_stats(offset, page_len):
        w = _ThriftWriter()
        w.i32(1, _T_INT64)
        w.list_i32(2, [_ENC_PLAIN])
        w.list_str(3, ['x'])
        w.i32(4, _CODEC_UNCOMPRESSED)
        w.i64(5, 3)
        w.i64(6, page_len)
        w.i64(7, page_len)
        w.i64(9, offset)
        stats = _ThriftWriter()
        stats.i64(20, 42)            # field id 20 → long-form header
        w.struct(12, stats.stop())   # fid 12: unknown to the parser → skipped
        return w.stop()

    p = tmp_path / "stats.parquet"
    _single_column_file(p, phys=_T_INT64, num_values=3, page_body=values,
                        meta_builder=meta_with_stats)
    t = read_parquet(str(p))
    assert list(t['x']) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Multiple row groups
# ---------------------------------------------------------------------------

def test_multi_row_group_strings_with_nulls(tmp_path):
    """Two row groups on one nullable string column — exercises
    storage-owned concatenation including null-mask combination."""
    group1 = ['aa', None, 'b']
    group2 = ['ccc', None]

    buf = bytearray(_MAGIC)
    row_groups = []
    for values in (group1, group2):
        body = _string_page(values)
        dph = _enc_data_page_header(len(values))
        ph = _enc_page_header(len(body), len(body), dph)
        page = ph + body
        offset = len(buf)
        buf += page
        meta = _enc_column_metadata(_T_BYTE_ARRAY, _CT_UTF8, 'x',
                                    _CODEC_UNCOMPRESSED, len(values),
                                    len(page), len(page), offset, True)
        chunk = _enc_column_chunk(meta, offset)
        row_groups.append(_enc_row_group([chunk], len(page), len(values)))

    schema = [
        _enc_schema_element('schema', None, None, _REP_REQUIRED, num_children=1),
        _enc_schema_element('x', _T_BYTE_ARRAY, _CT_UTF8, _REP_OPTIONAL),
    ]
    footer = _enc_file_metadata(schema, row_groups, len(group1) + len(group2))
    buf += footer
    buf += struct.pack('<I', len(footer))
    buf += _MAGIC

    p = tmp_path / "two_groups.parquet"
    p.write_bytes(bytes(buf))

    t = read_parquet(str(p))
    assert list(t['x']) == ['aa', None, 'b', 'ccc', None]
    assert t['x'].schema().kind is str
    assert t['x'].schema().nullable is True


# ---------------------------------------------------------------------------
# Datetime microsecond exactness (round-trip through serif's own writer)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dt", [
    datetime(2026, 7, 15, 23, 59, 59, 999999),
    datetime(2200, 6, 15, 12, 34, 56, 123457),   # float µs math loses this one
    datetime(1969, 12, 31, 23, 59, 59, 999999),  # pre-epoch, negative micros
])
def test_datetime_microsecond_exact_roundtrip(tmp_path, dt):
    p = tmp_path / "ts.parquet"
    write_parquet(Table({'ts': [dt, dt]}), str(p))
    result = read_parquet(str(p))
    assert list(result['ts']) == [dt, dt]
