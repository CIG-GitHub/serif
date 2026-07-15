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
from serif.errors import SerifValueError
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


# ---------------------------------------------------------------------------
# File assembly helpers
# ---------------------------------------------------------------------------

def _single_column_file(path, *, phys, conv=None, optional=False, page_body,
                        uncompressed_size=None, num_values,
                        codec=_CODEC_UNCOMPRESSED, meta_builder=None):
    """Assemble a one-column, one-row-group Parquet file byte by byte."""
    page_body = bytes(page_body)
    if uncompressed_size is None:
        uncompressed_size = len(page_body)

    buf = bytearray(_MAGIC)
    dph = _enc_data_page_header(num_values)
    ph = _enc_page_header(uncompressed_size, len(page_body), dph)
    page = ph + page_body
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
    schema = [
        _enc_schema_element('schema', None, None, _REP_REQUIRED, num_children=1),
        _enc_schema_element('x', phys, conv, rep),
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
        read_parquet(str(p))


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
    _concat_string_storages including null-mask combination."""
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
