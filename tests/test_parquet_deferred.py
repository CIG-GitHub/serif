"""Footer-backed Parquet materialization and mask pushdown."""

import struct
from array import array

import pytest

from serif import Table, read_parquet
from serif.errors import SerifValueError
from serif.table import MaskedTable
import serif.io.parquet as parquet
from serif.io.parquet import (
    _MAGIC,
    _CODEC_UNCOMPRESSED,
    _REP_REQUIRED,
    _T_INT64,
    _enc_column_chunk,
    _enc_column_metadata,
    _enc_data_page_header,
    _enc_file_metadata,
    _enc_page_header,
    _enc_row_group,
    _enc_schema_element,
)


@pytest.fixture(autouse=True)
def _pure_reader(monkeypatch):
    monkeypatch.setattr(parquet, '_USE_ARROW', False)


def _write_table(path):
    Table({
        'a': [1, 2, 3, 4],
        'b': [10, 20, 30, 40],
        's': ['w', 'x', 'y', 'z'],
    }).to_parquet(str(path))


def _write_two_row_groups(path):
    buf = bytearray(_MAGIC)
    row_groups = []
    for a_values, b_values in (([1, 2], [10, 20]), ([3, 4], [30, 40])):
        chunks = []
        total = 0
        for name, values in (('a', a_values), ('b', b_values)):
            body = array('q', values).tobytes()
            header = _enc_page_header(
                len(body), len(body), _enc_data_page_header(len(values)))
            page = header + body
            offset = len(buf)
            buf.extend(page)
            meta = _enc_column_metadata(
                _T_INT64, None, name, _CODEC_UNCOMPRESSED,
                len(values), len(page), len(page), offset, False)
            chunks.append(_enc_column_chunk(meta, offset))
            total += len(page)
        row_groups.append(_enc_row_group(chunks, total, len(a_values)))

    schema = [
        _enc_schema_element(
            'schema', None, None, _REP_REQUIRED, num_children=2),
        _enc_schema_element('a', _T_INT64, None, _REP_REQUIRED),
        _enc_schema_element('b', _T_INT64, None, _REP_REQUIRED),
    ]
    footer = _enc_file_metadata(schema, row_groups, 4)
    buf.extend(footer)
    buf.extend(struct.pack('<I', len(footer)))
    buf.extend(_MAGIC)
    path.write_bytes(bytes(buf))


def test_read_is_footer_only_and_schema_view_is_free(tmp_path, monkeypatch):
    path = tmp_path / 'table.parquet'
    _write_table(path)

    def forbidden(*args, **kwargs):
        raise AssertionError('column data was read')

    monkeypatch.setattr(parquet._ParquetSource, 'load_column', forbidden)
    result = read_parquet(path)

    assert isinstance(result, Table)
    assert type(result) is parquet._ParquetTable
    assert len(result) == 4
    assert result.shape == (4, 3)
    assert result.column_names() == ['a', 'b', 's']
    assert '.a' in repr(result._)
    assert result._mat is None
    assert result._gathered == {}


def test_column_access_loads_once_and_does_not_latch(tmp_path, monkeypatch):
    path = tmp_path / 'table.parquet'
    _write_table(path)
    calls = []
    original = parquet._ParquetSource.load_column

    def spy(self, idx, mask=None):
        calls.append((idx, mask))
        return original(self, idx, mask)

    monkeypatch.setattr(parquet._ParquetSource, 'load_column', spy)
    result = read_parquet(path)

    assert list(result.b) == [10, 20, 30, 40]
    assert result.b is result.b
    assert calls == [(1, None)]
    assert result._mat is None
    assert set(result._gathered) == {1}


def test_boolean_mask_reaches_remaining_parquet_columns(tmp_path, monkeypatch):
    path = tmp_path / 'table.parquet'
    _write_table(path)
    calls = []
    original = parquet._ParquetSource.load_column

    def spy(self, idx, mask=None):
        calls.append((idx, mask is not None))
        return original(self, idx, mask)

    monkeypatch.setattr(parquet._ParquetSource, 'load_column', spy)
    source = read_parquet(path)
    selected = source[source.a > 2]

    assert type(selected) is MaskedTable
    assert list(selected.b) == [30, 40]
    assert calls == [(0, False), (1, True)]
    assert source._mat is None
    assert selected._mat is None
    assert set(selected._gathered) == {1}


def test_pure_mask_filter_does_not_require_numpy(tmp_path, monkeypatch):
    path = tmp_path / 'table.parquet'
    Table({
        'a': [1, 2, 3, 4],
        'b': [None, 20, None, 40],
    }).to_parquet(str(path))
    monkeypatch.setattr(parquet, '_accel_filter', lambda storage, mask: None)

    source = read_parquet(path)
    selected = source[source.a > 1]

    assert list(selected.b) == [20, None, 40]
    assert source._mat is None
    assert selected._mat is None


def test_all_false_row_group_is_not_decoded_for_payload(tmp_path, monkeypatch):
    path = tmp_path / 'groups.parquet'
    _write_two_row_groups(path)
    decoded = []
    original = parquet._read_column_chunk

    def spy(*args, **kwargs):
        decoded.append(kwargs['col_name'])
        return original(*args, **kwargs)

    monkeypatch.setattr(parquet, '_read_column_chunk', spy)
    source = read_parquet(path)
    selected = source[source.a > 2]
    assert decoded == ['a', 'a']

    decoded.clear()
    assert list(selected.b) == [30, 40]
    assert decoded == ['b']


def test_mask_capture_survives_source_table_mutation(tmp_path):
    path = tmp_path / 'table.parquet'
    _write_table(path)
    source = read_parquet(path)
    selected = source[source.a > 2]

    source[2, 'b'] = 999

    assert list(source.b) == [10, 20, 999, 40]
    assert list(selected.b) == [30, 40]


def test_changed_file_raises_instead_of_reading_different_data(tmp_path):
    path = tmp_path / 'table.parquet'
    _write_table(path)
    source = read_parquet(path)
    path.write_bytes(path.read_bytes() + b'changed')

    with pytest.raises(SerifValueError, match='changed after read_parquet'):
        list(source.a)
