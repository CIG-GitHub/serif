"""
Round-trip tests for serif's Parquet read/write implementation.

Coverage targets:
- All supported physical types: str, float, bool, int, date, datetime
- Non-nullable (REQUIRED) and nullable (OPTIONAL) variants of each
- Edge cases: all-null column, single row, single value, empty strings, unicode
- Null position fidelity: None lands back in exactly the right slot
- Error cases: unsupported types raise SerifTypeError cleanly
"""
import os
import tempfile
from array import array
from datetime import date, datetime
from decimal import Decimal

import pytest

import serif.io.parquet as parquet_mod
from serif import Table, Vector
from serif.errors import SerifTypeError
from serif.io.parquet import _decode_array_raw
from serif._vector.dtype import Schema
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage
from serif._vector.storage import DecimalStorage
from serif._vector.storage import StringStorage


@pytest.fixture(autouse=True)
def _force_pure_reader(monkeypatch):
    """This suite round-trips serif's OWN zero-dependency reader/writer.
    With pyarrow installed, the optional accelerator would take over reads
    of int/float/str files and this suite would silently stop covering the
    pure reader. Pin the pure path; the arrow path has its own conformance
    suite (test_parquet_arrow.py)."""
    import serif.io.parquet as parquet_mod
    monkeypatch.setattr(parquet_mod, '_USE_ARROW', False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def roundtrip(t: Table) -> Table:
    """Write t, materialize the deferred read, then remove its source."""
    path = tempfile.mktemp(suffix='.parquet')
    try:
        t.to_parquet(path)
        result = Table.from_parquet(path)
        result.cols()
        return result
    finally:
        if os.path.exists(path):
            os.unlink(path)


def col(t: Table, name: str) -> list:
    """Extract a column as a plain list for easy assertion."""
    return list(t[name])


def writer_metadata(path):
    """Return the raw file and parsed footer written by serif."""
    data = memoryview(path.read_bytes())
    assert bytes(data[:4]) == parquet_mod._MAGIC
    assert bytes(data[-4:]) == parquet_mod._MAGIC
    footer_size = int.from_bytes(data[-8:-4], 'little')
    footer_start = len(data) - 8 - footer_size
    metadata, footer_end = parquet_mod._parse_file_metadata(
        data, footer_start)
    assert footer_end == len(data) - 8
    return data, metadata


def empty_vector(kind, nullable):
    """Build an empty vector carrying all metadata the writer needs."""
    schema = Schema(kind, nullable)
    if kind is Decimal:
        storage = DecimalStorage.from_iterable(
            [], scale=2, precision=1, nullable=nullable)
        return Vector._from_storage(storage, schema)
    return Vector([], dtype=schema)


# ---------------------------------------------------------------------------
# Non-nullable columns
# ---------------------------------------------------------------------------

class TestNonNullableRoundtrip:

    def test_string_column(self):
        t = Table({'s': ['alice', 'bob', 'carol']})
        t2 = roundtrip(t)
        assert col(t2, 's') == ['alice', 'bob', 'carol']

    def test_float_column(self):
        t = Table({'f': [1.5, 2.5, 3.5]})
        t2 = roundtrip(t)
        assert col(t2, 'f') == [1.5, 2.5, 3.5]

    def test_bool_column(self):
        t = Table({'b': [True, False, True, False]})
        t2 = roundtrip(t)
        assert col(t2, 'b') == [True, False, True, False]

    def test_int_column(self):
        # int columns need ArrayStorage('q') — arithmetic produces this
        v = Vector([10, 20, 30]) + 0
        t = Table({'n': v})
        t2 = roundtrip(t)
        assert col(t2, 'n') == [10, 20, 30]

    def test_date_column(self):
        dates = [date(2024, 1, 1), date(2024, 6, 15), date(2025, 1, 1)]
        t = Table({'d': dates})
        t2 = roundtrip(t)
        assert col(t2, 'd') == dates

    def test_datetime_column(self):
        dts = [datetime(2024, 1, 1, 12, 0), datetime(2024, 6, 15, 9, 30)]
        t = Table({'ts': dts})
        t2 = roundtrip(t)
        assert col(t2, 'ts') == dts


# ---------------------------------------------------------------------------
# Nullable columns — null position fidelity
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "typecode,packed,null_flags,raw_values,values,mask_bytes",
    [
        (
            'q',
            [10, -3],
            [False, True, False, True],
            [10, 0, -3, 0],
            [10, None, -3, None],
            b'\x05',
        ),
        (
            'd',
            [1.5, -2.25],
            [False, True, False, True],
            [1.5, 0.0, -2.25, 0.0],
            [1.5, None, -2.25, None],
            b'\x05',
        ),
        (
            'q',
            [],
            [True, True, True],
            [0, 0, 0],
            [None, None, None],
            b'\x00',
        ),
        (
            'd',
            [],
            [True, True, True],
            [0.0, 0.0, 0.0],
            [None, None, None],
            b'\x00',
        ),
    ],
    ids=['int64-mixed', 'double-mixed', 'int64-all-null', 'double-all-null'],
)
def test_nullable_packed_page_builds_array_storage(
        typecode, packed, null_flags, raw_values, values, mask_bytes):
    storage = _decode_array_raw(array(typecode, packed), null_flags)

    assert type(storage) is ArrayStorage
    assert storage._data.typecode == typecode
    assert storage._data.tolist() == raw_values
    assert list(storage) == values
    assert bytes(storage._mask._buf) == mask_bytes


@pytest.mark.parametrize(
    "typecode,values",
    [('q', [1, -2]), ('d', [1.5, -2.25])],
    ids=['int64', 'double'],
)
def test_all_valid_optional_packed_page_reuses_array(typecode, values):
    packed = array(typecode, values)
    storage = _decode_array_raw(packed, [False] * len(values))

    assert storage._data is packed
    assert storage._mask is None


def _read_optional_pages(page_values, kind, phys_type, *,
                         conv_type=None, decimal_scale=None,
                         decimal_precision=None):
    chunk = bytearray()
    for values in page_values:
        null_flags = [value is None for value in values]
        non_null = [value for value in values if value is not None]
        body = (
            parquet_mod._encode_def_levels(null_flags)
            + parquet_mod._encode_plain(
                non_null, kind, 'x', decimal_scale)
        )
        data_header = parquet_mod._enc_data_page_header(len(values))
        chunk.extend(parquet_mod._enc_page_header(
            len(body), len(body), data_header))
        chunk.extend(body)

    return parquet_mod._read_column_chunk(
        memoryview(chunk),
        {
            'codec': parquet_mod._CODEC_UNCOMPRESSED,
            'num_values': sum(map(len, page_values)),
            'data_page_offset': 0,
        },
        kind=kind,
        phys_type=phys_type,
        conv_type=conv_type,
        is_optional=True,
        col_name='x',
        decimal_scale=decimal_scale,
        decimal_precision=decimal_precision,
    )


@pytest.mark.parametrize(
    "kind,phys_type,page_values,options",
    [
        (
            int,
            parquet_mod._T_INT64,
            [[1, None], [None, -3], [4]],
            {},
        ),
        (
            float,
            parquet_mod._T_DOUBLE,
            [[1.5, None], [None, -3.25], [4.5]],
            {},
        ),
        (
            bool,
            parquet_mod._T_BOOLEAN,
            [[True, None], [None, False], [True]],
            {},
        ),
        (
            str,
            parquet_mod._T_BYTE_ARRAY,
            [['a', None], [None, 'β'], ['']],
            {'conv_type': parquet_mod._CT_UTF8},
        ),
        (
            Decimal,
            parquet_mod._T_FIXED_LEN_BYTE_ARRAY,
            [
                [Decimal('1.25'), None],
                [None, Decimal('-2.50')],
                [Decimal('0.01')],
            ],
            {'decimal_scale': 2, 'decimal_precision': 4},
        ),
    ],
    ids=['int64', 'double', 'boolean', 'string', 'decimal'],
)
def test_column_chunk_concatenates_page_storages_once(
        monkeypatch, kind, phys_type, page_values, options):
    calls = []
    original = parquet_mod.concatenate_storages

    def recording_concatenate(storages):
        storages = tuple(storages)
        calls.append(storages)
        return original(storages)

    monkeypatch.setattr(
        parquet_mod, 'concatenate_storages', recording_concatenate)

    result = _read_optional_pages(
        page_values, kind, phys_type, **options)

    assert len(calls) == 1
    assert len(calls[0]) == len(page_values)
    assert all(type(page) is type(calls[0][0]) for page in calls[0])
    assert list(result) == [
        value
        for page in page_values
        for value in page
    ]


def test_decoded_part_combiner_preserves_empty_and_list_results():
    assert parquet_mod._combine_decoded_parts([]) == []
    assert parquet_mod._combine_decoded_parts([
        [1, 2],
        [None, 4],
    ]) == [1, 2, None, 4]


def test_decoded_part_combiner_rejects_mixed_representations():
    with pytest.raises(RuntimeError, match='mixed physical'):
        parquet_mod._combine_decoded_parts([
            array('q', [1, 2]),
            [None, 4],
        ])


def test_column_combiner_rejects_impossible_mixed_storage_types():
    with pytest.raises(RuntimeError, match='mixed storage'):
        parquet_mod._combine_columns(
            [
                Vector([1], name='x'),
                Vector([2**70], name='x'),
            ],
            {
                'name': 'x',
                'kind': int,
                'is_optional': False,
            },
        )


def test_decoded_part_combiner_extends_packed_arrays_in_place():
    first = array('q', [1, 2])
    result = parquet_mod._combine_decoded_parts([
        first,
        array('q', [3]),
        array('q', [4, 5]),
    ])

    assert result is first
    assert result.tolist() == [1, 2, 3, 4, 5]


@pytest.mark.parametrize(
    "kind,phys_type,is_optional,options,expected_type,typecode",
    [
        (
            int, parquet_mod._T_INT64, False,
            {}, array, 'q',
        ),
        (
            int, parquet_mod._T_INT64, True,
            {}, ArrayStorage, 'q',
        ),
        (
            float, parquet_mod._T_DOUBLE, False,
            {}, array, 'd',
        ),
        (
            float, parquet_mod._T_DOUBLE, True,
            {}, ArrayStorage, 'd',
        ),
        (
            int, parquet_mod._T_INT32, False,
            {}, list, None,
        ),
        (
            str, parquet_mod._T_BYTE_ARRAY, True,
            {'conv_type': parquet_mod._CT_UTF8}, StringStorage, None,
        ),
        (
            bool, parquet_mod._T_BOOLEAN, True,
            {}, BoolStorage, None,
        ),
        (
            Decimal, parquet_mod._T_FIXED_LEN_BYTE_ARRAY, True,
            {'decimal_scale': 2, 'decimal_precision': 4},
            DecimalStorage, None,
        ),
    ],
    ids=[
        'required-int64',
        'optional-int64',
        'required-double',
        'optional-double',
        'int32-list',
        'string',
        'boolean',
        'decimal',
    ],
)
def test_empty_chunk_uses_peer_physical_representation(
        kind, phys_type, is_optional, options, expected_type, typecode):
    result = parquet_mod._read_column_chunk(
        memoryview(b''),
        {
            'codec': parquet_mod._CODEC_UNCOMPRESSED,
            'num_values': 0,
            'data_page_offset': 0,
        },
        kind=kind,
        phys_type=phys_type,
        conv_type=options.get('conv_type'),
        is_optional=is_optional,
        col_name='x',
        decimal_scale=options.get('decimal_scale'),
        decimal_precision=options.get('decimal_precision'),
    )

    assert type(result) is expected_type
    if typecode is not None:
        data = result._data if isinstance(result, ArrayStorage) else result
        assert data.typecode == typecode
    assert len(result) == 0


class TestNullableRoundtrip:

    def test_nullable_int_nulls_in_right_slots(self):
        t = Table({'i': [1, None, -3, None]})
        t2 = roundtrip(t)
        result = t2['i']
        assert type(result._storage) is ArrayStorage
        assert list(result) == [1, None, -3, None]

    def test_nullable_string_nulls_in_right_slots(self):
        t = Table({'s': ['alice', None, 'carol', None]})
        t2 = roundtrip(t)
        result = col(t2, 's')
        assert result[0] == 'alice'
        assert result[1] is None
        assert result[2] == 'carol'
        assert result[3] is None

    def test_nullable_float_nulls_in_right_slots(self):
        t = Table({'f': [1.5, None, 3.5, None]})
        t2 = roundtrip(t)
        result = t2['f']
        assert type(result._storage) is ArrayStorage
        assert list(result) == [1.5, None, 3.5, None]

    def test_nullable_bool_nulls_in_right_slots(self):
        t = Table({'b': [True, None, False, None]})
        t2 = roundtrip(t)
        result = col(t2, 'b')
        assert result[0] is True
        assert result[1] is None
        assert result[2] is False
        assert result[3] is None

    def test_nullable_date_nulls_in_right_slots(self):
        t = Table({'d': [date(2024, 1, 1), None, date(2025, 1, 1)]})
        t2 = roundtrip(t)
        result = col(t2, 'd')
        assert result[0] == date(2024, 1, 1)
        assert result[1] is None
        assert result[2] == date(2025, 1, 1)

    def test_nullable_datetime_nulls_in_right_slots(self):
        t = Table({'ts': [datetime(2024, 1, 1), None, datetime(2025, 6, 1)]})
        t2 = roundtrip(t)
        result = col(t2, 'ts')
        assert result[0] == datetime(2024, 1, 1)
        assert result[1] is None
        assert result[2] == datetime(2025, 6, 1)

    def test_null_first_roundtrips(self):
        # Inference is order-independent: [None, 'bob', 'carol'] is str?
        # exactly like ['bob', 'carol', None], so this writes and reads.
        t = Table({'s': [None, 'bob', 'carol']})
        t2 = roundtrip(t)
        result = col(t2, 's')
        assert result[0] is None
        assert result[1] == 'bob'
        assert result[2] == 'carol'

    def test_null_last(self):
        t = Table({'s': ['alice', 'bob', None]})
        t2 = roundtrip(t)
        result = col(t2, 's')
        assert result[2] is None

    def test_all_null_column_raises_due_to_type_inference(self):
        # All-None columns have no type-establishing values; serif infers
        # 'object'.  Parquet can't write object columns.
        t = Table({'s': [None, None, None]})
        path = tempfile.mktemp(suffix='.parquet')
        try:
            with pytest.raises(SerifTypeError):
                t.to_parquet(path)
        finally:
            if os.path.exists(path):
                os.unlink(path)


# ---------------------------------------------------------------------------
# Mixed-type table (multiple columns together)
# ---------------------------------------------------------------------------

class TestMixedTable:

    def test_mixed_types_round_trip(self):
        t = Table({
            'name':   ['alice', 'bob', None, 'carol'],
            'score':  [1.5, 2.5, 3.5, None],
            'active': [True, False, True, False],
            'joined': [date(2024, 1, 1), date(2024, 6, 15), None, date(2025, 1, 1)],
        })
        t2 = roundtrip(t)
        assert t2.column_names() == ['name', 'score', 'active', 'joined']
        assert col(t2, 'name')[2] is None
        assert col(t2, 'score')[3] is None
        assert col(t2, 'joined')[2] is None
        assert col(t2, 'name')[0] == 'alice'
        assert col(t2, 'score')[0] == 1.5
        assert col(t2, 'active')[0] is True

    def test_column_names_preserved(self):
        t = Table({'foo': [1.0, 2.0], 'bar': ['x', 'y'], 'baz': [True, False]})
        t2 = roundtrip(t)
        assert t2.column_names() == ['foo', 'bar', 'baz']

    def test_row_count_preserved(self):
        t = Table({'x': list(range(100)), 'y': [float(i) for i in range(100)]})
        t2 = roundtrip(t)
        assert len(t2) == 100


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_single_row(self):
        t = Table({'s': ['only'], 'n': [42.0]})
        t2 = roundtrip(t)
        assert col(t2, 's') == ['only']
        assert col(t2, 'n') == [42.0]

    def test_single_value_non_null(self):
        t = Table({'x': [99.9]})
        t2 = roundtrip(t)
        assert col(t2, 'x') == [99.9]

    def test_single_value_null_raises_due_to_type_inference(self):
        # Single-None column: serif infers 'object', parquet write raises.
        t = Table({'x': [None]})
        path = tempfile.mktemp(suffix='.parquet')
        try:
            with pytest.raises(SerifTypeError):
                t.to_parquet(path)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_empty_string(self):
        t = Table({'s': ['', 'non-empty', '']})
        t2 = roundtrip(t)
        assert col(t2, 's') == ['', 'non-empty', '']

    def test_unicode_strings(self):
        t = Table({'s': ['café', '日本語', 'emoji 🎉']})
        t2 = roundtrip(t)
        assert col(t2, 's') == ['café', '日本語', 'emoji 🎉']

    def test_unicode_with_nulls(self):
        t = Table({'s': ['café', None, '日本語']})
        t2 = roundtrip(t)
        result = col(t2, 's')
        assert result[0] == 'café'
        assert result[1] is None
        assert result[2] == '日本語'

    def test_boolean_all_true(self):
        t = Table({'b': [True, True, True]})
        t2 = roundtrip(t)
        assert col(t2, 'b') == [True, True, True]

    def test_boolean_all_false(self):
        t = Table({'b': [False, False, False]})
        t2 = roundtrip(t)
        assert col(t2, 'b') == [False, False, False]

    def test_date_epoch(self):
        # date(1970,1,1) = day 0; date before epoch should be negative
        t = Table({'d': [date(1970, 1, 1), date(1969, 12, 31)]})
        t2 = roundtrip(t)
        result = col(t2, 'd')
        assert result[0] == date(1970, 1, 1)
        assert result[1] == date(1969, 12, 31)

    def test_datetime_microsecond_precision(self):
        dt = datetime(2024, 3, 15, 10, 30, 45, 123456)
        t = Table({'ts': [dt]})
        t2 = roundtrip(t)
        assert col(t2, 'ts')[0] == dt

    def test_many_rows(self):
        n = 10_000
        t = Table({'x': [float(i) for i in range(n)], 'y': [str(i) for i in range(n)]})
        t2 = roundtrip(t)
        assert len(t2) == n
        assert col(t2, 'x')[0] == 0.0
        assert col(t2, 'x')[-1] == float(n - 1)
        assert col(t2, 'y')[0] == '0'
        assert col(t2, 'y')[-1] == str(n - 1)

    def test_nine_columns(self):
        # Exercises multiple schema elements and column chunks
        t = Table({
            'a': [1.0], 'b': [2.0], 'c': [3.0],
            'd': [4.0], 'e': [5.0], 'f': [6.0],
            'g': [7.0], 'h': [8.0], 'i': [9.0],
        })
        t2 = roundtrip(t)
        assert len(t2._storage) == 9
        assert col(t2, 'e') == [5.0]


# ---------------------------------------------------------------------------
# Decimal columns (decimal128, 16-byte FIXED_LEN_BYTE_ARRAY)
# ---------------------------------------------------------------------------

class TestDecimalRoundtrip:
    """Decimals round-trip through serif's own reader/writer as decimal128.
    Value, scale, and precision are preserved, and None lands back in the
    right slot. Scale/precision live in DecimalStorage (not the Schema), so
    those assertions reach into storage."""

    def test_non_nullable(self):
        from decimal import Decimal
        vals = [Decimal('123.45'), Decimal('67.89'), Decimal('-0.01')]
        t2 = roundtrip(Table({'amount': vals}))
        assert col(t2, 'amount') == vals
        assert t2['amount'].schema().kind is Decimal
        assert t2['amount'].schema().nullable is False

    def test_nullable(self):
        from decimal import Decimal
        vals = [Decimal('1.50'), None, Decimal('999.99')]
        t2 = roundtrip(Table({'amount': vals}))
        assert col(t2, 'amount') == vals
        assert t2['amount'].schema().kind is Decimal
        assert t2['amount'].schema().nullable is True

    def test_scale_and_precision_preserved(self):
        from decimal import Decimal
        from serif._vector.storage import DecimalStorage
        t2 = roundtrip(Table({'amount': [Decimal('123.45'), Decimal('67.89')]}))
        st = t2['amount']._storage
        assert isinstance(st, DecimalStorage)
        assert st._scale == 2
        assert st._precision == 5


# ---------------------------------------------------------------------------
# Writer output and failure semantics
# ---------------------------------------------------------------------------

class TestWriterCharacterization:

    def test_physical_schema_and_repetition_metadata(self, tmp_path):
        table = Table({
            'bool_required': [True, False],
            'bool_optional': [True, None],
            'int_required': [1, -2],
            'int_optional': [1, None],
            'float_required': [1.5, -2.25],
            'float_optional': [1.5, None],
            'string_required': ['café', '日本語'],
            'string_optional': ['emoji 🎉', None],
            'date_required': [date(1970, 1, 1), date(2024, 6, 15)],
            'date_optional': [date(1969, 12, 31), None],
            'datetime_required': [
                datetime(1969, 12, 31, 23, 59, 59, 999999),
                datetime(2200, 6, 15, 12, 34, 56, 123457),
            ],
            'datetime_optional': [
                datetime(2024, 3, 15, 10, 30, 45, 123456),
                None,
            ],
            'decimal_required': [Decimal('12.34'), Decimal('-0.01')],
            'decimal_optional': [Decimal('1.20'), None],
        })
        path = tmp_path / 'schema.parquet'

        table.to_parquet(str(path))
        _, metadata = writer_metadata(path)

        expected = [
            ('bool_required', parquet_mod._T_BOOLEAN, None,
             parquet_mod._REP_REQUIRED, None, None, None),
            ('bool_optional', parquet_mod._T_BOOLEAN, None,
             parquet_mod._REP_OPTIONAL, None, None, None),
            ('int_required', parquet_mod._T_INT64, None,
             parquet_mod._REP_REQUIRED, None, None, None),
            ('int_optional', parquet_mod._T_INT64, None,
             parquet_mod._REP_OPTIONAL, None, None, None),
            ('float_required', parquet_mod._T_DOUBLE, None,
             parquet_mod._REP_REQUIRED, None, None, None),
            ('float_optional', parquet_mod._T_DOUBLE, None,
             parquet_mod._REP_OPTIONAL, None, None, None),
            ('string_required', parquet_mod._T_BYTE_ARRAY,
             parquet_mod._CT_UTF8, parquet_mod._REP_REQUIRED,
             None, None, None),
            ('string_optional', parquet_mod._T_BYTE_ARRAY,
             parquet_mod._CT_UTF8, parquet_mod._REP_OPTIONAL,
             None, None, None),
            ('date_required', parquet_mod._T_INT32,
             parquet_mod._CT_DATE, parquet_mod._REP_REQUIRED,
             None, None, None),
            ('date_optional', parquet_mod._T_INT32,
             parquet_mod._CT_DATE, parquet_mod._REP_OPTIONAL,
             None, None, None),
            ('datetime_required', parquet_mod._T_INT64,
             parquet_mod._CT_TIMESTAMP_MICROS, parquet_mod._REP_REQUIRED,
             None, None, None),
            ('datetime_optional', parquet_mod._T_INT64,
             parquet_mod._CT_TIMESTAMP_MICROS, parquet_mod._REP_OPTIONAL,
             None, None, None),
            ('decimal_required', parquet_mod._T_FIXED_LEN_BYTE_ARRAY,
             parquet_mod._CT_DECIMAL, parquet_mod._REP_REQUIRED,
             16, 2, 4),
            ('decimal_optional', parquet_mod._T_FIXED_LEN_BYTE_ARRAY,
             parquet_mod._CT_DECIMAL, parquet_mod._REP_OPTIONAL,
             16, 2, 3),
        ]

        assert metadata['version'] == 2
        assert metadata['num_rows'] == 2
        assert len(metadata['row_groups']) == 1
        root, *leaves = metadata['schema']
        assert root['name'] == 'schema'
        assert root['repetition_type'] == parquet_mod._REP_REQUIRED
        assert root['num_children'] == len(expected)

        chunks = metadata['row_groups'][0]['columns']
        assert len(chunks) == len(expected)
        for leaf, chunk, expected_column in zip(
                leaves, chunks, expected, strict=True):
            name, phys, conv, rep, type_length, scale, precision = (
                expected_column)
            assert (
                leaf['name'],
                leaf['type'],
                leaf['converted_type'],
                leaf['repetition_type'],
                leaf['type_length'],
                leaf['scale'],
                leaf['precision'],
            ) == expected_column

            column_metadata = chunk['meta_data']
            assert column_metadata['path_in_schema'] == [name]
            assert column_metadata['type'] == phys
            assert column_metadata['num_values'] == 2
            assert column_metadata['codec'] == parquet_mod._CODEC_UNCOMPRESSED
            expected_encodings = [parquet_mod._ENC_PLAIN]
            if rep == parquet_mod._REP_OPTIONAL:
                expected_encodings.append(parquet_mod._ENC_RLE)
            assert column_metadata['encodings'] == expected_encodings

    def test_nullable_definition_level_bytes(self, tmp_path):
        values = [1, None, 3, None, 5, 6, None, 8, 9, None]
        path = tmp_path / 'definition-levels.parquet'
        Table({'x': values}).to_parquet(str(path))

        data, metadata = writer_metadata(path)
        column_metadata = (
            metadata['row_groups'][0]['columns'][0]['meta_data'])
        page, body_start = parquet_mod._parse_page_header(
            data, column_metadata['data_page_offset'])

        assert page['type'] == parquet_mod._PAGE_DATA
        assert page['data_page_header'] == {
            'num_values': len(values),
            'encoding': parquet_mod._ENC_PLAIN,
            'definition_level_encoding': parquet_mod._ENC_RLE,
            'repetition_level_encoding': parquet_mod._ENC_RLE,
        }
        assert bytes(data[body_start:body_start + 7]) == (
            b'\x03\x00\x00\x00\x05\xb5\x01')

    def test_empty_table_roundtrips(self):
        result = roundtrip(Table())
        assert result.shape == (0, 0)
        assert result.column_names() == []

    @pytest.mark.parametrize(
        'kind',
        [bool, int, float, str, date, datetime, Decimal],
        ids=['bool', 'int', 'float', 'str', 'date', 'datetime', 'decimal'],
    )
    @pytest.mark.parametrize(
        'nullable',
        [False, True],
        ids=['required', 'optional'],
    )
    def test_empty_typed_column_roundtrips(self, kind, nullable):
        table = Table({'empty': empty_vector(kind, nullable)})

        result = roundtrip(table)

        assert result.column_names() == ['empty']
        assert list(result['empty']) == []
        assert result['empty'].schema() == Schema(kind, nullable)

    @pytest.mark.parametrize(
        'values,nullable',
        [
            (
                [Decimal('123.45'), Decimal('-0.01')],
                False,
            ),
            (
                [Decimal('1.50'), None, Decimal('-2.25')],
                True,
            ),
        ],
        ids=['required', 'optional'],
    )
    def test_decimal_storage_roundtrips(self, values, nullable):
        storage = DecimalStorage.from_iterable(
            values, scale=2, precision=5, nullable=nullable)
        vector = Vector._from_storage(
            storage, Schema(Decimal, nullable), name='amount')

        result = roundtrip(Table([vector]))

        assert list(result['amount']) == values
        assert result['amount'].schema() == Schema(Decimal, nullable)
        assert type(result['amount']._storage) is DecimalStorage
        assert result['amount']._storage._scale == 2
        assert result['amount']._storage._precision == 5

    @pytest.mark.parametrize(
        'table_factory,error_type,message',
        [
            pytest.param(
                lambda: Table({'empty': []}),
                SerifTypeError,
                'cannot write untyped',
                id='untyped-empty-column',
            ),
            pytest.param(
                lambda: Table({
                    'empty': Vector([], dtype=Decimal),
                }),
                SerifTypeError,
                'empty Decimal column',
                id='decimal-empty-without-scale',
            ),
            pytest.param(
                lambda: Table({'too_wide': [2 ** 63]}),
                SerifTypeError,
                'must be backed by',
                id='oversized-int',
            ),
            pytest.param(
                lambda: Table({'unsupported': [1 + 2j]}),
                SerifTypeError,
                "unsupported type 'complex'",
                id='complex',
            ),
            pytest.param(
                lambda: Table({'unsupported': [b'bytes']}),
                SerifTypeError,
                "unsupported type 'bytes'",
                id='bytes',
            ),
            pytest.param(
                lambda: Table({
                    'valid_first': [1],
                    'too_wide': [Decimal('1e100')],
                }),
                OverflowError,
                'too big',
                id='oversized-decimal-after-valid-column',
            ),
        ],
    )
    def test_value_failure_preserves_existing_destination(
            self, tmp_path, table_factory, error_type, message):
        path = tmp_path / 'existing.parquet'
        original = b'existing destination contents'
        path.write_bytes(original)

        with pytest.raises(error_type, match=message):
            table_factory().to_parquet(str(path))

        assert path.read_bytes() == original

    def test_open_failure_preserves_existing_destination(
            self, tmp_path, monkeypatch):
        path = tmp_path / 'existing.parquet'
        original = b'existing destination contents'
        path.write_bytes(original)

        def failing_open(file, mode):
            assert os.fspath(file) == str(path)
            assert mode == 'wb'
            raise OSError('simulated open failure')

        monkeypatch.setattr(
            parquet_mod, 'open', failing_open, raising=False)

        with pytest.raises(OSError, match='simulated open failure'):
            Table({'x': [1]}).to_parquet(str(path))

        assert path.read_bytes() == original

    def test_write_failure_after_open_does_not_preserve_destination(
            self, tmp_path, monkeypatch):
        path = tmp_path / 'existing.parquet'
        path.write_bytes(b'existing destination contents')
        real_open = open

        class FailingWriter:
            def __init__(self, file, mode):
                self._handle = real_open(file, mode)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                self._handle.close()

            def write(self, data):
                self._handle.write(data[:4])
                self._handle.flush()
                raise OSError('simulated write failure')

        monkeypatch.setattr(
            parquet_mod, 'open', FailingWriter, raising=False)

        with pytest.raises(OSError, match='simulated write failure'):
            Table({'x': [1]}).to_parquet(str(path))

        assert path.read_bytes() == parquet_mod._MAGIC


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestErrorCases:

    def test_object_column_raises(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            t = Table({'mixed': ['string', 42, True]})
        path = tempfile.mktemp(suffix='.parquet')
        try:
            with pytest.raises(SerifTypeError, match="object"):
                t.to_parquet(path)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_plain_int_list_works(self):
        # [1, 2, 3] creates ArrayStorage('q') — works fine
        t = Table({'n': [1, 2, 3]})
        # verify the storage is actually 'q'
        from serif._vector.storage import ArrayStorage
        assert isinstance(t['n']._storage, ArrayStorage)
        assert t['n']._storage._data.typecode == 'q'
        t2 = roundtrip(t)
        assert col(t2, 'n') == [1, 2, 3]
