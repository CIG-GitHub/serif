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
from datetime import date, datetime

import pytest

from serif import Table, Vector
from serif.errors import SerifTypeError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def roundtrip(t: Table) -> Table:
    """Write t to a temp file, read it back, return the result."""
    path = tempfile.mktemp(suffix='.parquet')
    try:
        t.to_parquet(path)
        return Table.from_parquet(path)
    finally:
        if os.path.exists(path):
            os.unlink(path)


def col(t: Table, name: str) -> list:
    """Extract a column as a plain list for easy assertion."""
    return list(t[name])


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

class TestNullableRoundtrip:

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
        result = col(t2, 'f')
        assert result[0] == 1.5
        assert result[1] is None
        assert result[2] == 3.5
        assert result[3] is None

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

    def test_null_first_raises_due_to_type_inference(self):
        # serif infers 'object' when the first element is None, even if
        # all subsequent values are str.  Parquet can't write object columns.
        # This is a serif type-inference limitation, not a parquet bug.
        t = Table({'s': [None, 'bob', 'carol']})
        path = tempfile.mktemp(suffix='.parquet')
        try:
            with pytest.raises(SerifTypeError):
                t.to_parquet(path)
        finally:
            if os.path.exists(path):
                os.unlink(path)

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
# Error cases
# ---------------------------------------------------------------------------

class TestErrorCases:

    def test_object_column_raises(self):
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
