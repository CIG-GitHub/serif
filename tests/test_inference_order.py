"""
Type inference is order-independent with respect to Nones.

Doctrine: a leading None only sets nullable — the kind comes from the first
non-None value, with normal promotion after. [None, 1, 2] and [1, 2, None]
infer the identical schema. Only all-None (or empty) data infers object.

Before this pass, [None, 1, 2] locked to object at the first element while
[1, 2, None] inferred int? — same values, different schema depending on row
order, which surfaced as "can't write to parquet" on any CSV whose first row
had a blank.
"""

import io
from datetime import date

import pytest

from serif import Table, Vector, read_csv
from serif._vector.dtype import infer_dtype
from serif._vector.storage import ArrayStorage, StringStorage


def test_null_first_and_null_last_infer_identically():
    assert Vector([None, 1, 2]).schema() == Vector([1, 2, None]).schema()


def test_null_first_int():
    s = Vector([None, 1, 2]).schema()
    assert s.kind is int
    assert s.nullable is True


def test_null_first_str():
    s = Vector([None, 'a']).schema()
    assert s.kind is str
    assert s.nullable is True


def test_null_first_date():
    s = Vector([None, date(2024, 1, 1)]).schema()
    assert s.kind is date
    assert s.nullable is True


def test_null_first_then_promotion():
    # Leading None, then int, then float → float? (promotion still applies)
    s = Vector([None, 1, 2.5]).schema()
    assert s.kind is float
    assert s.nullable is True


def test_null_first_mixed_degrades_with_warning():
    with pytest.warns(UserWarning, match='[Dd]egrading'):
        s = Vector([None, 1, 'a']).schema()
    assert s.kind is object


def test_all_none_still_object():
    s = Vector([None, None]).schema()
    assert s.kind is object
    assert s.nullable is True


def test_infer_dtype_function_directly():
    assert infer_dtype([None, 1]) == infer_dtype([1, None])


def test_null_first_values_roundtrip():
    v = Vector([None, 1, 2])
    assert list(v) == [None, 1, 2]
    assert isinstance(v._storage, ArrayStorage)  # typed backend, not tuples


def test_null_first_string_gets_string_backend():
    v = Vector([None, 'a', 'b'])
    assert isinstance(v._storage, StringStorage)
    assert list(v) == [None, 'a', 'b']


def test_table_column_null_first():
    t = Table({'x': [None, 1, 2]})
    assert t.x.schema().kind is int
    assert t.x.schema().nullable is True


def test_csv_column_with_blank_first_cell_is_typed():
    t = read_csv(io.StringIO("a\n\n1\n2\n"))
    assert list(t.a) == [None, 1, 2]
    assert t.a.schema().kind is int
    assert t.a.schema().nullable is True
