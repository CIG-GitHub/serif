"""
Pins for the semantic fixes (commit 3): loud errors instead of silent
wrongness, honest schemas, backend preservation across mutation, and the
documented ride-along Row semantics.
"""

import warnings
from collections import OrderedDict

import pytest

import serif
from serif import Vector, Table, Schema
from serif import SerifTypeError, SerifValueError, SerifIndexError
from serif._vector.storage import ArrayStorage, StringStorage, TupleStorage


# ---------------------------------------------------------------------------
# Ragged Table construction pukes instead of truncating
# ---------------------------------------------------------------------------

def test_ragged_table_list_raises():
    with pytest.raises(SerifValueError, match="same length"):
        Table([Vector([1, 2]), Vector([1, 2, 3])])


def test_ragged_table_dict_raises():
    with pytest.raises(SerifValueError, match="same length"):
        Table({'a': [1, 2], 'b': [1, 2, 3]})


# ---------------------------------------------------------------------------
# Table.__getitem__ never silently returns None
# ---------------------------------------------------------------------------

def test_table_getitem_float_key_raises():
    t = Table({'a': [1, 2]})
    with pytest.raises(SerifTypeError, match="Table indices"):
        t[1.5]


def test_table_getitem_nullable_int_vector_raises():
    t = Table({'a': [1, 2, 3]})
    with pytest.raises(SerifTypeError, match="Table indices"):
        t[Vector([0, None, 1])]


# ---------------------------------------------------------------------------
# Table comparisons work and return a Table of bool columns
# ---------------------------------------------------------------------------

def test_table_eq_table():
    t1 = Table({'a': [1, 2], 'b': [3, 4]})
    t2 = Table({'a': [1, 9], 'b': [3, 4]})
    r = t1 == t2
    assert isinstance(r, Table)
    assert r.column_names() == ['a', 'b']
    assert list(r['a']) == [True, False]
    assert list(r['b']) == [True, True]


def test_table_eq_scalar_broadcasts():
    t = Table({'a': [1, 2], 'b': [2, 2]})
    r = t == 2
    assert isinstance(r, Table)
    assert list(r['a']) == [False, True]
    assert list(r['b']) == [True, True]


def test_table_compare_column_count_mismatch_raises():
    t1 = Table({'a': [1]})
    t2 = Table({'a': [1], 'b': [2]})
    with pytest.raises(SerifValueError, match="Column count mismatch"):
        t1 == t2


def test_table_gt_null_propagates():
    t = Table({'a': [1, None, 3]})
    r = t > 1
    assert list(r['a']) == [False, None, True]


# ---------------------------------------------------------------------------
# Column replacement does not alias or mutate the caller's vector
# ---------------------------------------------------------------------------

def test_setattr_column_copies_and_preserves_caller():
    t = Table({'x': [1, 2]})
    v = Vector([5, 6], name='mycol')

    t.x = v

    assert list(t.x) == [5, 6]
    assert v.name == 'mycol', "caller's vector must not be renamed"

    t.x[0] = 99
    assert list(v) == [5, 6], "caller's vector must not share storage with the table"
    assert list(t.x) == [99, 6]


# ---------------------------------------------------------------------------
# None assignment widens the schema (honest nullability)
# ---------------------------------------------------------------------------

def test_setitem_none_promotes_schema_to_nullable():
    v = Vector([1, 2, 3])
    assert v.schema() == Schema(int, False)
    v[0] = None
    assert v.schema() == Schema(int, True), "schema must not lie about stored None"
    assert list(v) == [None, 2, 3]


def test_setitem_none_then_parquet_roundtrip(tmp_path):
    t = Table({'x': [1, 2, 3]})
    t.x[0] = None
    p = str(tmp_path / 'nullable.parquet')
    t.to_parquet(p)
    back = Table.from_parquet(p)
    assert list(back['x']) == [None, 2, 3]


# ---------------------------------------------------------------------------
# Mutation preserves storage backends
# ---------------------------------------------------------------------------

def test_setitem_int_keeps_arraystorage_and_parquet_writable(tmp_path):
    v = Vector([1, 2, 3])
    v[0] = 9
    assert isinstance(v._storage, ArrayStorage)

    t = Table({'x': [1, 2, 3]})
    t.x[0] = 9
    p = str(tmp_path / 'mutated.parquet')
    t.to_parquet(p)
    assert list(Table.from_parquet(p)['x']) == [9, 2, 3]


def test_setitem_str_keeps_stringstorage():
    v = Vector(['a', 'b'])
    v[0] = 'z'
    assert isinstance(v._storage, StringStorage)
    assert list(v) == ['z', 'b']


def test_setitem_huge_int_falls_back_exactly():
    # Arbitrary-precision semantics: values beyond i64 stay exact.
    v = Vector([1, 2, 3])
    v[0] = 10**30
    assert isinstance(v._storage, TupleStorage)
    assert v[0] == 10**30
    assert v.schema().kind is int


def test_setitem_promotion_int_to_float_keeps_array_backend():
    v = Vector([1, 2, 3])
    v[0] = 1.5
    assert v.schema().kind is float
    assert isinstance(v._storage, ArrayStorage)
    assert list(v) == [1.5, 2.0, 3.0]


# ---------------------------------------------------------------------------
# & | ^ raise on non-bool, non-int dtypes (Python semantics first)
# ---------------------------------------------------------------------------

def test_bitwise_ops_raise_on_float():
    v = Vector([1.5, 0.0])
    for expr in (lambda: v & v, lambda: v | v, lambda: v ^ v):
        with pytest.raises(SerifTypeError):
            expr()


def test_bitwise_ops_raise_on_str():
    v = Vector(['a', 'b'])
    with pytest.raises(SerifTypeError):
        v & v


def test_bitwise_dispatch_unchanged_for_bool_and_int():
    assert list(Vector([True, False]) & Vector([True, True])) == [True, False]
    assert list(Vector([3]) & 1) == [1]


def test_table_bitwise_recurses_per_column():
    t = Table({'flags': [True, False], 'bits': [3, 2]})
    r = t & Vector([True, True])  # bool col: Kleene; int col: bitwise
    cols = r.cols()
    assert list(cols[0]) == [True, False]
    assert list(cols[1]) == [1, 0]


# ---------------------------------------------------------------------------
# Exception-type discipline
# ---------------------------------------------------------------------------

def test_out_of_range_index_raises_serif_index_error():
    v = Vector([1, 2, 3])
    with pytest.raises(SerifIndexError):
        v[99]
    with pytest.raises(IndexError):  # subclass relationship holds
        v[99]


def test_vector_rshift_scalar_raises_intended_message():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(SerifTypeError, match="constant values"):
            Vector([1, 2]) >> 5


def test_empty_vector_lshift_concatenates():
    assert list(Vector() << [1, 2]) == [1, 2]


def test_length_mismatch_is_serif_value_error():
    with pytest.raises(SerifValueError):
        Vector([1, 2]) + Vector([1, 2, 3])


def test_set_repr_rows_validates_input():
    with pytest.raises(TypeError):
        serif.set_repr_rows("lots")
    with pytest.raises(TypeError):
        serif.set_repr_rows(0)
    serif.set_repr_rows(None)  # reset stays valid


# ---------------------------------------------------------------------------
# aggregate()/window() never emit duplicate column names
# ---------------------------------------------------------------------------

def test_aggregate_output_name_collision_uniquified():
    t = Table({'key': [1, 1, 2], 'x': [1, 2, 3]})
    r = t.aggregate('key', {'key': t.x.sum})
    assert r.column_names() == ['key', 'key2']
    assert list(r['key2']) == [3, 3]


def test_window_output_name_collision_uniquified():
    t = Table({'key': [1, 1, 2], 'x': [1, 2, 3]})
    r = t.window('key', {'key': t.x.sum})
    assert r.column_names() == ['key', 'key2']


# ---------------------------------------------------------------------------
# Join right-key drop is by column identity, not name
# ---------------------------------------------------------------------------

def test_join_external_key_does_not_drop_same_named_column():
    t1 = Table({'id': [1, 2]})
    t2 = Table({'id': [10, 20], 'v': [100, 200]})
    # External computed key that happens to be named 'id' — t2's real 'id'
    # column is NOT the key and must survive the join.
    ext_key = Vector([1, 2]).alias('id')
    with pytest.warns(UserWarning, match="Duplicate column name"):
        j = t1.join(t2, 'id', ext_key)
    assert j.column_names() == ['id', 'id', 'v']
    assert list(j.cols(1)) == [10, 20]


def test_join_own_key_column_still_dropped():
    t1 = Table({'id': [1, 2], 'a': [5, 6]})
    t2 = Table({'id': [1, 2], 'b': [7, 8]})
    j = t1.join(t2, 'id', 'id')
    assert j.column_names() == ['id', 'a', 'b']


# ---------------------------------------------------------------------------
# Parquet round-trips duplicate column names without merging
# ---------------------------------------------------------------------------

def test_parquet_duplicate_column_names_roundtrip(tmp_path):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        t = Table([
            Vector([1, 2], name='x'),
            Vector([10.5, 20.5], name='x'),
        ])
    p = str(tmp_path / 'dup.parquet')
    t.to_parquet(p)
    back = Table.from_parquet(p)
    assert back.column_names() == ['x', 'x']
    assert list(back.cols(0)) == [1, 2]
    assert list(back.cols(1)) == [10.5, 20.5]


# ---------------------------------------------------------------------------
# Ride-along Row views: intentional, documented semantics
# ---------------------------------------------------------------------------

def test_row_iteration_is_ride_along_view():
    t = Table({'a': [1, 2, 3]})
    rows = list(t)
    # One view object, parked on the last row — this is the documented
    # zero-allocation design (docs/table-model.md #4), not a bug.
    assert rows[0] is rows[1] is rows[2]
    assert rows[0]['a'] == 3


def test_row_iteration_consume_in_loop_is_correct():
    t = Table({'a': [1, 2, 3], 'b': [10, 20, 30]})
    seen = [(row['a'], row['b']) for row in t]
    assert seen == [(1, 10), (2, 20), (3, 30)]
    # Explicit materialization pattern from the docs:
    assert [tuple(row) for row in t] == [(1, 10), (2, 20), (3, 30)]


# ---------------------------------------------------------------------------
# Inference/validation agree on subclass instances for non-primitive kinds
# ---------------------------------------------------------------------------

def test_dict_column_accepts_ordereddict_on_assignment():
    v = Vector([{'a': 1}, {'b': 2}])
    assert v.schema().kind is dict
    v[0] = OrderedDict(c=3)  # inferred kind dict; assignment must agree
    assert v[0] == {'c': 3}


def test_datetime_still_rejected_by_date_column_exactness():
    from datetime import date, datetime
    v = Vector([date(2024, 1, 1)])
    # datetime into a date column goes through promotion (kind widens to
    # datetime), NOT silent subclass acceptance.
    v[0] = datetime(2024, 1, 1, 12, 0)
    assert v.schema().kind is datetime


# ---------------------------------------------------------------------------
# copy(()) means "empty copy", not "copy the original"
# ---------------------------------------------------------------------------

def test_table_copy_empty_tuple_is_empty():
    t = Table({'a': [1, 2]})
    e = t.copy(())
    assert len(e) == 0
