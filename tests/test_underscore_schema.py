"""
Table._ — the column schema listing that replaced peek().

One row per column: dot-accessor name, dtype (with schema params —
'Decimal(4,2)?', 'category(3)'), the category ordering where one exists,
and the original name where sanitization structurally changed it.
Left-aligned columns; optional columns disappear entirely when empty.
Metadata only — no data scan — and every column is shown (up to 1000).
"""

from datetime import date

import pytest

from serif import Table


def lines(t):
    return repr(t._).splitlines()


# ---------------------------------------------------------------------------
# Clean names: two columns, no originals
# ---------------------------------------------------------------------------

def test_clean_names_collapse_to_two_columns():
    t = Table({
        'integers': [1, 2],
        'squares': [1, 4],
        'created': [date(2026, 1, 1), date(2026, 1, 2)],
        'city': ['a', 'b'],
    })
    assert lines(t) == [
        '.integers   int',
        '.squares    int',
        '.created    date',
        '.city       str',
    ]


def test_reserved_name_collision_shows_original():
    # 'last' collides with the Vector.last() method, so it sanitizes to
    # 'last_' — a structural change, so the original is shown. ('name' is no
    # longer reserved — the name property is .vector_name / .table_name now.)
    with pytest.warns(UserWarning, match="reserved"):
        t = Table({'last': ['a']})
    assert lines(t) == [".last_   str   'last'"]


def test_no_quotes_when_nothing_sanitized():
    assert "'" not in repr(Table({'a': [1], 'b': [2]})._)


# ---------------------------------------------------------------------------
# Sanitized names: third column with quoted originals
# ---------------------------------------------------------------------------

def test_sanitized_names_show_original():
    t = Table({
        'some string': ['a', 'the bravo'],
        'some other strings': ['d', 'e'],
        'a number': [1, 2],
        'a float': [2.3, 3.3],
    })
    assert lines(t) == [
        ".some_string          str     'some string'",
        ".some_other_strings   str     'some other strings'",
        ".a_number             int     'a number'",
        ".a_float              float   'a float'",
    ]


def test_original_only_on_rows_that_changed():
    t = Table({'clean': [1], 'needs fix': [2]})
    out = lines(t)
    assert out[0] == '.clean       int'
    assert out[1] == ".needs_fix   int   'needs fix'"


def test_case_only_change_is_not_structural():
    # 'City' -> .city is a case change, not a structural one: no third column
    t = Table({'City': ['a']})
    assert lines(t) == ['.city   str']


def test_duplicate_names_show_disambiguated_accessor():
    t = Table({'x': [1]})
    with pytest.warns(UserWarning, match="Duplicate column name 'x'"):
        t2 = Table([t.cols()[0], t.cols()[0]])
    out = lines(t2)
    assert out[0].startswith('.x ')
    assert '.x__1' in out[1]


# ---------------------------------------------------------------------------
# Dtypes
# ---------------------------------------------------------------------------

def test_nullable_dtype_shows_question_mark():
    t = Table({'a': [1, None]})
    assert lines(t) == ['.a   int?']


# ---------------------------------------------------------------------------
# Schema params: t._ is the EXACT view. The table footer is deliberately
# lossy ('Decimal*' = mixed flavors) and points here, so here is where
# decimal precision/scale and category cardinality + ordering must show.
# ---------------------------------------------------------------------------

def _decimal_col(name, values, scale, precision, nullable=False):
    from serif import Vector
    from serif._vector.dtype import Schema
    from serif._vector.storage import DecimalStorage
    from decimal import Decimal
    storage = DecimalStorage.from_iterable(values, scale, precision,
                                           nullable=nullable)
    return Vector._from_storage(storage, Schema(Decimal, nullable), name=name)


def test_decimal_storage_shows_precision_and_scale():
    from decimal import Decimal
    t = Table._from_columns_nocopy([
        _decimal_col('amt', [Decimal('12.34')], 2, 4),
        _decimal_col('rate', [Decimal('0.00012345'), None], 8, 9, nullable=True),
    ])
    assert lines(t) == [
        '.amt    Decimal(4,2)',
        '.rate   Decimal(9,8)?',
    ]


def test_tuple_decimal_shows_bare_kind():
    # A TupleStorage decimal column declares no fixed scale — showing
    # invented params would lie, so the kind stays bare.
    from decimal import Decimal
    t = Table({'amt': [Decimal('12.34'), Decimal('5.678')]})
    assert lines(t) == ['.amt   Decimal']


def test_category_shows_cardinality_and_order():
    from serif._vector.categorical import _Category
    grade = _Category.from_values(['low', 'mid'], ['low', 'mid', 'high'],
                                  name='grade')
    t = Table._from_columns_nocopy([grade])
    assert lines(t) == ['.grade   category(3)   low < mid < high']


def test_category_long_order_elides_middle():
    from serif._vector.categorical import _Category
    cats = [f'cat_{i:02d}' for i in range(20)]
    c = _Category.from_values([cats[0]], cats, name='c')
    t = Table._from_columns_nocopy([c])
    assert lines(t) == ['.c   category(20)   cat_00 < cat_01 < … < cat_19']


def test_category_order_and_original_name_coexist():
    from serif._vector.categorical import _Category
    grade = _Category.from_values(['low'], ['low', 'high'], name='Grade Level')
    t = Table._from_columns_nocopy([grade])
    # Ordering hugs the dtype it annotates; the original name stays last.
    assert lines(t) == [".grade_level   category(2)   low < high   'Grade Level'"]


# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------

def test_empty_table():
    assert repr(Table({})._) == '# 0×0 table'


def test_all_columns_shown_no_row_truncation():
    t = Table({f'c{i}': [1] for i in range(100)})
    assert len(lines(t)) == 100
    assert '...' not in repr(t._)


def test_more_than_1000_columns_folds_tail():
    t = Table({f'c{i}': [1] for i in range(1005)})
    out = lines(t)
    assert len(out) == 1001
    assert out[-1] == '... (+5 more columns)'


def test_peek_is_gone():
    assert not hasattr(Table({'a': [1]}), 'peek')
