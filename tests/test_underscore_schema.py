"""
Table._ — the column schema listing that replaced peek().

One row per column: dot-accessor name, dtype, and the original name where
sanitization structurally changed it. Left-aligned columns; the original-
name column disappears entirely when no name was changed. Metadata only —
no data scan — and every column is shown (up to 1000).
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
    # 'name' collides with the Vector.name property, so it sanitizes to
    # 'name_' — a structural change, so the original is shown
    with pytest.warns(UserWarning, match="reserved"):
        t = Table({'name': ['a']})
    assert lines(t) == [".name_   str   'name'"]


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
