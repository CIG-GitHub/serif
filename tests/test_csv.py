"""
read_csv coverage: input forms, structure handling, and type inference.

Inference doctrine pinned here:
- A cell is a number only if it LOOKS like one (ASCII digits, optional sign,
  decimal point, exponent). Python-literal quirks that CSV cells never mean —
  underscores ("1_000"), nan/inf words — stay strings.
- Leading-zero integer forms ("0123") are identifiers, not numbers. Eating
  the zeros is data loss (the classic Excel complaint), so they stay strings.
- No bool or date inference: "True" and "2024-01-01" stay strings.
- Empty / whitespace-only cells are None.
- Rows longer than the header raise (silent field-dropping is data loss);
  shorter rows pad with None.

Deliberately NOT pinned: columns whose first cell is empty (inference
order-dependence is a known issue slated for a later group).
"""

import io
from datetime import date

import pytest

from serif import Table, read_csv
from serif.errors import SerifValueError


def _read(text, **kwargs):
    return read_csv(io.StringIO(text), **kwargs)


# ---------------------------------------------------------------------------
# Input forms
# ---------------------------------------------------------------------------

def test_read_from_file_object():
    t = _read("a,b\n1,x\n2,y\n")
    assert t.column_names() == ['a', 'b']
    assert list(t.a) == [1, 2]
    assert list(t.b) == ['x', 'y']


def test_read_from_path(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text("a,b\n1,2\n", encoding='utf-8')
    t = read_csv(str(p))
    assert list(t.a) == [1]
    assert list(t.b) == [2]


def test_bom_not_in_header_names(tmp_path):
    p = tmp_path / "bom.csv"
    p.write_bytes(b'\xef\xbb\xbfa,b\n1,2\n')
    t = read_csv(str(p))
    assert t.column_names() == ['a', 'b']


def test_delimiter():
    t = _read("a\tb\n1\t2\n", delimiter='\t')
    assert list(t.a) == [1]
    assert list(t.b) == [2]


def test_no_header_generates_names():
    t = _read("1,2\n3,4\n", has_header=False)
    assert t.column_names() == ['col_0', 'col_1']
    assert list(t['col_0']) == [1, 3]


def test_quoted_field_containing_delimiter():
    t = _read('a,b\n"x,y",2\n')
    assert list(t.a) == ['x,y']


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------

def test_empty_file_returns_empty_table():
    t = _read("")
    assert isinstance(t, Table)
    assert len(t) == 0


def test_header_only_returns_empty_columns():
    t = _read("a,b\n")
    assert t.column_names() == ['a', 'b']
    assert len(t) == 0


def test_short_rows_pad_with_none():
    t = _read("a,b,c\n1,2,3\n4,5\n")
    assert list(t.c) == [3, None]


def test_long_rows_raise():
    with pytest.raises(SerifValueError, match=r'[Rr]ow 3'):
        _read("a,b\n1,2\n1,2,3\n")


# ---------------------------------------------------------------------------
# Type inference — numbers
# ---------------------------------------------------------------------------

def test_int_column():
    t = _read("a\n1\n-5\n+7\n0\n")
    assert list(t.a) == [1, -5, 7, 0]
    assert t.a.schema().kind is int


def test_float_column():
    t = _read("a\n1.5\n-2.25\n.5\n")
    assert list(t.a) == [1.5, -2.25, 0.5]
    assert t.a.schema().kind is float


def test_scientific_notation_is_float():
    t = _read("a\n1e3\n2E-2\n")
    assert list(t.a) == [1000.0, 0.02]
    assert t.a.schema().kind is float


def test_mixed_int_float_promotes_to_float():
    t = _read("a\n1\n2.5\n")
    assert list(t.a) == [1.0, 2.5]
    assert t.a.schema().kind is float


def test_whitespace_around_numbers_is_trimmed():
    t = _read("a\n 42 \n")
    assert list(t.a) == [42]


# ---------------------------------------------------------------------------
# Type inference — things that must STAY strings
# ---------------------------------------------------------------------------

@pytest.mark.filterwarnings("error")
def test_leading_zero_identifiers_stay_strings():
    # One leading-zero cell marks the whole column as identifiers: 90210
    # must not become an int alongside '01234'. Clean str column, and no
    # degrade-to-object warning (filterwarnings turns any warning into a
    # failure).
    t = _read("zip\n01234\n90210\n")
    assert list(t.zip) == ['01234', '90210']
    assert t.zip.schema().kind is str


def test_underscore_numerals_stay_strings():
    t = _read("a\n1_000\n")
    assert list(t.a) == ['1_000']


@pytest.mark.parametrize("token", ["nan", "NaN", "inf", "-inf", "Infinity"])
def test_nan_inf_words_stay_strings(token):
    t = _read(f"a\n{token}\n")
    assert list(t.a) == [token]
    assert t.a.schema().kind is str


def test_bool_words_stay_strings():
    t = _read("a\nTrue\nFalse\n")
    assert list(t.a) == ['True', 'False']


def test_date_strings_stay_strings():
    t = _read("a\n2024-01-01\n")
    assert list(t.a) == ['2024-01-01']
    assert t.a.schema().kind is str


def test_non_ascii_digits_stay_strings():
    t = _read("a\n١٢٣\n")  # Arabic-Indic ١٢٣
    assert t.a.schema().kind is str


# ---------------------------------------------------------------------------
# Nulls and mixed columns
# ---------------------------------------------------------------------------

def test_empty_cell_is_none_and_column_nullable():
    t = _read("a\n1\n\n3\n")
    assert list(t.a) == [1, None, 3]
    assert t.a.schema().kind is int
    assert t.a.schema().nullable is True


def test_whitespace_only_cell_is_none():
    t = _read("a,b\n1, \n2,x\n")
    assert list(t.b) == [None, 'x']


def test_mixed_number_and_string_degrades_to_object_with_warning():
    with pytest.warns(UserWarning, match='[Dd]egrading'):
        t = _read("a\n1\nhello\n")
    assert list(t.a) == [1, 'hello']
    assert t.a.schema().kind is object
