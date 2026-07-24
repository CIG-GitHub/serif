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
    file_obj = io.StringIO("a,b\n1,x\n2,y\n")
    t = read_csv(file_obj)
    assert t.column_names() == ['a', 'b']
    assert list(t.a) == [1, 2]
    assert list(t.b) == ['x', 'y']
    assert file_obj.closed is False


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


def test_bom_not_in_first_value_without_header(tmp_path):
    p = tmp_path / "bom-no-header.csv"
    p.write_bytes(b'\xef\xbb\xbf1,2\n3,4\n')
    t = read_csv(str(p), has_header=False)
    assert list(t['col_0']) == [1, 3]
    assert list(t['col_1']) == [2, 4]


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
    assert list(t.a) == []
    assert list(t.b) == []


def test_short_rows_pad_with_none():
    t = _read("a,b,c\n1,2,3\n4,5\n6\n")
    assert list(t.a) == [1, 4, 6]
    assert list(t.b) == [2, 5, None]
    assert list(t.c) == [3, None, None]


@pytest.mark.parametrize(
    ("text", "has_header", "physical_row"),
    [
        ("a,b\n1,2\n3,4,5\n", True, 3),
        ("1,2\n3,4,5\n", False, 2),
    ],
)
def test_long_rows_raise_with_physical_row_number(
    text, has_header, physical_row
):
    message = (
        rf"Row {physical_row} has 3 fields, but the header has 2 columns\."
    )
    with pytest.raises(SerifValueError, match=message):
        _read(text, has_header=has_header)


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


def test_trailing_decimal_and_signed_exponent_are_float():
    t = _read("a\n3.\n+4.5e+1\n")
    assert list(t.a) == [3.0, 45.0]
    assert t.a.schema().kind is float


def test_mixed_int_float_promotes_to_float():
    t = _read("a\n1\n2.5\n")
    assert list(t.a) == [1.0, 2.5]
    assert t.a.schema().kind is float


def test_whitespace_around_numbers_is_trimmed():
    t = _read("a\n 42 \n")
    assert list(t.a) == [42]


def test_whitespace_around_strings_is_trimmed():
    t = _read("a\n  café  \n")
    assert list(t.a) == ['café']
    assert t.a.schema().kind is str


def test_unicode_header_and_values():
    t = _read("café,emoji\nnaïve,🙂\n")
    assert t.column_names() == ['café', 'emoji']
    assert list(t['café']) == ['naïve']
    assert list(t.emoji) == ['🙂']


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


@pytest.mark.filterwarnings("error")
def test_late_leading_zero_marks_the_whole_column_as_identifiers():
    t = _read("zip\n90210\n10001\n01234\n")
    assert list(t.zip) == ['90210', '10001', '01234']
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
