"""
Date vector arithmetic.

Python semantics first: date + timedelta and date - date are core Python and
must work on vectors. serif's own extension (int means days, Excel-style) is
pinned here too, symmetric across + and -.
"""

from datetime import date, datetime, timedelta

import pytest

from serif import Vector

D1 = date(2024, 1, 1)
D2 = date(2024, 1, 2)
D3 = date(2024, 1, 3)


# ---------------------------------------------------------------------------
# timedelta — core Python semantics
# ---------------------------------------------------------------------------

def test_date_plus_timedelta_scalar():
    assert list(Vector([D1, D2]) + timedelta(days=1)) == [D2, D3]


def test_timedelta_plus_date_vector():
    assert list(timedelta(days=1) + Vector([D1, D2])) == [D2, D3]


def test_date_plus_timedelta_vector():
    tds = Vector([timedelta(days=2), timedelta(days=1)])
    assert list(Vector([D1, D2]) + tds) == [D3, D3]


def test_date_minus_timedelta_scalar():
    assert list(Vector([D2, D3]) - timedelta(days=1)) == [D1, D2]


def test_date_minus_date_vector_gives_timedeltas():
    result = Vector([D3, D2]) - Vector([D1, D1])
    assert list(result) == [timedelta(days=2), timedelta(days=1)]


def test_date_plus_timedelta_keeps_date_kind():
    result = Vector([D1]) + timedelta(days=1)
    assert result.schema().kind is date


def test_date_plus_timedelta_propagates_none():
    assert list(Vector([D1, None]) + timedelta(days=1)) == [D2, None]


def test_datetime_vector_plus_timedelta():
    dt = datetime(2024, 1, 1, 12, 0)
    result = Vector([dt]) + timedelta(hours=6)
    assert list(result) == [datetime(2024, 1, 1, 18, 0)]


# ---------------------------------------------------------------------------
# int means days — serif's Excel-style extension, symmetric
# ---------------------------------------------------------------------------

def test_date_plus_int_days():
    assert list(Vector([D1, D2]) + 1) == [D2, D3]


def test_date_minus_int_days():
    assert list(Vector([D2, D3]) - 1) == [D1, D2]


def test_date_plus_int_vector():
    assert list(Vector([D1, D1]) + Vector([1, 2])) == [D2, D3]


def test_date_minus_int_vector():
    assert list(Vector([D3, D3]) - Vector([1, 2])) == [D2, D1]


def test_date_int_days_propagates_none():
    assert list(Vector([D1, None]) + 1) == [D2, None]


# ---------------------------------------------------------------------------
# Incompatible operands
# ---------------------------------------------------------------------------

def test_date_plus_string_raises_typeerror():
    with pytest.raises(TypeError):
        Vector([D1]) + 'x'


def test_date_plus_float_raises_typeerror():
    with pytest.raises(TypeError):
        Vector([D1]) + 1.5
