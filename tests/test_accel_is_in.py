"""
is_in() through numpy (numbers, date ticks) and Arrow (strings).

Membership is Python's `==`, and the pure loop is the specification: every
accelerated result must match it exactly, including the int/float corners
numpy would otherwise blur (2**53 + 1 is not 2.0**53), nan never matching,
bool members counting as 0/1, and null lanes (null, or True when None is a
member). Members only Python can judge (Decimal, subclasses) decline.
"""

from datetime import date
from datetime import datetime
from decimal import Decimal

import pytest

from serif import Schema
from serif import Vector
from serif._execution import DECLINED
from serif._vector._arrow import transforms as arrow_transforms
from serif._vector._numpy import transforms as numpy_transforms


def _pure(fn, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(numpy_transforms, '_USE_NUMPY', False)
        m.setattr(arrow_transforms, '_USE_ARROW', False)
        return fn()


def _assert_matches_pure(make, members, monkeypatch):
    fast = make().is_in(members)
    pure = _pure(lambda: make().is_in(members), monkeypatch)
    assert list(fast) == list(pure)
    assert fast.schema() == pure.schema()
    return fast


NAN = float('nan')

CASES = [
    ("int", lambda: Vector([3, None, 2, 2**53 + 1, -7, 0]),
     [3, 2.0, 2.5, True, 2**70, NAN, float('inf'), float(2**53)]),
    ("int_match_null", lambda: Vector([3, None, 2]), [2, None]),
    ("float", lambda: Vector([1.5, None, -0.0, NAN, 2.0**53, 1.0]),
     [0.0, 1.5, 2**53 + 1, True, NAN]),
    ("float_exact_big_int", lambda: Vector([2.0**53, 2.0**60]), [2**53, 2**60 + 1]),
    ("date", lambda: Vector([date(2024, 1, 2), None, date(1969, 12, 31)]),
     [date(1969, 12, 31), date(2000, 1, 1)]),
    ("date_match_null", lambda: Vector([date(2024, 1, 2), None]),
     [None, date(2024, 1, 2)]),
    ("datetime", lambda: Vector([datetime(2024, 1, 2, 3), None, datetime(1, 1, 1)]),
     [datetime(1, 1, 1), datetime(2024, 1, 2, 3, 0, 0, 1)]),
    ("str", lambda: Vector(['a', None, '', 'héllo 🐍', 'b']), ['', 'héllo 🐍', 'z']),
    ("str_match_null", lambda: Vector(['a', None, 'b']), ['b', None]),
    ("empty_members", lambda: Vector([1, None, 3]), []),
    ("decimal_member_declines", lambda: Vector([3, None, 4]), [Decimal(3)]),
    ("bool_column", lambda: Vector([True, None, False]), [1]),
    ("sliced_mask_without_nulls", lambda: Vector([1, None, 3, 4])[2:], [3]),
]


@pytest.mark.parametrize("make,members", [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_is_in_matches_python(make, members, monkeypatch):
    _assert_matches_pure(make, members, monkeypatch)


def test_exact_int_float_corners():
    v = Vector([2**53 + 1, 2**53])
    assert list(v.is_in([float(2**53)])) == [False, True]

    f = Vector([2.0**53])
    assert list(f.is_in([2**53 + 1])) == [False]
    assert list(f.is_in([2**53])) == [True]


def test_nulls_are_null_unless_none_is_a_member():
    v = Vector([1, None])
    assert list(v.is_in([1])) == [True, None]
    assert v.is_in([1]).schema() == Schema(bool, True)
    assert list(v.is_in([1, None])) == [True, True]
    assert v.is_in([1, None]).schema() == Schema(bool, False)


def _spy(monkeypatch, module, name, calls):
    original = getattr(module, name)

    def wrapper(*args):
        result = original(*args)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(module, name, wrapper)


def test_numpy_kernel_engages(monkeypatch):
    pytest.importorskip("numpy")
    calls = []
    _spy(monkeypatch, numpy_transforms, 'is_in', calls)
    Vector([1, None]).is_in([1])
    Vector([1.5, None]).is_in([1.5])
    Vector([date(2024, 1, 1), None]).is_in([date(2024, 1, 1)])
    Vector([datetime(2024, 1, 1), None]).is_in([datetime(2024, 1, 1)])
    assert calls == [True] * 4

    calls.clear()
    Vector([3, 4]).is_in([Decimal(3)])          # only Python judges Decimal
    assert calls == [False]


def test_arrow_kernel_engages_for_strings(monkeypatch):
    pytest.importorskip("pyarrow")
    calls = []
    _spy(monkeypatch, arrow_transforms, 'is_in_strings', calls)
    Vector(['a', None, 'b']).is_in(['b'])
    assert calls == [True]
