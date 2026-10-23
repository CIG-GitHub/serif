"""
is_na() answers from the validity bitmap for every masked storage.

The numpy path unpacks the bitmap in one call; without numpy the pure
generator walks it. Either way no value is boxed, and the result must be
identical: one bool per lane, never nullable, padding bits ignored.
"""

from datetime import date
from datetime import datetime
from decimal import Decimal

import pytest

from serif import Schema
from serif import Vector
from serif._execution import DECLINED
from serif._vector import dispatch
from serif._vector._numpy import storage as numpy_storage
from serif._vector.storage import BoolStorage
from serif._vector.storage import DateStorage
from serif._vector.storage import DecimalStorage


def _with_nulls(value, length):
    return [None if i % 3 == 1 or i == length - 1 else value
            for i in range(length)]


def _decimal_column(values):
    # Python-built Decimal columns are tuple-backed; DecimalStorage comes
    # from Parquet/Arrow, so build it directly.
    storage = DecimalStorage.from_iterable(
        values, scale=2, precision=4, nullable=True)
    return Vector._from_storage(storage, Schema(Decimal, True))


KINDS = [
    ("int", 7, Vector),
    ("float", 1.5, Vector),
    ("str", 'x', Vector),
    ("bool", True, Vector),
    ("date", date(2024, 1, 2), Vector),
    ("datetime", datetime(2024, 1, 2, 3), Vector),
    ("decimal", Decimal('1.25'), _decimal_column),
]
KIND_IDS = [k[0] for k in KINDS]


@pytest.fixture(params=['accelerated', 'pure'])
def backend(request, monkeypatch):
    if request.param == 'pure':
        monkeypatch.setattr(numpy_storage, '_USE_NUMPY', False)
    return request.param


@pytest.mark.parametrize("value,make", [k[1:] for k in KINDS], ids=KIND_IDS)
@pytest.mark.parametrize("length", [1, 7, 8, 9, 17])
def test_is_na_reads_the_bitmap(backend, value, make, length):
    values = _with_nulls(value, length)

    result = make(values).is_na()

    assert type(result._storage) is BoolStorage
    assert result.schema() == Schema(bool, False)
    assert list(result) == [x is None for x in values]


def test_dense_column_is_all_false(backend):
    assert list(Vector([date(2024, 1, 1)] * 3).is_na()) == [False] * 3


def test_padding_bits_never_read_as_lanes(backend):
    v = Vector([1, None, 3])
    v._storage._mask._buf[0] |= 0b11111000

    assert list(v.is_na()) == [False, True, False]


def test_masked_storage_never_boxes(backend, monkeypatch):
    def fail(*args):
        raise AssertionError('is_na boxed a value')

    expected = [x is None for x in _with_nulls(1, 10)]
    dates = Vector(_with_nulls(date(2024, 1, 2), 10))
    decimals = _decimal_column(_with_nulls(Decimal('1.25'), 10))

    monkeypatch.setattr(DateStorage, '_box', staticmethod(fail))
    monkeypatch.setattr(DecimalStorage, '__iter__', fail)
    assert list(dates.is_na()) == expected
    assert list(decimals.is_na()) == expected


def test_numpy_unpacks_the_bitmap(monkeypatch):
    pytest.importorskip("numpy")
    calls = []
    original = numpy_storage.null_bytes

    def spy(mask, n):
        result = original(mask, n)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(numpy_storage, 'null_bytes', spy)
    for _, value, make in KINDS:
        make(_with_nulls(value, 9)).is_na()
    assert calls == [True] * len(KINDS)


def test_dispatch_declines_without_numpy(monkeypatch):
    monkeypatch.setattr(numpy_storage, '_USE_NUMPY', False)
    mask = Vector([1, None])._storage._mask

    assert dispatch.null_bytes(mask, 2) is DECLINED
