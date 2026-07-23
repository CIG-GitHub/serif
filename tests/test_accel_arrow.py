"""
Tests for the OPTIONAL Vector pyarrow storage bridge.

These tests exercise the physical storage boundary directly — round-trip
identity (wrap a StringStorage, read identical values back through
arrow; unwrap an arrow bool array, read identical values back through
BoolStorage), zero-copy-ness (the arrow array's buffers ARE serif's
buffers, same addresses), and every decline path.

Skipped entirely when pyarrow isn't installed.
"""

from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.compute as pc

from serif._execution import DECLINED
from serif._vector._arrow import storage as bridge
from serif._vector.nullable import BitMask
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage
from serif._vector.storage import DecimalStorage
from serif._vector.storage import StringStorage


# ---------------------------------------------------------------------------
# string_array — StringStorage → pa.StringArray
# ---------------------------------------------------------------------------

def test_string_array_round_trip():
    values = ['apple', 'banana', '', 'cherry', 'a' * 100]
    arr = bridge.string_array(StringStorage.from_iterable(values))
    assert arr.to_pylist() == values
    assert arr.null_count == 0


def test_string_array_round_trip_nulls():
    values = ['α', None, '', '🎉🎉', None, '日本語', 'plain']
    arr = bridge.string_array(StringStorage.from_iterable(values))
    assert arr.to_pylist() == values
    assert arr.null_count == 2


def test_string_array_null_only_column():
    values = [None, None, None]
    arr = bridge.string_array(StringStorage.from_iterable(values))
    assert arr.to_pylist() == values
    assert arr.null_count == 3


def test_string_array_is_zero_copy():
    s = StringStorage.from_iterable(['aa', 'bbb', 'c'])
    arr = bridge.string_array(s)
    # The arrow array's buffers ARE serif's buffers — same addresses,
    # no copies. buffers() = [validity, offsets, data].
    assert arr.buffers()[1].address == pa.py_buffer(s._offsets).address
    assert arr.buffers()[2].address == pa.py_buffer(s._buf).address


def test_string_array_validity_is_the_bitmask_buffer():
    s = StringStorage.from_iterable(['x', None, 'y'])
    arr = bridge.string_array(s)
    assert arr.buffers()[0].address == pa.py_buffer(s._mask._buf).address


def test_string_array_declines_empty():
    assert bridge.string_array(StringStorage.from_iterable([])) is DECLINED


def test_string_array_declines_non_string_storage():
    assert bridge.string_array(BoolStorage.from_iterable([True])) is DECLINED


def test_string_array_declines_when_switched_off(monkeypatch):
    s = StringStorage.from_iterable(['a'])
    monkeypatch.setattr(bridge, '_USE_ARROW', False)
    assert bridge.string_array(s) is DECLINED


def test_string_array_declines_offsets_past_int32(monkeypatch):
    # Arrow string offsets are SIGNED int32; serif's are uint32. The guard
    # compares the (monotonic) last offset against the int32 ceiling.
    # Building a real 2 GiB column to test one comparison is not happening
    # (and arrow's from_buffers validation rejects a storage that lies
    # about its buffer), so lower the ceiling instead and probe both sides.
    monkeypatch.setattr(bridge, '_I32_MAX', 3)
    over = StringStorage.from_iterable(['ab', 'cd'])   # last offset 4 > 3
    assert bridge.string_array(over) is DECLINED
    at = StringStorage.from_iterable(['ab', 'c'])      # last offset 3 == 3
    assert bridge.string_array(at) is not DECLINED


# ---------------------------------------------------------------------------
# Arrow arrays → canonical Serif storage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "values,arrow_type,typecode",
    [
        ([1, None, -3], pa.int64(), 'q'),
        ([1.5, None, -2.25], pa.float64(), 'd'),
    ],
    ids=['int64', 'float64'],
)
def test_numeric_storage_round_trip(values, arrow_type, typecode):
    storage = bridge.numeric_storage(pa.array(values, type=arrow_type))
    assert type(storage) is ArrayStorage
    assert storage._data.typecode == typecode
    assert list(storage) == values


def test_numeric_storage_empty_and_declines():
    storage = bridge.numeric_storage(pa.array([], type=pa.int64()))
    assert type(storage) is ArrayStorage
    assert storage._data.typecode == 'q'
    assert storage._mask is None
    assert bridge.numeric_storage(pa.array(['a'])) is DECLINED
    assert bridge.numeric_storage(pa.array([1, 2, 3]).slice(1)) is DECLINED


def test_string_storage_round_trip():
    values = ['α', None, '', '🎉', '日本語']
    storage = bridge.string_storage(pa.array(values, type=pa.string()))
    expected = StringStorage.from_iterable(values)
    assert type(storage) is StringStorage
    assert list(storage) == values
    assert storage._buf == expected._buf
    assert tuple(storage._offsets) == tuple(expected._offsets)
    assert bytes(storage._mask._buf) == bytes(expected._mask._buf)


def test_string_storage_empty_and_declines():
    storage = bridge.string_storage(pa.array([], type=pa.string()))
    assert type(storage) is StringStorage
    assert tuple(storage._offsets) == (0,)
    assert storage._mask is None
    assert bridge.string_storage(pa.array(['a'], type=pa.large_string())) is DECLINED
    assert bridge.string_storage(pa.array(['a', 'b']).slice(1)) is DECLINED


def test_decimal_storage_round_trip():
    arrow_type = pa.decimal128(6, 2)
    values = [Decimal('123.45'), None, Decimal('-0.01')]
    storage = bridge.decimal_storage(pa.array(values, type=arrow_type))
    assert type(storage) is DecimalStorage
    assert list(storage) == values
    assert storage._scale == 2
    assert storage._precision == 6


def test_decimal_storage_empty_and_declines():
    storage = bridge.decimal_storage(
        pa.array([], type=pa.decimal128(9, 3)))
    assert type(storage) is DecimalStorage
    assert len(storage) == 0
    assert storage._scale == 3
    assert storage._precision == 9
    assert storage._mask is None
    assert bridge.decimal_storage(
        pa.array([Decimal('1')], type=pa.decimal256(40, 0))) is DECLINED
    assert bridge.decimal_storage(
        pa.array([Decimal('1'), Decimal('2')],
                 type=pa.decimal128(4, 0)).slice(1)) is DECLINED


# ---------------------------------------------------------------------------
# bool_storage — pa.BooleanArray → BoolStorage
# ---------------------------------------------------------------------------

def test_bool_storage_round_trip():
    values = [True, False, None, True, None] * 7   # crosses byte boundaries
    st = bridge.bool_storage(pa.array(values, type=pa.bool_()))
    assert type(st) is BoolStorage
    assert list(st) == values
    assert all(type(v) is bool for v in st if v is not None)


def test_bool_storage_all_valid_has_no_mask():
    st = bridge.bool_storage(pa.array([True, False, True]))
    assert st._mask is None
    assert list(st) == [True, False, True]


def test_bool_storage_empty():
    st = bridge.bool_storage(pa.array([], type=pa.bool_()))
    assert type(st) is BoolStorage
    assert len(st) == 0
    assert st._mask is None


def test_bool_storage_declines_sliced_array():
    arr = pa.array([True, False, None, True]).slice(1)
    assert arr.offset != 0          # the premise of the decline
    assert bridge.bool_storage(arr) is DECLINED


def test_bool_storage_declines_non_boolean_array():
    assert bridge.bool_storage(pa.array([1, 0], type=pa.int64())) is DECLINED


def test_storage_reconstruction_without_numpy(monkeypatch):
    monkeypatch.setattr(bridge, '_np', None)
    assert list(bridge.bool_storage(
        pa.array([True, None, False]))) == [True, None, False]
    assert list(bridge.decimal_storage(pa.array(
        [Decimal('1.25'), None, Decimal('-2.50')],
        type=pa.decimal128(4, 2),
    ))) == [Decimal('1.25'), None, Decimal('-2.50')]


# ---------------------------------------------------------------------------
# bitmask — arrow validity → BitMask
# ---------------------------------------------------------------------------

def test_bitmask_round_trip():
    values = ['a', None, 'b', None, None, 'c', 'd', 'e', None]  # > 1 byte
    arr = pa.array(values, type=pa.string())
    mask = bridge.bitmask(arr)
    assert type(mask) is BitMask
    assert len(mask) == len(values)
    assert [mask.is_null(i) for i in range(len(values))] == \
           [v is None for v in values]


def test_bitmask_none_when_no_nulls():
    assert bridge.bitmask(pa.array(['a', 'b'])) is None


# ---------------------------------------------------------------------------
# End-to-end smoke: wrap → kernel → unwrap
# ---------------------------------------------------------------------------

def test_kernel_smoke():
    values = ['apple', None, 'banana', 'apple', '', None, 'Apple']
    s = StringStorage.from_iterable(values)
    st = bridge.bool_storage(pc.equal(bridge.string_array(s), 'apple'))
    expected = [None if v is None else v == 'apple' for v in values]
    assert list(st) == expected
