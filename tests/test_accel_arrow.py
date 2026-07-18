"""
Tests for the OPTIONAL pyarrow bridge (serif._accel.arrow).

Commit-1 plumbing only: nothing user-facing routes through the bridge
yet, so these tests exercise it at the storage level — round-trip
identity (wrap a StringStorage, read identical values back through
arrow; unwrap an arrow bool array, read identical values back through
BoolStorage), zero-copy-ness (the arrow array's buffers ARE serif's
buffers, same addresses), and every decline path.

Skipped entirely when pyarrow isn't installed.
"""

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.compute as pc

from serif._accel import arrow as bridge
from serif._vector.nullable import BitMask
from serif._vector.storage import BoolStorage, StringStorage


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
    assert bridge.string_array(StringStorage.from_iterable([])) is None


def test_string_array_declines_non_string_storage():
    assert bridge.string_array(BoolStorage.from_iterable([True])) is None


def test_string_array_declines_when_switched_off(monkeypatch):
    s = StringStorage.from_iterable(['a'])
    monkeypatch.setattr(bridge, '_USE_ARROW', False)
    assert bridge.string_array(s) is None


def test_string_array_declines_offsets_past_int32(monkeypatch):
    # Arrow string offsets are SIGNED int32; serif's are uint32. The guard
    # compares the (monotonic) last offset against the int32 ceiling.
    # Building a real 2 GiB column to test one comparison is not happening
    # (and arrow's from_buffers validation rejects a storage that lies
    # about its buffer), so lower the ceiling instead and probe both sides.
    monkeypatch.setattr(bridge, '_I32_MAX', 3)
    over = StringStorage.from_iterable(['ab', 'cd'])   # last offset 4 > 3
    assert bridge.string_array(over) is None
    at = StringStorage.from_iterable(['ab', 'c'])      # last offset 3 == 3
    assert bridge.string_array(at) is not None


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
    assert bridge.bool_storage(arr) is None


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
