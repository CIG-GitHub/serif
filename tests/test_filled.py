"""
Vector.filled builds its storage in bulk.

The spec is the per-element build it replaced: storage_from_known_iterable
over repeat(value, n). Every result must match it physically (storage type,
buffers, mask) and logically (values, schema), including the values the bulk
forms can't hold, which still degrade or warn exactly as before.
"""

from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from itertools import repeat

import pytest

from serif import Schema
from serif import Vector
from serif._vector.dtype import infer_dtype
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage
from serif._vector.storage import DateStorage
from serif._vector.storage import DatetimeStorage
from serif._vector.storage import StringStorage
from serif._vector.storage import TupleStorage
from serif._vector.storage import storage_from_known_iterable


def _signature(storage):
    if isinstance(storage, ArrayStorage):
        data = (storage._data.typecode, storage._data.tobytes())
    elif isinstance(storage, BoolStorage):
        data = bytes(storage._data)
    elif isinstance(storage, StringStorage):
        data = (bytes(storage._buf), tuple(storage._offsets))
    elif isinstance(storage, (DateStorage, DatetimeStorage)):
        data = (storage._ticks.typecode, storage._ticks.tobytes())
    else:
        data = storage._data
    mask = getattr(storage, '_mask', None)   # TupleStorage has none
    mask = None if mask is None else bytes(mask._buf)
    return type(storage), data, mask


def _reference(value, n):
    return storage_from_known_iterable(repeat(value, n), infer_dtype([value]).kind)


@pytest.mark.parametrize("value", [
    7, -2**63, 2**63 - 1, True, False, 1.5, -0.0,
    'x', 'héllo 🐍', '',
    date(1969, 12, 31), datetime(2024, 1, 2, 3, 4, 5, 6),
    Decimal('1.25'), b'raw',
], ids=repr)
@pytest.mark.parametrize("n", [1, 2, 9, 1000])
def test_filled_matches_per_element_build(value, n):
    v = Vector.filled(value, n)

    assert _signature(v._storage) == _signature(_reference(value, n))
    assert v.schema() == infer_dtype([value])
    assert list(v) == [value] * n


def test_filled_none_is_untyped_nullable():
    v = Vector.filled(None, 3)

    assert type(v._storage) is TupleStorage
    assert v.schema() == Schema(object, True)
    assert list(v) == [None, None, None]


def test_filled_typesafe_keeps_kind_not_nullable():
    v = Vector.filled(date(2024, 1, 1), 4, typesafe=True)

    assert type(v._storage) is DateStorage
    assert v.schema() == Schema(date, False)


def test_filled_beyond_int64_degrades_exactly():
    huge = 2**80
    v = Vector.filled(huge, 3)

    assert type(v._storage) is TupleStorage
    assert list(v) == [huge] * 3


def test_filled_aware_datetime_still_falls_back_with_warning():
    aware = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.warns(UserWarning, match='timezone-aware'):
        v = Vector.filled(aware, 2)

    assert type(v._storage) is TupleStorage
    assert list(v) == [aware, aware]


def test_filled_strings_slice_and_index_like_built_strings():
    v = Vector.filled('ab', 5)

    assert v[3] == 'ab'
    assert list(v[1:3]) == ['ab', 'ab']
