"""Optional NumPy physical implementations for Vector transforms."""

from __future__ import annotations

import array as _pyarray

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import NP_DTYPES
from .storage import valid_bits
from ..storage import ArrayStorage
from ..storage import BoolStorage
from ..storage import _TemporalStorage


_I64_MIN = -2**63
_I64_MAX = 2**63 - 1


def fill_nulls(storage, fill):
    """The storage with every null lane set to `fill`, unmasked, or DECLINED.

    `fill` is already the lane value: an int or float for ArrayStorage, a
    bool for BoolStorage, an int tick for date/datetime storage. Storage
    without nulls comes back as is (storage is immutable).
    """
    if not _USE_NUMPY:
        return DECLINED
    if isinstance(storage, _TemporalStorage):
        filled = _fill_lanes(storage.tick_view(), fill)
        if filled is DECLINED:
            return DECLINED
        return type(storage)(filled._data, None)
    return _fill_lanes(storage, fill)


def _int_lanes(members):
    """Members as the int64 values an int column can equal exactly.

    A float member equals an int only when it is integral (2.0 matches 2;
    2.5, nan and inf match nothing); ints past int64 match no lane.
    """
    lanes = []
    for member in members:
        if type(member) is float:
            if not member.is_integer():
                continue
            member = int(member)
        member = int(member)                      # bool → 0/1
        if _I64_MIN <= member <= _I64_MAX:
            lanes.append(member)
    return lanes


def _float_lanes(members):
    """Members as the float64 values a float column can equal exactly.

    Python compares int and float exactly, so an int matches only if it is
    exactly representable as a float; nan never matches a fresh value.
    """
    lanes = []
    for member in members:
        try:
            as_float = float(member)
        except OverflowError:
            continue
        if as_float == member:                    # nan != nan drops nan too
            lanes.append(as_float)
    return lanes


def is_in(storage, members, match_null):
    """Element-wise membership as BoolStorage, or DECLINED.

    `members` are the non-None members; Python's `==` decides membership,
    so anything but exact bool/int/float members declines. Null lanes are
    True when match_null, else null.
    """
    if not _USE_NUMPY or not isinstance(storage, ArrayStorage):
        return DECLINED
    typecode = storage._data.typecode
    np_dtype = NP_DTYPES.get(typecode)
    if np_dtype is None:
        return DECLINED
    if any(type(member) not in (bool, int, float) for member in members):
        return DECLINED

    if typecode == 'd':
        lanes = _np.array(_float_lanes(members), dtype=_np.float64)
    else:
        lanes = _np.array(_int_lanes(members), dtype=_np.int64)
    values = _np.frombuffer(storage._data, dtype=np_dtype)
    hits = _np.isin(values, lanes)

    mask = storage._mask
    if mask is not None and mask.any_null():
        if match_null:
            hits |= ~valid_bits(mask, len(values))
            mask = None
    else:
        mask = None
    return BoolStorage(bytearray(hits.tobytes()), mask)


def _fill_lanes(storage, fill):
    """fill_nulls for BoolStorage and numeric ArrayStorage."""
    if isinstance(storage, BoolStorage):
        np_dtype, fill = _np.uint8, 1 if fill else 0
    elif isinstance(storage, ArrayStorage):
        np_dtype = NP_DTYPES.get(storage._data.typecode)
        if np_dtype is None:
            return DECLINED
        # An int fill past int64 can't sit in the lane; the pure path
        # degrades the column to exact Python ints instead.
        if storage._data.typecode == 'q' and not _I64_MIN <= fill <= _I64_MAX:
            return DECLINED
    else:
        return DECLINED

    mask = storage._mask
    if mask is None:
        return storage
    values = _np.frombuffer(storage._data, dtype=np_dtype).copy()
    values[~valid_bits(mask, len(values))] = fill

    if isinstance(storage, BoolStorage):
        return BoolStorage(bytearray(values.tobytes()))
    data = _pyarray.array(storage._data.typecode)
    data.frombytes(values.tobytes())
    return ArrayStorage(data)
