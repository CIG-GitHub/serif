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
