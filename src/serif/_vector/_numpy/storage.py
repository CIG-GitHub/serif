"""NumPy views over Serif's existing fixed-width storage buffers."""

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY


# 'i' is int32, Arrow's width for date32 days (DateStorage ticks travel as
# ArrayStorage('i') views; see _vector/temporal.py).
NP_DTYPES = {'q': 'int64', 'd': 'float64', 'i': 'int32'}


def valid_bits(mask, n):
    """Return a NumPy bool array that is true for valid Serif lanes."""
    bits = _np.frombuffer(mask._buf, dtype=_np.uint8)
    return _np.unpackbits(bits, count=n, bitorder='little').view(_np.bool_)


def null_bytes(mask, n):
    """One 0/1 byte per lane, 1 where null (BoolStorage layout), or DECLINED."""
    if not _USE_NUMPY:
        return DECLINED
    return bytearray((~valid_bits(mask, n)).tobytes())


def valid_values(storage):
    """Return the supported, non-null ArrayStorage lanes as a NumPy view."""
    np_dtype = NP_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return None
    values = _np.frombuffer(storage._data, dtype=np_dtype)
    if storage._mask is not None:
        values = values[valid_bits(storage._mask, len(values))]
    return values
