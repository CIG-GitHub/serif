"""NumPy views over Serif's existing fixed-width storage buffers."""

from . import _np


NP_DTYPES = {'q': 'int64', 'd': 'float64'}


def valid_bits(mask, n):
    """Return a NumPy bool array that is true for valid Serif lanes."""
    bits = _np.frombuffer(mask._buf, dtype=_np.uint8)
    return _np.unpackbits(bits, count=n, bitorder='little').view(_np.bool_)


def valid_values(storage):
    """Return the supported, non-null ArrayStorage lanes as a NumPy view."""
    np_dtype = NP_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return None
    values = _np.frombuffer(storage._data, dtype=np_dtype)
    if storage._mask is not None:
        values = values[valid_bits(storage._mask, len(values))]
    return values
