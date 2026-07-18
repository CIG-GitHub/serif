"""
OPTIONAL numpy-accelerated compute — transport, never representation.

serif stays zero-dependency: this package only activates when numpy
happens to be installed, and nothing anywhere requires it. The doctrine
is the one proven by the pyarrow reader (serif/io/_arrow.py):

  * python in → python out. Whatever computes a result, what surfaces is
    concrete Python values (int, float, bool, None) — never numpy
    scalars. np.float64 IS a float subclass, so leaks are invisible to
    equality checks; conformance tests assert `type(x) is float`.

  * Accelerators may widen TRANSPORT, never SEMANTICS. Every accelerated
    operation returns results IDENTICAL to the pure path, or DECLINES
    (returns None) and the pure path runs. Declines are per-call — an
    unsupported storage type, typecode, or value range costs one
    isinstance check, not a mode switch.

  * No new representation. numpy operates on serif's EXISTING buffers:
    ArrayStorage's array.array and BoolStorage's bytearray are viewed
    zero-copy via np.frombuffer; BitMask's packed LSB-first bits are one
    np.unpackbits(bitorder='little') from a bool array and one
    np.packbits from coming back. The storage layout being arrow-shaped
    keeps paying.

Modules: mask (boolean-mask filtering), with reduce (sum/mean/stdev/
min/max) and ops (elementwise) to follow.

_USE_NUMPY is a private switch for tests/benchmarks, not API.
"""

try:
    import numpy as _np
except ImportError:            # numpy not installed — every call declines
    _np = None

_USE_NUMPY = _np is not None

# Sentinel for "the accelerator declined" — distinct from None, which is a
# legitimate reduction RESULT (max of an all-null column is None).
DECLINED = object()

# array.array typecode → numpy dtype name, for the two accelerated kinds.
NP_DTYPES = {'q': 'int64', 'd': 'float64'}


def valid_bits(mask, n):
    """BitMask → np bool array, True where VALID (BitMask is LSB-first
    packed with 1=valid — exactly np.unpackbits(bitorder='little'))."""
    bits = _np.frombuffer(mask._buf, dtype=_np.uint8)
    return _np.unpackbits(bits, count=n, bitorder='little').view(_np.bool_)


def valid_values(storage):
    """ArrayStorage → np view of its buffer, compressed to valid lanes.

    Select, don't multiply: masked lanes are EXCLUDED by boolean compress,
    never neutralized by arithmetic — inf·0 and nan·0 are nan, so any
    multiply-by-mask scheme corrupts float columns. Returns None to decline
    (unsupported typecode)."""
    np_dtype = NP_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return None
    vals = _np.frombuffer(storage._data, dtype=np_dtype)  # zero-copy view
    if storage._mask is not None:
        vals = vals[valid_bits(storage._mask, len(vals))]
    return vals
