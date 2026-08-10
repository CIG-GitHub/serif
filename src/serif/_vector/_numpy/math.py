"""Optional NumPy implementations for exact pointwise math."""

import array as _pyarray

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import NP_DTYPES
from .storage import valid_bits
from ..storage import ArrayStorage
from ..storage import BoolStorage


_PREDICATES = {'isfinite', 'isinf', 'isnan'}
_SUPPORTED = _PREDICATES | {'fabs'}


def unary_storage(storage, function_name):
    """Return exact unary math storage or DECLINED."""
    if (
        not _USE_NUMPY
        or function_name not in _SUPPORTED
        or not isinstance(storage, ArrayStorage)
    ):
        return DECLINED

    np_dtype = NP_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return DECLINED

    values = _np.frombuffer(storage._data, dtype=np_dtype)
    output = getattr(_np, function_name)(values)
    mask = storage._mask
    if mask is not None and valid_bits(mask, len(values)).all():
        mask = None

    if function_name in _PREDICATES:
        if output.dtype != _np.bool_:
            return DECLINED
        return BoolStorage(bytearray(output.tobytes()), mask)

    if output.dtype != _np.float64:
        return DECLINED
    data = _pyarray.array('d')
    data.frombytes(output.tobytes())
    return ArrayStorage(data, mask)
