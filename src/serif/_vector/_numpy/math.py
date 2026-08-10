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
_ROUND_TO_INT = {'ceil', 'floor', 'trunc'}
_SUPPORTED = _PREDICATES | _ROUND_TO_INT | {'fabs'}


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
    mask = storage._mask
    valid = None if mask is None else valid_bits(mask, len(values))
    if valid is not None and valid.all():
        mask = valid = None

    if function_name in _ROUND_TO_INT:
        if np_dtype == 'int64':
            return ArrayStorage(_pyarray.array('q', storage._data), mask)
        effective = values if valid is None else _np.where(valid, values, 0.0)
        output = getattr(_np, function_name)(effective)
        if (
            not _np.isfinite(output).all()
            or (output < -2**63).any()
            or (output >= 2**63).any()
        ):
            return DECLINED
        output = output.astype(_np.int64)
        data = _pyarray.array('q')
        data.frombytes(output.tobytes())
        return ArrayStorage(data, mask)

    output = getattr(_np, function_name)(values)

    if function_name in _PREDICATES:
        if output.dtype != _np.bool_:
            return DECLINED
        return BoolStorage(bytearray(output.tobytes()), mask)

    if output.dtype != _np.float64:
        return DECLINED
    data = _pyarray.array('d')
    data.frombytes(output.tobytes())
    return ArrayStorage(data, mask)
