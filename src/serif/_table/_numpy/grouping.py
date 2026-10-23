"""Optional NumPy physical implementation for single-key row bucketing."""

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from ..._vector.storage import ArrayStorage


_INT_DTYPES = {'q': 'int64', 'i': 'int32'}


def group_indices(storage):
    """Bucket dense int64 / int32 keys, or return ``DECLINED``."""
    if not _USE_NUMPY:
        return DECLINED
    if not isinstance(storage, ArrayStorage) or storage._mask is not None:
        return DECLINED
    np_dtype = _INT_DTYPES.get(storage._data.typecode)
    if np_dtype is None:
        return DECLINED

    values = _np.frombuffer(storage._data, dtype=np_dtype)
    unique, first_indices, inverse = _np.unique(
        values,
        return_index=True,
        return_inverse=True,
    )
    order = _np.argsort(inverse, kind='stable')
    counts = _np.bincount(inverse, minlength=len(unique))
    groups = _np.split(order, _np.cumsum(counts)[:-1])
    keys = unique.tolist()

    appearance = _np.argsort(first_indices, kind='stable')
    return {
        (keys[code],): groups[code]
        for code in appearance
    }

