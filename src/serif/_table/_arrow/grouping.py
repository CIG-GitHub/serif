"""Optional Arrow physical implementation for string-key row bucketing."""

from ..._execution import DECLINED
from ..._vector._arrow import storage as _arrow_storage
from ..._vector.storage import StringStorage
from .._numpy import grouping as _numpy_grouping
from . import _USE_ARROW


def group_strings(storage):
    """Bucket one dense string key, or return ``DECLINED``."""
    if not _USE_ARROW or not _numpy_grouping._USE_NUMPY:
        return DECLINED
    if not isinstance(storage, StringStorage) or storage._mask is not None:
        return DECLINED

    array = _arrow_storage.string_array(storage)
    if array is DECLINED:
        return DECLINED

    encoded = array.dictionary_encode()
    codes = encoded.indices.to_numpy(zero_copy_only=True)
    key_count = len(encoded.dictionary)
    order = _numpy_grouping._np.argsort(codes, kind='stable')
    counts = _numpy_grouping._np.bincount(codes, minlength=key_count)
    groups = _numpy_grouping._np.split(
        order,
        _numpy_grouping._np.cumsum(counts)[:-1],
    )
    keys = encoded.dictionary.to_pylist()
    return {
        (keys[code],): groups[code].tolist()
        for code in range(key_count)
    }

