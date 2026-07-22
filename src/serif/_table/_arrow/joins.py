"""Optional Arrow physical implementations for string-key Table joins."""

from ..._execution import DECLINED
from ..._vector._arrow import storage as _arrow_storage
from ..._vector.storage import StringStorage
from .._numpy import joins as _numpy_joins
from . import _pa
from . import _USE_ARROW


def _arrays(left_storage, right_storage):
    if not _USE_ARROW or not _numpy_joins._USE_NUMPY:
        return DECLINED
    if not (
        isinstance(left_storage, StringStorage)
        and isinstance(right_storage, StringStorage)
    ):
        return DECLINED
    if left_storage._mask is not None or right_storage._mask is not None:
        return DECLINED

    left_array = _arrow_storage.string_array(left_storage)
    if left_array is DECLINED:
        return DECLINED
    right_array = _arrow_storage.string_array(right_storage)
    if right_array is DECLINED:
        return DECLINED
    return left_array, right_array


def probe_strings(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Probe dense strings through shared Arrow dictionary codes."""
    arrays = _arrays(left_storage, right_storage)
    if arrays is DECLINED:
        return DECLINED
    left_array, right_array = arrays

    encoded = _pa.concat_arrays([
        left_array,
        right_array,
    ]).dictionary_encode()
    codes = encoded.indices.to_numpy(zero_copy_only=True)
    left_length = len(left_storage)
    result = _numpy_joins.probe_codes(
        codes[:left_length],
        codes[left_length:],
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )
    if result[0] == 'ok':
        return result
    tag, (code,), count = result
    return tag, (encoded.dictionary[code].as_py(),), count


def probe_strings_hash(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Run the direct-address right-unique string probe, or decline."""
    if not expect_right_unique:
        return DECLINED
    arrays = _arrays(left_storage, right_storage)
    if arrays is DECLINED:
        return DECLINED
    left_array, right_array = arrays

    encoded = _pa.concat_arrays([
        right_array,
        left_array,
    ]).dictionary_encode()
    codes = encoded.indices.to_numpy(zero_copy_only=True)
    right_length = len(right_storage)
    return _numpy_joins.probe_unique_codes(
        codes[right_length:],
        codes[:right_length],
        len(encoded.dictionary),
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )

