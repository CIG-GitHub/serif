"""Optional NumPy physical implementations for single-key Table joins."""

from ..._execution import DECLINED
from ..._vector.storage import ArrayStorage
from . import _np
from . import _USE_NUMPY


def _supported(storage):
    return (
        isinstance(storage, ArrayStorage)
        and storage._mask is None
        and storage._data.typecode == 'q'
    )


def _first_duplicate(sorted_keys, order):
    """Return the scan-order-first duplicate key and its count."""
    duplicate = sorted_keys[1:] == sorted_keys[:-1]
    if not duplicate.any():
        return None
    run_second = duplicate & ~_np.concatenate(([False], duplicate[:-1]))
    positions = _np.nonzero(run_second)[0] + 1
    seconds = order[positions]
    position = positions[int(seconds.argmin())]
    key = sorted_keys[position]
    count = int(
        _np.searchsorted(sorted_keys, key, side='right')
        - _np.searchsorted(sorted_keys, key, side='left')
    )
    return int(key), count


def probe_int64(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Run the stable sorted int64 probe, or return ``DECLINED``."""
    if not _USE_NUMPY:
        return DECLINED
    if not (_supported(left_storage) and _supported(right_storage)):
        return DECLINED

    left_values = _np.frombuffer(left_storage._data, dtype=_np.int64)
    right_values = _np.frombuffer(right_storage._data, dtype=_np.int64)
    return probe_codes(
        left_values,
        right_values,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )


def probe_int64_dense(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Run the direct-address right-unique int64 probe, or decline."""
    if not _USE_NUMPY or not expect_right_unique:
        return DECLINED
    if not (_supported(left_storage) and _supported(right_storage)):
        return DECLINED
    if len(left_storage) == 0 or len(right_storage) == 0:
        return DECLINED

    left_values = _np.frombuffer(left_storage._data, dtype=_np.int64)
    right_values = _np.frombuffer(right_storage._data, dtype=_np.int64)
    low = min(int(left_values.min()), int(right_values.min()))
    high = max(int(left_values.max()), int(right_values.max()))
    span = high - low + 1
    max_span = max(4096, 2 * (len(left_values) + len(right_values)))
    if span > max_span:
        return DECLINED

    base = _np.uint64(low % (2**64))
    left_codes = (left_values.view(_np.uint64) - base).astype(_np.intp)
    right_codes = (right_values.view(_np.uint64) - base).astype(_np.intp)
    return probe_unique_codes(
        left_codes,
        right_codes,
        span,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )


def probe_unique_codes(
    left_codes,
    right_codes,
    code_count,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Probe dense non-negative codes when the right must be unique."""
    if not expect_right_unique:
        return DECLINED
    right_counts = _np.bincount(right_codes, minlength=code_count)
    if (right_counts > 1).any():
        return DECLINED
    if expect_left_unique:
        left_counts = _np.bincount(left_codes, minlength=code_count)
        if (left_counts > 1).any():
            return DECLINED

    lookup = _np.full(code_count, -1, dtype=_np.intp)
    lookup[right_codes] = _np.arange(len(right_codes), dtype=_np.intp)
    right_for_left = lookup[left_codes]
    matched = right_for_left >= 0

    if keep_unmatched_left:
        left_take = _np.arange(len(left_codes), dtype=_np.intp)
        right_take = right_for_left
    else:
        left_take = _np.nonzero(matched)[0]
        right_take = right_for_left[matched]

    if keep_unmatched_right:
        hit = _np.zeros(len(right_codes), dtype=bool)
        hit[right_for_left[matched]] = True
        unmatched = _np.nonzero(~hit)[0]
        if len(unmatched):
            left_take = _np.concatenate([
                left_take,
                _np.full(len(unmatched), -1, dtype=_np.intp),
            ])
            right_take = _np.concatenate([right_take, unmatched])

    return 'ok', left_take.tolist(), right_take.tolist()


def probe_codes(
    left_values,
    right_values,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Run the stable sorted probe over integer lane arrays."""
    order = _np.argsort(right_values, kind='stable')
    sorted_keys = right_values[order]

    if expect_right_unique:
        found = _first_duplicate(sorted_keys, order)
        if found is not None:
            return 'right_dup', (found[0],), found[1]

    if expect_left_unique:
        left_order = _np.argsort(left_values, kind='stable')
        found = _first_duplicate(left_values[left_order], left_order)
        if found is not None:
            return 'left_dup', (found[0],), None

    low = _np.searchsorted(sorted_keys, left_values, side='left')
    high = _np.searchsorted(sorted_keys, left_values, side='right')
    counts = high - low

    match_count = int(counts.sum())
    cumulative = _np.cumsum(counts)
    intra = (
        _np.arange(match_count, dtype=_np.intp)
        - _np.repeat(cumulative - counts, counts)
    )
    right_matched = order[_np.repeat(low, counts) + intra]

    if keep_unmatched_left:
        output_counts = _np.where(counts > 0, counts, 1)
        left_take = _np.repeat(
            _np.arange(len(left_values), dtype=_np.intp),
            output_counts,
        )
        right_take = _np.full(len(left_take), -1, dtype=_np.intp)
        right_take[_np.repeat(counts > 0, output_counts)] = right_matched
    else:
        left_take = _np.repeat(
            _np.arange(len(left_values), dtype=_np.intp),
            counts,
        )
        right_take = right_matched

    if keep_unmatched_right:
        hit = _np.zeros(len(right_values), dtype=bool)
        hit[right_matched] = True
        unmatched = _np.nonzero(~hit)[0]
        if len(unmatched):
            left_take = _np.concatenate([
                left_take,
                _np.full(len(unmatched), -1, dtype=_np.intp),
            ])
            right_take = _np.concatenate([right_take, unmatched])

    return 'ok', left_take.tolist(), right_take.tolist()

