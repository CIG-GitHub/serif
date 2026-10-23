"""Optional Rust physical implementation for single-key int64 Table joins.

One compiled probe covers every cardinality and outer-join shape, including
nullable keys (nulls never match), with the pure probe's row order and
duplicate diagnostics.
"""

from ..._execution import DECLINED
from ..._vector.storage import ArrayStorage
from . import _rs
from . import _USE_RUST


def _keys(storage):
    if isinstance(storage, ArrayStorage) and storage._data.typecode == 'q':
        mask = storage._mask
        return storage._data, None if mask is None else mask._buf
    return None


def probe_int64(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Run the compiled int64 hash probe, or return ``DECLINED``."""
    if not _USE_RUST:
        return DECLINED
    left = _keys(left_storage)
    right = _keys(right_storage)
    if left is None or right is None:
        return DECLINED
    # An empty array('q') exposes an unaligned buffer pointer; Python
    # handles empty sides instantly anyway.
    if not len(left_storage) or not len(right_storage):
        return DECLINED

    result = _rs.join_probe_int64(
        *left,
        *right,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )
    if result is None:
        return DECLINED
    if result[0] == 'ok':
        # Zero-copy int64 views; every gather path accepts a sequence.
        return 'ok', memoryview(result[1]).cast('q'), memoryview(result[2]).cast('q')
    if result[0] == 'right_dup':
        return 'right_dup', (result[1],), result[2]
    return 'left_dup', (result[1],), None
