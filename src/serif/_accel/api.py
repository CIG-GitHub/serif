"""Remaining legacy optional-accelerator call-throughs.

Vector operations and Table grouping dispatch from their semantic modules.
This boundary preserves the established per-call ``None`` decline behavior
only for Table joins until that family migrates.
"""


def _accel_group(storage):
    """Temporary ``None``-decline adapter for the join right index."""
    from .. import _accel
    from .._execution import DECLINED

    if not _accel._USE_NUMPY:
        return None

    from .._table.grouping import _dispatch_single_key

    result = _dispatch_single_key(storage)
    return None if result is DECLINED else result


def _accel_join_probe(left_storage, right_storage,
                      expect_left_unique, expect_right_unique,
                      keep_unmatched_left, keep_unmatched_right):
    """Accelerated single-key join probe. Returns a tagged tuple (see
    serif/_accel/join.py) or None = decline to the pure matcher, whose
    behavior is the specification. numpy probes int64 keys; string keys
    encode through arrow into the same probe core (serif/_accel/arrow.py,
    which gates on both switches itself)."""
    from .. import _accel
    fast = None
    if _accel._USE_NUMPY:
        from .join import probe_int64_dense
        fast = probe_int64_dense(
            left_storage, right_storage,
            expect_left_unique, expect_right_unique,
            keep_unmatched_left, keep_unmatched_right)
    if fast is None:
        from .arrow import join_probe_strings_hash
        fast = join_probe_strings_hash(
            left_storage, right_storage,
            expect_left_unique, expect_right_unique,
            keep_unmatched_left, keep_unmatched_right)
    if fast is None and _accel._USE_NUMPY:
        from .join import probe_int64
        fast = probe_int64(left_storage, right_storage,
                           expect_left_unique, expect_right_unique,
                           keep_unmatched_left, keep_unmatched_right)
    if fast is None:
        from .arrow import join_probe_strings
        fast = join_probe_strings(left_storage, right_storage,
                                  expect_left_unique, expect_right_unique,
                                  keep_unmatched_left, keep_unmatched_right)
    return fast


