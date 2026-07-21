"""Remaining legacy optional-accelerator call-throughs.

Vector operators dispatch from their semantic module. This boundary preserves
the established per-call ``None`` decline behavior for selection, grouping,
joins, and reductions until those semantic families migrate.
"""


def _accel_filter(storage, mask):
    """Numpy-accelerated boolean-mask filter; None = decline to the pure
    path, whose behavior is the specification. OPTIONAL numpy — transport,
    never semantics; see serif/_accel/__init__.py for the doctrine."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .mask import filter_storage
    return filter_storage(storage, mask)


def _accel_popcount(mask_storage):
    """Numpy-accelerated True count of a boolean mask (nulls count False);
    None = decline to the pure count, whose behavior is the specification.
    OPTIONAL numpy — transport, never semantics; see serif/_accel/__init__.py."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .mask import popcount_storage
    return popcount_storage(mask_storage)


def _accel_take(storage, indices):
    """Numpy-accelerated positional gather; None = decline to the pure
    storage.take(), whose behavior is the specification. OPTIONAL numpy —
    transport, never semantics; see serif/_accel/__init__.py."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .mask import take_storage
    return take_storage(storage, indices)


def _take(storage, indices):
    """storage.take() behind the accelerator: numpy gather when the backend
    is supported, the protocol's pure take() otherwise."""
    fast = _accel_take(storage, indices)
    return fast if fast is not None else storage.take(indices)


def _accel_take_pad(storage, indices):
    """Numpy-accelerated gather where index -1 emits null (join pad rows);
    None = decline to the caller's pure emission, whose behavior is the
    specification."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .mask import take_pad_storage
    return take_pad_storage(storage, indices)


def _accel_group(storage):
    """Accelerated single-key bucketing ({(key,): [rows]} in first-
    appearance order); None = decline to the pure dict loop, whose
    behavior is the specification. numpy buckets int64 keys from buffer
    math; string keys ride arrow's hash kernel into the same math
    (serif/_accel/arrow.py, which gates on both switches itself)."""
    from .. import _accel
    fast = None
    if _accel._USE_NUMPY:
        from .group import group_indices
        fast = group_indices(storage)
    if fast is None:
        from .arrow import group_strings
        fast = group_strings(storage)
    return fast


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


def _accel_reduce(storage, op, **kwargs):
    """Try a numpy-accelerated reduction. Returns (True, value) when the
    fast path produced the answer, (False, None) on decline — the caller
    runs the pure path, whose behavior is the specification. None is a
    legitimate value (max of all-null), hence the flag."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return False, None
    from . import reduce as _reduce
    result = getattr(_reduce, op)(storage, **kwargs)
    if result is _accel.DECLINED:
        return False, None
    return True, result
