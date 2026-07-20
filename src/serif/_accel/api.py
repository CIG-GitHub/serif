"""Existing optional-accelerator call-throughs.

This module is the private boundary between Serif's semantic operations and
the current NumPy/PyArrow helpers. It preserves the existing per-call decline
and fallback behavior; it is not the generic execution dispatcher planned for
the later backend refactor.
"""

import operator

from .._vector.dtype import Schema


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


def _accel_binop(storage, rhs, op_func, result_dtype):
    """Accelerated elementwise arithmetic; None = decline. The schema is
    already resolved before this call — the accelerator computes values,
    never semantics. Returns a Vector or None.

    Ordering: TRUE DIVISION tries arrow first — its checked kernel skips
    null lanes natively, so the numpy tier's neutralize-divisors copy
    and zero-scan never run (identical results, fewer passes). Everything
    else runs numpy first; int lanes its overflow bounds pass declined
    (it must over-predict) get arrow's CHECKED kernels, which decline
    only on actual overflow (serif/_accel/arrow.py)."""
    from .. import _accel
    fast = None
    if op_func is operator.truediv:
        from .arrow import div_floats
        fast = div_floats(storage, rhs, op_func, result_dtype.kind)
    if fast is None and _accel._USE_NUMPY:
        from .ops import binop_storage
        fast = binop_storage(storage, rhs, op_func, result_dtype.kind)
    if fast is None:
        from .arrow import binop_ints
        fast = binop_ints(storage, rhs, op_func, result_dtype.kind)
    if fast is None:
        return None
    from ..vector import Vector
    result = Vector._from_storage(fast, result_dtype)
    result._wild = True   # match the pure constructors' name-tracking flag
    return result


def _accel_compare(storage, rhs, op_func, nullable):
    """Accelerated elementwise comparison; None = decline to the pure
    path, whose behavior is the specification. Both backends get a try
    (see serif/_accel/__init__.py): numpy for fixed-width lanes, then
    arrow for the string content numpy cannot see."""
    from .. import _accel
    fast = None
    if _accel._USE_NUMPY:
        from .ops import compare_storage
        fast = compare_storage(storage, rhs, op_func)
    if fast is None:
        from .arrow import compare_strings
        fast = compare_strings(storage, rhs, op_func)
    if fast is None:
        return None
    from ..vector import Vector
    result = Vector._from_storage(fast, Schema(bool, nullable))
    result._wild = True
    return result


def _accel_logical(storage, rhs, op_name):
    """Numpy-accelerated Kleene &/|/^; None = decline to the pure zip,
    whose behavior is the specification. Nullability is post-hoc like the
    pure path's `any(v is None)` — the mask-None convention makes the two
    agree exactly."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .ops import logical_storage
    fast = logical_storage(storage, rhs, op_name)
    if fast is None:
        return None
    from ..vector import Vector
    result = Vector._from_storage(fast, Schema(bool, fast._mask is not None))
    result._wild = True
    return result


def _accel_invert(storage, nullable):
    """Numpy-accelerated Kleene NOT; None = decline. nullable is the
    schema-carried flag — __invert__ preserves the input schema rather
    than recomputing it post-hoc."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .ops import invert_storage
    fast = invert_storage(storage)
    if fast is None:
        return None
    from ..vector import Vector
    result = Vector._from_storage(fast, Schema(bool, nullable))
    result._wild = True
    return result
