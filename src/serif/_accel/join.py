"""
Vectorized single-key join probe.

The pure matcher (serif._table.joins._join_probe_pure) hash-indexes the right
side and walks the left side row by row — a tuple allocation and a dict lookup
per row. Here the whole match runs on buffers: stable-argsort the right keys
once, binary-search every left key against the sorted run
(np.searchsorted), and expand fan-out matches with the same ragged
repeat/cumsum arithmetic the string gather uses. Cardinality checks ride
the same sort: a duplicate key is an adjacent equal pair in sorted order,
and the scan-order-first duplicate the pure loop reports is the run whose
SECOND original row index is smallest (stable sort keeps original row
order within equal-key runs).

probe_int64 returns a tagged tuple — ('ok', left_take, right_take) with
intp arrays and -1 as the pad sentinel; ('right_dup', key, count) or
('left_dup', key, None) for cardinality violations (the CALLER raises,
so the error text lives in one layer next to the pure loop's) — or None
to DECLINE (either key column not int64 ArrayStorage, or nullable). The
pure matcher's behavior is the specification.

The match itself never looks at what the ints MEAN, so the core lives
in probe_codes, which probes any pair of integer lane arrays: int64 key
values here, dictionary codes for the arrow string probe
(serif/_accel/arrow.py). Keys inside its dup tags are raw lane values —
probe_int64 surfaces them as-is; code-space callers translate.
"""

from __future__ import annotations

from . import _np
from .._vector.storage import ArrayStorage


def _supported(storage):
    return (isinstance(storage, ArrayStorage)
            and storage._mask is None
            and storage._data.typecode == 'q')


def _first_duplicate(sorted_keys, order):
    """Scan-order-first duplicate in a stable-sorted key array: the run
    whose SECOND element has the smallest original row index. Returns
    (key, count) as Python ints, or None if all keys are unique."""
    dup = sorted_keys[1:] == sorted_keys[:-1]
    if not dup.any():
        return None
    # Each run's second element: a dup position whose predecessor is not dup.
    run_second = dup & ~_np.concatenate(([False], dup[:-1]))
    positions = _np.nonzero(run_second)[0] + 1      # sorted-domain positions
    seconds = order[positions]                      # original row indices
    pos = positions[int(seconds.argmin())]
    key = sorted_keys[pos]
    count = int(_np.searchsorted(sorted_keys, key, side='right')
                - _np.searchsorted(sorted_keys, key, side='left'))
    return int(key), count


def probe_int64(left_storage, right_storage,
                expect_left_unique, expect_right_unique,
                keep_unmatched_left, keep_unmatched_right):
    if _np is None:
        return None
    if not (_supported(left_storage) and _supported(right_storage)):
        return None

    left_vals  = _np.frombuffer(left_storage._data,  dtype=_np.int64)
    right_vals = _np.frombuffer(right_storage._data, dtype=_np.int64)
    return probe_codes(left_vals, right_vals,
                       expect_left_unique, expect_right_unique,
                       keep_unmatched_left, keep_unmatched_right)


def probe_int64_dense(left_storage, right_storage,
                      expect_left_unique, expect_right_unique,
                      keep_unmatched_left, keep_unmatched_right):
    """O(n) direct-address probe for compact, right-unique int ranges.

    Two lookup-sized intp arrays are the material cost (counts and right-row
    lookup), so the key span is capped relative to input size. Sparse ranges,
    nullable keys, duplicate diagnostics, and non-unique-right joins decline
    to probe_int64's sort path.
    """
    if _np is None or not expect_right_unique:
        return None
    if not (_supported(left_storage) and _supported(right_storage)):
        return None
    if len(left_storage) == 0 or len(right_storage) == 0:
        return None

    left_vals = _np.frombuffer(left_storage._data, dtype=_np.int64)
    right_vals = _np.frombuffer(right_storage._data, dtype=_np.int64)
    low = min(int(left_vals.min()), int(right_vals.min()))
    high = max(int(left_vals.max()), int(right_vals.max()))
    span = high - low + 1
    max_span = max(4096, 2 * (len(left_vals) + len(right_vals)))
    if span > max_span:
        return None

    # Unsigned modular subtraction avoids signed overflow when low is near
    # -2**63. Because span passed the small-range guard, every code is the
    # exact mathematical difference from low.
    base = _np.uint64(low % (2**64))
    left_codes = (left_vals.view(_np.uint64) - base).astype(_np.intp)
    right_codes = (right_vals.view(_np.uint64) - base).astype(_np.intp)
    return probe_unique_codes(
        left_codes, right_codes, span,
        expect_left_unique, expect_right_unique,
        keep_unmatched_left, keep_unmatched_right)


def probe_unique_codes(left_codes, right_codes, n_codes,
                       expect_left_unique, expect_right_unique,
                       keep_unmatched_left, keep_unmatched_right):
    """Probe non-negative dense codes when the right side must be unique.

    Duplicate cases decline so probe_codes can reproduce the pure path's
    exact scan-order diagnostic. Otherwise this is O(n + n_codes).
    """
    if not expect_right_unique:
        return None
    right_counts = _np.bincount(right_codes, minlength=n_codes)
    if (right_counts > 1).any():
        return None
    if expect_left_unique:
        left_counts = _np.bincount(left_codes, minlength=n_codes)
        if (left_counts > 1).any():
            return None

    lookup = _np.full(n_codes, -1, dtype=_np.intp)
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
            left_take = _np.concatenate(
                [left_take, _np.full(len(unmatched), -1, dtype=_np.intp)])
            right_take = _np.concatenate([right_take, unmatched])

    return 'ok', left_take, right_take


def probe_codes(left_vals, right_vals,
                expect_left_unique, expect_right_unique,
                keep_unmatched_left, keep_unmatched_right):
    """The probe core over two integer lane arrays (any int dtype): the
    module docstring's algorithm, verbatim. Never declines — storage-
    level gates live in the callers. Dup-tag keys are raw lane values."""
    order       = _np.argsort(right_vals, kind='stable')
    sorted_keys = right_vals[order]

    # Cardinality checks first, in the pure path's raise order: right
    # uniqueness is enforced while the index is built, left during probing.
    if expect_right_unique:
        found = _first_duplicate(sorted_keys, order)
        if found is not None:
            return ('right_dup', (found[0],), found[1])

    if expect_left_unique:
        left_order = _np.argsort(left_vals, kind='stable')
        found = _first_duplicate(left_vals[left_order], left_order)
        if found is not None:
            return ('left_dup', (found[0],), None)

    lo = _np.searchsorted(sorted_keys, left_vals, side='left')
    hi = _np.searchsorted(sorted_keys, left_vals, side='right')
    counts = hi - lo                                 # matches per left row

    # Matched lanes: the right rows for left row i are order[lo[i]:hi[i]],
    # ascending original index within the run (stable sort) — the pure
    # bucket order. Ragged expansion: one global lane counter, shifted
    # per-row to its run start.
    m = int(counts.sum())
    csum  = _np.cumsum(counts)
    intra = _np.arange(m, dtype=_np.intp) - _np.repeat(csum - counts, counts)
    right_matched = order[_np.repeat(lo, counts) + intra]

    if keep_unmatched_left:
        # Unmatched left rows contribute one pad lane each, interleaved in
        # left-row order.
        out_counts = _np.where(counts > 0, counts, 1)
        left_take  = _np.repeat(_np.arange(len(left_vals), dtype=_np.intp),
                                out_counts)
        right_take = _np.full(len(left_take), -1, dtype=_np.intp)
        right_take[_np.repeat(counts > 0, out_counts)] = right_matched
    else:
        left_take  = _np.repeat(_np.arange(len(left_vals), dtype=_np.intp),
                                counts)
        right_take = right_matched

    if keep_unmatched_right:
        # Full join: right rows no left key hit, appended in right-row order.
        hit = _np.zeros(len(right_vals), dtype=bool)
        hit[right_matched] = True
        unmatched = _np.nonzero(~hit)[0]
        if len(unmatched):
            left_take  = _np.concatenate(
                [left_take, _np.full(len(unmatched), -1, dtype=_np.intp)])
            right_take = _np.concatenate([right_take, unmatched])

    return ('ok', left_take, right_take)
