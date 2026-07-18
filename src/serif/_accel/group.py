"""
Single-key bucketing over storage buffers.

The pure loops (Table._build_partition_index, Table._join_impl step 2)
bucket row indices by key with one dict operation per row — plus a tuple
allocation per row for the key itself. Here the same dict is built from
buffer math: np.unique on the zero-copy int64 view yields the distinct
keys, their first occurrences, and a per-row code; a stable argsort of
the codes groups the row indices in C. Only the final wrap — one small
list and one dict entry per DISTINCT key — runs in Python.

group_indices returns the same dict the pure loops build, or None to
DECLINE — the caller falls back to the pure loop, whose behavior is the
specification.
"""

from __future__ import annotations

from . import _np
from .._vector.storage import ArrayStorage


def group_indices(storage):
    """
    Bucket row indices by key value: {(key,): [row, ...]} with keys in
    FIRST-APPEARANCE order and row indices ascending within each bucket —
    exactly the dict the pure loops build. Keys are 1-tuples of Python
    ints (the callers' multi-key shape, single-key case).

    int64 ArrayStorage with no nulls only; anything else returns None to
    DECLINE. Floats stay pure by design, not just for now: the pure dict
    keys each NaN row into its own group (hash-equal but never ==), while
    np.unique merges NaNs — semantics, not transport. Nullable declines
    because None is a legitimate pure-path group key numpy cannot carry.
    """
    if _np is None:
        return None
    if not isinstance(storage, ArrayStorage) or storage._mask is not None:
        return None
    if storage._data.typecode != 'q':
        return None

    vals = _np.frombuffer(storage._data, dtype=_np.int64)  # zero-copy view
    uniq, first_idx, inverse = _np.unique(
        vals, return_index=True, return_inverse=True)
    order  = _np.argsort(inverse, kind='stable')   # rows grouped by code,
                                                   # ascending within group
    counts = _np.bincount(inverse, minlength=len(uniq))
    groups = _np.split(order, _np.cumsum(counts)[:-1])
    keys   = uniq.tolist()                         # Python ints

    # dict insertion order = first appearance, matching the pure loop
    # (np.unique sorts by value; first_idx restores scan order).
    appearance = _np.argsort(first_idx, kind='stable')
    return {(keys[c],): groups[c].tolist() for c in appearance.tolist()}
