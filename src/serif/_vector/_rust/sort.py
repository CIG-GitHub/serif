"""Optional Rust stable argsort for int, float, bool and category keys.

The compiled radix sort reproduces Python's stable multi-key ordering,
including per-key direction and null placement, and declines on NaN.
"""

from ..._execution import DECLINED
from . import _rs
from . import _USE_RUST
from ..storage import ArrayStorage
from ..storage import BoolStorage


_ARRAY_KINDS = {'q': 0, 'd': 1}
_BOOL = 2


def _key(storage):
    """Return (kind, values buffer, mask buffer) or None to decline."""
    from ..categorical import _CategoryStorage

    if isinstance(storage, _CategoryStorage):
        # Codes follow category order, which is what sort_by compares.
        storage = storage._codes
    if isinstance(storage, ArrayStorage):
        kind = _ARRAY_KINDS.get(storage._data.typecode)
        if kind is None:
            return None
        values = storage._data
    elif isinstance(storage, BoolStorage):
        kind, values = _BOOL, storage._data
    else:
        return None
    mask = storage._mask
    return kind, values, None if mask is None else mask._buf


def sort_indices(storages, reverse_flags, na_last):
    """Return the stable sorted row order, or ``DECLINED``."""
    if not _USE_RUST:
        return DECLINED
    n = len(storages[0])
    # An empty buffer exposes an unaligned pointer; Python sorts it instantly.
    if not n:
        return DECLINED
    keys = []
    for storage in storages:
        key = _key(storage)
        if key is None:
            return DECLINED
        keys.append(key)

    order = _rs.argsort(
        n,
        [kind for kind, _, _ in keys],
        [values for _, values, _ in keys],
        [mask for _, _, mask in keys],
        [bool(flag) for flag in reverse_flags],
        na_last,
    )
    if order is None:
        return DECLINED
    return memoryview(order).cast('q')
