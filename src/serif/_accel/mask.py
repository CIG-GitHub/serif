"""
Boolean-mask filtering over storage buffers.

The pure path (base.Vector.__getitem__) filters by unboxing every element
through Python: zip(self, key), test, rebuild storage through dispatch.
Here the same program runs on buffers: view values zero-copy, build one
np.bool_ selection from the mask (null mask entries select False — SQL
WHERE semantics, docs/null-semantics.md), compress in C, wrap the result
bytes back into the same concrete storage type. The source null mask
rides along as unpackbits → compress → packbits.

filter_storage returns a new storage of the SAME concrete type, or None
to DECLINE (unsupported storage/typecode) — the caller falls back to the
pure path, whose behavior is the specification.
"""

from __future__ import annotations

import array as _pyarray

from . import _np, NP_DTYPES, valid_bits as _valid_bits
from .._vector.nullable import BitMask
from .._vector.storage import ArrayStorage, BoolStorage


def _selection(mask_storage, n: int):
    """BoolStorage mask → np bool selection; null entries select False."""
    sel = _np.frombuffer(mask_storage._data, dtype=_np.bool_)
    if mask_storage._mask is not None:
        sel = sel & _valid_bits(mask_storage._mask, n)
    return sel


def _filtered_mask(mask: BitMask | None, sel, out_n: int):
    """Compress the source null mask through the selection."""
    if mask is None:
        return None
    valid = _valid_bits(mask, len(sel))[sel]
    if valid.all():
        return None  # no nulls survived — mask-None convention
    packed = _np.packbits(valid, bitorder='little')
    return BitMask(bytearray(packed.tobytes()), out_n)


def filter_storage(storage, mask):
    """
    Filter `storage` by a boolean mask.

    mask may be a BoolStorage (nullable allowed: None excludes) or a plain
    list of Python bools (the caller has already type-checked it).
    Returns a new storage of the same concrete type, or None to decline.
    """
    if _np is None:
        return None

    if isinstance(mask, BoolStorage):
        sel = _selection(mask, len(mask))
    elif isinstance(mask, list):
        sel = _np.array(mask, dtype=_np.bool_)
    else:
        return None

    if isinstance(storage, ArrayStorage):
        np_dtype = NP_DTYPES.get(storage._data.typecode)
        if np_dtype is None:
            return None
        vals = _np.frombuffer(storage._data, dtype=np_dtype)  # zero-copy view
        out  = vals[sel]
        data = _pyarray.array(storage._data.typecode)
        data.frombytes(out.tobytes())
        return ArrayStorage(data, _filtered_mask(storage._mask, sel, len(out)))

    if isinstance(storage, BoolStorage):
        vals = _np.frombuffer(storage._data, dtype=_np.uint8)  # zero-copy view
        out  = vals[sel]
        return BoolStorage(bytearray(out.tobytes()),
                           _filtered_mask(storage._mask, sel, len(out)))

    return None
