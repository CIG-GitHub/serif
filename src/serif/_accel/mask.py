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
from .._vector.storage import ArrayStorage, BoolStorage, StringStorage

# String filtering picks its copy strategy by average surviving span:
# below this many bytes/span, numpy's per-byte ragged gather wins; above,
# a Python slice per span + b''.join wins (measured crossover ~28).
_JOIN_SPAN_BYTES = 32


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

    if isinstance(storage, StringStorage):
        # No string content is ever touched: the pure path decodes every
        # surviving string to str and re-encodes it; here the survivors'
        # byte spans are copied out of the UTF-8 buffer directly.
        offs     = _np.frombuffer(storage._offsets, dtype=_np.uint32)  # zero-copy
        starts   = offs[:-1][sel]
        ends_src = offs[1:][sel]
        lengths  = (ends_src - starts).astype(_np.int64)
        k        = len(lengths)

        # New offsets: cumsum with a leading 0. uint32 is safe — total
        # bytes only shrink under a filter, and the source was uint32.
        ends = _np.cumsum(lengths)
        new_offs = _pyarray.array('I')
        new_offs.frombytes(_np.concatenate(
            ([0], ends)).astype(_np.uint32).tobytes())

        total = int(ends[-1]) if k else 0
        if not total:
            new_buf = b''
        elif total >= k * _JOIN_SPAN_BYTES:
            # Long spans: one Python slice per SPAN (~250ns each) beats
            # numpy's per-BYTE index construction. Measured crossover
            # ~28 bytes/span on 1M-row filters.
            buf = storage._buf
            new_buf = b''.join(
                [buf[s:e] for s, e in zip(starts.tolist(), ends_src.tolist())])
        else:
            # Short spans: ragged gather — one flat index per output byte,
            # arange over the output shifted per-span to its source start.
            span_shift = _np.repeat(
                starts.astype(_np.int64) - _np.concatenate(([0], ends[:-1])),
                lengths)
            idx = _np.arange(total, dtype=_np.int64) + span_shift
            new_buf = _np.frombuffer(storage._buf, dtype=_np.uint8)[idx].tobytes()
        return StringStorage.from_raw(new_buf, new_offs,
                                      _filtered_mask(storage._mask, sel, k))

    return None
