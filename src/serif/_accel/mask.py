"""
Row gathering over storage buffers: boolean-mask filtering and positional
take are one program.

The pure paths gather by unboxing every element through Python: filtering
zips values against the mask and rebuilds storage through dispatch;
storage.take() runs a per-index generator. Here the same program runs on
buffers: view the values zero-copy, gather in C with one numpy fancy
index, wrap the result bytes back into the same concrete storage type.
The indexer is either a np.bool_ selection (filter — null mask entries
select False, SQL WHERE semantics, docs/null-semantics.md) or an intp
position array (take) — numpy treats the two interchangeably, so both
operations share _gather. The source null mask rides along as
unpackbits → gather → packbits.

filter_storage / take_storage return a new storage of the SAME concrete
type, or None to DECLINE (unsupported storage/typecode) — the caller
falls back to the pure path, whose behavior is the specification.
"""

from __future__ import annotations

import array as _pyarray

from . import _np, NP_DTYPES, valid_bits as _valid_bits
from .._vector.nullable import BitMask
from .._vector.storage import ArrayStorage, BoolStorage, StringStorage

# String gathering picks its copy strategy by average surviving span:
# below this many bytes/span, numpy's per-byte ragged gather wins; above,
# a Python slice per span + b''.join wins (measured crossover ~28).
_JOIN_SPAN_BYTES = 32

# StringStorage offsets are uint32. A take with duplicated indices can
# GROW the output buffer past the source size; past the uint32 ceiling
# the gather must DECLINE — the pure path raises OverflowError building
# array('I'), and that puke is the specification. Silently wrapping
# offsets is not.
_U32_MAX = 0xFFFFFFFF


def _selection(mask_storage, n: int):
    """BoolStorage mask → np bool selection; null entries select False."""
    sel = _np.frombuffer(mask_storage._data, dtype=_np.bool_)
    if mask_storage._mask is not None:
        sel = sel & _valid_bits(mask_storage._mask, n)
    return sel


def _gathered_mask(mask: BitMask | None, indexer, src_n: int):
    """Gather the source null mask through the indexer."""
    if mask is None:
        return None
    valid = _valid_bits(mask, src_n)[indexer]
    if valid.all():
        return None  # no nulls survived — mask-None convention
    packed = _np.packbits(valid, bitorder='little')
    return BitMask(bytearray(packed.tobytes()), len(valid))


def _gather(storage, indexer, src_n: int):
    """Gather rows of `storage` by a numpy indexer (bool selection or intp
    positions). Returns a new storage of the same concrete type, or None
    to decline."""
    if isinstance(storage, ArrayStorage):
        np_dtype = NP_DTYPES.get(storage._data.typecode)
        if np_dtype is None:
            return None
        vals = _np.frombuffer(storage._data, dtype=np_dtype)  # zero-copy view
        out  = vals[indexer]
        data = _pyarray.array(storage._data.typecode)
        data.frombytes(out.tobytes())
        return ArrayStorage(data, _gathered_mask(storage._mask, indexer, src_n))

    if isinstance(storage, BoolStorage):
        vals = _np.frombuffer(storage._data, dtype=_np.uint8)  # zero-copy view
        out  = vals[indexer]
        return BoolStorage(bytearray(out.tobytes()),
                           _gathered_mask(storage._mask, indexer, src_n))

    if isinstance(storage, StringStorage):
        # No string content is ever touched: the pure path decodes every
        # gathered string to str and re-encodes it; here the survivors'
        # byte spans are copied out of the UTF-8 buffer directly.
        offs     = _np.frombuffer(storage._offsets, dtype=_np.uint32)  # zero-copy
        starts   = offs[:-1][indexer]
        ends_src = offs[1:][indexer]
        lengths  = (ends_src - starts).astype(_np.int64)
        k        = len(lengths)

        # New offsets: cumsum with a leading 0, computed in int64 so a
        # growing take cannot overflow mid-sum; declined past uint32.
        ends  = _np.cumsum(lengths)
        total = int(ends[-1]) if k else 0
        if total > _U32_MAX:
            return None
        new_offs = _pyarray.array('I')
        new_offs.frombytes(_np.concatenate(
            ([0], ends)).astype(_np.uint32).tobytes())

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
                                      _gathered_mask(storage._mask, indexer, src_n))

    return None


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

    return _gather(storage, sel, len(storage))


def take_storage(storage, indices):
    """
    Gather rows of `storage` at the given positions, in order. Duplicate
    positions are allowed (join fan-out); indices must be non-negative and
    in range, as every storage.take() caller already guarantees.
    Returns a new storage of the same concrete type, or None to decline.
    """
    if _np is None:
        return None
    idx = _np.asarray(indices, dtype=_np.intp)
    return _gather(storage, idx, len(storage))
