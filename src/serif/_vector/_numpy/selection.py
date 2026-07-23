"""Optional NumPy physical implementations for row selection."""

from __future__ import annotations

import array as _pyarray

from ..._execution import DECLINED
from . import _np
from . import _USE_NUMPY
from .storage import NP_DTYPES
from .storage import valid_bits as _valid_bits
from ..nullable import BitMask
from ..storage import ArrayStorage
from ..storage import BoolStorage
from ..storage import StringStorage


_JOIN_SPAN_BYTES = 32
_U32_MAX = 0xFFFFFFFF


def _selection(mask_storage, n):
    selected = _np.frombuffer(mask_storage._data, dtype=_np.bool_)
    if mask_storage._mask is not None:
        selected = selected & _valid_bits(mask_storage._mask, n)
    return selected


def _gathered_mask(mask, indexer, source_length, pad=None):
    if mask is None:
        if pad is None:
            return None
        valid = ~pad
    else:
        valid = _valid_bits(mask, source_length)[indexer]
        if pad is not None:
            valid = valid & ~pad
    if valid.all():
        return None
    packed = _np.packbits(valid, bitorder='little')
    return BitMask(bytearray(packed.tobytes()), len(valid))


def _gather(storage, indexer, source_length, pad=None):
    if isinstance(storage, ArrayStorage):
        numpy_dtype = NP_DTYPES.get(storage._data.typecode)
        if numpy_dtype is None:
            return DECLINED
        values = _np.frombuffer(storage._data, dtype=numpy_dtype)
        output = values[indexer]
        if pad is not None:
            output[pad] = 0
        data = _pyarray.array(storage._data.typecode)
        data.frombytes(memoryview(output).cast('B'))
        return ArrayStorage(
            data,
            _gathered_mask(
                storage._mask,
                indexer,
                source_length,
                pad,
            ),
        )

    if isinstance(storage, BoolStorage):
        values = _np.frombuffer(storage._data, dtype=_np.uint8)
        output = values[indexer]
        if pad is not None:
            output[pad] = 0
        return BoolStorage(
            bytearray(output.tobytes()),
            _gathered_mask(
                storage._mask,
                indexer,
                source_length,
                pad,
            ),
        )

    if isinstance(storage, StringStorage):
        offsets = _np.frombuffer(storage._offsets, dtype=_np.uint32)
        starts = offsets[:-1][indexer]
        source_ends = offsets[1:][indexer]
        if pad is not None:
            source_ends = _np.where(pad, starts, source_ends)
        lengths = (source_ends - starts).astype(_np.int64)
        output_length = len(lengths)

        ends = _np.cumsum(lengths)
        total = int(ends[-1]) if output_length else 0
        if total > _U32_MAX:
            return DECLINED
        new_offsets = _pyarray.array('I')
        new_offsets.frombytes(
            _np.concatenate(([0], ends)).astype(_np.uint32).tobytes()
        )

        if not total:
            new_buffer = b''
        elif total >= output_length * _JOIN_SPAN_BYTES:
            source_buffer = memoryview(storage._buf)
            target_buffer = bytearray(total)
            target_offset = 0
            for start, end in zip(starts, source_ends):
                start = int(start)
                end = int(end)
                next_offset = target_offset + end - start
                target_buffer[target_offset:next_offset] = (
                    source_buffer[start:end]
                )
                target_offset = next_offset
            new_buffer = bytes(target_buffer)
        else:
            span_shift = _np.repeat(
                starts.astype(_np.int64)
                - _np.concatenate(([0], ends[:-1])),
                lengths,
            )
            byte_indices = (
                _np.arange(total, dtype=_np.int64)
                + span_shift
            )
            new_buffer = _np.frombuffer(
                storage._buf,
                dtype=_np.uint8,
            )[byte_indices].tobytes()
        return StringStorage.from_raw(
            new_buffer,
            new_offsets,
            _gathered_mask(
                storage._mask,
                indexer,
                source_length,
                pad,
            ),
        )

    return DECLINED


def popcount_storage(mask):
    """Return a canonical Python survivor count or ``DECLINED``."""
    if not _USE_NUMPY or not isinstance(mask, BoolStorage):
        return DECLINED
    return int(_selection(mask, len(mask)).sum())


def filter_storage(storage, mask):
    """Return filtered Serif storage or ``DECLINED``."""
    if not _USE_NUMPY:
        return DECLINED
    if isinstance(mask, BoolStorage):
        selected = _selection(mask, len(mask))
    elif isinstance(mask, list):
        selected = _np.array(mask, dtype=_np.bool_)
    else:
        return DECLINED
    return _gather(storage, selected, len(storage))


def take_storage(storage, indices):
    """Return gathered Serif storage or ``DECLINED``."""
    if not _USE_NUMPY:
        return DECLINED
    indexer = _np.asarray(indices, dtype=_np.intp)
    return _gather(storage, indexer, len(storage))


def take_pad_storage(storage, indices):
    """Return gathered Serif storage with ``-1`` null pads, or decline."""
    if not _USE_NUMPY:
        return DECLINED
    indexer = _np.asarray(indices, dtype=_np.intp)
    pad = indexer == -1
    if not pad.any():
        return _gather(storage, indexer, len(storage))
    if not len(storage):
        return DECLINED
    clamped = _np.where(pad, 0, indexer)
    return _gather(storage, clamped, len(storage), pad=pad)
