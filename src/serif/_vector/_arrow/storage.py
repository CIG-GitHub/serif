"""Arrow views over Serif storage and canonical Serif storage reconstruction."""

import array as _pyarray

from ..._execution import DECLINED
from ..._execution import _load_numpy
from . import _pa
from . import _USE_ARROW
from ..nullable import BitMask
from ..storage import ArrayStorage
from ..storage import BoolStorage
from ..storage import DecimalStorage
from ..storage import StringStorage


_np = _load_numpy()


_I32_MAX = 2**31 - 1

_NUMERIC_PA_TYPES = (
    {'q': _pa.int64(), 'd': _pa.float64()}
    if _pa is not None
    else {}
)


def string_array(storage):
    """Wrap StringStorage as an Arrow array, or decline."""
    if not _USE_ARROW or not isinstance(storage, StringStorage):
        return DECLINED
    n = len(storage)
    if n == 0 or storage._offsets[-1] > _I32_MAX:
        return DECLINED
    mask = storage._mask
    validity = _pa.py_buffer(mask._buf) if mask is not None else None
    return _pa.Array.from_buffers(
        _pa.string(),
        n,
        [validity, _pa.py_buffer(storage._offsets), _pa.py_buffer(storage._buf)],
        -1 if mask is not None else 0,
    )


def numeric_array(storage):
    """Wrap supported ArrayStorage as an Arrow array, or decline."""
    if not _USE_ARROW or not isinstance(storage, ArrayStorage):
        return DECLINED
    pa_type = _NUMERIC_PA_TYPES.get(storage._data.typecode)
    if pa_type is None or len(storage) == 0:
        return DECLINED
    mask = storage._mask
    validity = _pa.py_buffer(mask._buf) if mask is not None else None
    return _pa.Array.from_buffers(
        pa_type,
        len(storage),
        [validity, _pa.py_buffer(storage._data)],
        -1 if mask is not None else 0,
    )


def int64_array(storage):
    if isinstance(storage, ArrayStorage) and storage._data.typecode == 'q':
        return numeric_array(storage)
    return DECLINED


def numeric_storage(array):
    """Convert an Arrow int64/float64 result to Serif storage, or decline."""
    if array.offset != 0:
        return DECLINED
    if _pa.types.is_int64(array.type):
        typecode = 'q'
    elif _pa.types.is_float64(array.type):
        typecode = 'd'
    else:
        return DECLINED
    n = len(array)
    data = _pyarray.array(typecode)
    if n:
        data.frombytes(memoryview(array.buffers()[1])[:n * 8])
    return ArrayStorage(data, bitmask(array))


def string_storage(array):
    """Convert an Arrow string result to StringStorage, or decline."""
    if array.offset != 0 or not _pa.types.is_string(array.type):
        return DECLINED

    n = len(array)
    if n == 0:
        return StringStorage.from_raw(b'', _pyarray.array('I', [0]), None)

    buffers = array.buffers()
    offsets = _pyarray.array('I')
    offsets.frombytes(memoryview(buffers[1])[:(n + 1) * 4])
    data_buffer = buffers[2]
    raw = (
        memoryview(data_buffer)[:offsets[-1]].tobytes()
        if data_buffer is not None
        else b''
    )
    return StringStorage.from_raw(raw, offsets, bitmask(array))


def bitmask(array):
    """Convert Arrow validity to BitMask; None means every lane is valid."""
    if array.null_count == 0:
        return None
    n = len(array)
    buffer = bytearray(memoryview(array.buffers()[0])[:(n + 7) // 8])
    return BitMask(buffer, n)


def bool_storage(array):
    """Convert an Arrow boolean result to BoolStorage, or decline."""
    if array.offset != 0 or not _pa.types.is_boolean(array.type):
        return DECLINED
    n = len(array)
    if n == 0:
        data = bytearray()
    else:
        bit_buffer = array.buffers()[1]
        if _np is not None:
            bits = _np.frombuffer(bit_buffer, dtype=_np.uint8)
            data = bytearray(
                _np.unpackbits(bits, count=n, bitorder='little').tobytes()
            )
        else:
            view = memoryview(bit_buffer)
            data = bytearray(n)
            for index in range(n):
                data[index] = (view[index >> 3] >> (index & 7)) & 1
    return BoolStorage.from_raw(data, bitmask(array))


def decimal_storage(array):
    """Convert an Arrow decimal128 result to DecimalStorage, or decline."""
    if (array.offset != 0
            or not _pa.types.is_decimal(array.type)
            or array.type.bit_width != 128):
        return DECLINED

    n = len(array)
    scale = array.type.scale
    precision = array.type.precision
    if n == 0:
        return DecimalStorage(bytearray(), scale, precision, None)

    little_endian = memoryview(array.buffers()[1])[:n * 16]
    if _np is not None:
        rows = _np.frombuffer(
            little_endian, dtype=_np.uint8).reshape(n, 16)
        big_endian = rows[:, ::-1].tobytes()
    else:
        source = little_endian.tobytes()
        target = bytearray(n * 16)
        for index in range(n):
            start = index * 16
            target[start:start + 16] = source[start:start + 16][::-1]
        big_endian = bytes(target)

    return DecimalStorage.from_raw_be(
        big_endian,
        scale,
        precision,
        bitmask(array),
    )
