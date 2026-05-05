# ============================================================
# Container for numeric backends
# ============================================================
from .base import Vector
from .storage import ArrayStorage
from .storage import TupleStorage


class _Float(Vector):
    dtype_name = 'float'
    typecode = 'd'


class _Int(Vector):
    dtype_name = 'int'

    def _build_storage(self, data, nullable):
        if not isinstance(data, (list, tuple)):
            data = list(data)
        try:
            return ArrayStorage.from_iterable(data, typecode='q', nullable=nullable)
        except OverflowError:
            return TupleStorage.from_iterable(data, nullable=nullable)


class _Int64(Vector):
    dtype_name = 'int64'
    typecode = 'q'

class _Int32(Vector):
    dtype_name = 'int32'
    typecode = 'i'

class _Int16(Vector):
    dtype_name = 'int16'
    typecode = 'h'

class _Int8(Vector):
    dtype_name = 'int8'
    typecode = 'b'

class _UInt64(Vector):
    dtype_name = 'uint64'
    typecode = 'Q'

class _UInt32(Vector):
    dtype_name = 'uint32'
    typecode = 'I'

class _UInt16(Vector):
    dtype_name = 'uint16'
    typecode = 'H'

class _UInt8(Vector):
    dtype_name = 'uint8'
    typecode = 'B'


class _Float32(Vector):
    dtype_name = 'float32'
    typecode = 'f'



_PROMOTION = {
    # Int8 ladder
    (_Int8, _Int16): _Int16,
    (_Int8, _Int32): _Int32,
    (_Int8, _Int64): _Int64,
    (_Int8, _UInt8): _Int16,
    (_Int8, _UInt16): _Int32,
    (_Int8, _UInt32): _Int64,
    (_Int8, _UInt64): _Int,

    # Int16 ladder
    (_Int16, _Int8): _Int16,
    (_Int16, _Int32): _Int32,
    (_Int16, _Int64): _Int64,
    (_Int16, _UInt8): _Int32,
    (_Int16, _UInt16): _Int32,
    (_Int16, _UInt32): _Int64,
    (_Int16, _UInt64): _Int,

    # Int32 ladder
    (_Int32, _Int8): _Int32,
    (_Int32, _Int16): _Int32,
    (_Int32, _Int64): _Int64,
    (_Int32, _UInt8): _Int64,
    (_Int32, _UInt16): _Int64,
    (_Int32, _UInt32): _Int64,
    (_Int32, _UInt64): _Int,

    # Int64 ladder
    (_Int64, _Int8): _Int64,
    (_Int64, _Int16): _Int64,
    (_Int64, _Int32): _Int64,
    (_Int64, _UInt8): _Int,
    (_Int64, _UInt16): _Int,
    (_Int64, _UInt32): _Int,
    (_Int64, _UInt64): _Int,

    # UInt8 ladder
    (_UInt8, _Int8): _Int16,
    (_UInt8, _Int16): _Int32,
    (_UInt8, _Int32): _Int64,
    (_UInt8, _Int64): _Int,
    (_UInt8, _UInt16): _Int32,
    (_UInt8, _UInt32): _Int64,
    (_UInt8, _UInt64): _Int,

    # UInt16 ladder
    (_UInt16, _Int8): _Int16,
    (_UInt16, _Int16): _Int32,
    (_UInt16, _Int32): _Int64,
    (_UInt16, _Int64): _Int,
    (_UInt16, _UInt8): _Int32,
    (_UInt16, _UInt32): _Int64,
    (_UInt16, _UInt64): _Int,

    # UInt32 ladder
    (_UInt32, _Int8): _Int32,
    (_UInt32, _Int16): _Int32,
    (_UInt32, _Int32): _Int64,
    (_UInt32, _Int64): _Int,
    (_UInt32, _UInt8): _Int64,
    (_UInt32, _UInt16): _Int64,
    (_UInt32, _UInt64): _Int,

    # UInt64 ladder
    (_UInt64, _Int8): _Int64,
    (_UInt64, _Int16): _Int64,
    (_UInt64, _Int32): _Int64,
    (_UInt64, _Int64): _Int,
    (_UInt64, _UInt8): _Int,
    (_UInt64, _UInt16): _Int,
    (_UInt64, _UInt32): _Int,

}
