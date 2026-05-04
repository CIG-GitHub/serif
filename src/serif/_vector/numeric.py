# ============================================================
# Container for numeric backends
# ============================================================
from .base import Vector


class _Float(Vector):
	dtype_name = 'float'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)


class _Int(Vector):
	dtype_name = 'int'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)


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
class _Float64(Vector):
	dtype_name = 'float64'
	typecode = 'd'



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
	(_Int16,_Int8): _Int16,
	(_Int16, _Int32): _Int32,
	(_Int16, _Int64): _Int64,
	(_Int16, _UInt8): _Int32,
	(_Int16, _UInt16): _Int32,
	(_Int16, _UInt32): _Int64,
	(_Int16, _UInt64): _Int,

	# Int32 ladder
	(_Int32,_Int8): _Int32,
	(_Int32, _Int16): _Int32,
	(_Int32, _Int64): _Int64,
	(_Int32, _UInt8): _Int64,
	(_Int32, _UInt16): _Int64,
	(_Int32, _UInt32): _Int64,
	(_Int32, _UInt64): _Int,


	# Int64 ladder
	(_Int64,_Int8): _Int64,
	(_Int64, _Int16): _Int64,
	(_Int64, _Int32): _Int64,
	(_Int64, _UInt8): _Int,
	(_Int64, _UInt16): _Int,
	(_Int64, _UInt32): _Int,
	(_Int64, _UInt64): _Int,
}
