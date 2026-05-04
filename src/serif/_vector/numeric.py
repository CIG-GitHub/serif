# ============================================================
# Container for numeric backends
# ============================================================
from .base import Vector


class _Float(Vector):
	dtype_name = 'float'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		"""
		Initialize a new Vector instance.
		"""
		self._name = None
		if name is not None:
			self._name = name
		self._display_as_row = as_row
		self._wild = True

		# We check self.__dict__ directly to avoid triggering Table.__getattr__
		# which would crash because the table isn't initialized yet.
		if '_precomputed_data' in self.__dict__:
			data = self._precomputed_data
			del self._precomputed_data
		else:
			data = initial

		nullable = self._dtype.nullable if self._dtype is not None else True
		self._storage = ArrayStorage.from_iterable(data, typecode=self.typecode, nullable=nullable)

		# Fingerprint cache + powers
		self._fp: int | None = None
		self._fp_powers: List[int] | None = None
		
		# Register with alias tracker after full initialization
		_ALIAS_TRACKER.register(self, id(self._storage))


class _Int(Vector):
	dtype_name = 'int'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		"""
		Initialize a new Vector instance.
		"""
		self._name = None
		if name is not None:
			self._name = name
		self._display_as_row = as_row
		self._wild = True

		# We check self.__dict__ directly to avoid triggering Table.__getattr__
		# which would crash because the table isn't initialized yet.
		if '_precomputed_data' in self.__dict__:
			data = self._precomputed_data
			del self._precomputed_data
		else:
			data = initial

		nullable = self._dtype.nullable if self._dtype is not None else True
		self._storage = ArrayStorage.from_iterable(data, typecode=self.typecode, nullable=nullable)

		# Fingerprint cache + powers
		self._fp: int | None = None
		self._fp_powers: List[int] | None = None
		
		# Register with alias tracker after full initialization
		_ALIAS_TRACKER.register(self, id(self._storage))


class _Int64(Vector):
	dtype_name = 'int64'
	typecode = 'q'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _Int32(Vector):
	dtype_name = 'int32'
	typecode = 'i'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _Int16(Vector):
	dtype_name = 'int16'
	typecode = 'h'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _Int8(Vector):
	dtype_name = 'int8'
	typecode = 'b'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _UInt64(Vector):
	dtype_name = 'uint64'
	typecode = 'Q'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _UInt32(Vector):
	dtype_name = 'uint32'
	typecode = 'I'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _UInt16(Vector):
	dtype_name = 'uint16'
	typecode = 'H'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

class _UInt8(Vector):
	dtype_name = 'uint8'
	typecode = 'B'
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

	typecode = 'B'


class _Float32(Vector):
	dtype_name = 'float32'
	typecode = 'f'
	
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)


class _Float64(Vector):
	dtype_name = 'float64'
	typecode = 'd'
	
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)



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
