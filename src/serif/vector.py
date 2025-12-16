import operator
import warnings
import re
import math
from builtins import isinstance as b_isinstance
from collections.abc import Iterator
from collections.abc import Iterable

from .alias_tracker import _ALIAS_TRACKER
from .alias_tracker import AliasError
from .errors import SerifTypeError
from .errors import SerifIndexError
from .errors import SerifValueError
from .errors import SerifKeyError
from .display import _printr
from .naming import _sanitize_user_name
from .typing import DataType
from .typing import infer_dtype
from .typing import validate_scalar

from copy import deepcopy
from datetime import date
from datetime import datetime
from datetime import timedelta
from .typeutils import slice_length

from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple

# ============================================================
# Reverse arithmetic operation helpers
# ============================================================
def _reverse_sub(y, x):
	return x - y

def _reverse_truediv(y, x):
	return x / y

def _reverse_floordiv(y, x):
	return x // y

def _reverse_mod(y, x):
	return x % y

def _reverse_pow(y, x):
	return x ** y

# ============================================================
# Small helpers
# ============================================================

def _is_hashable(x: Any) -> bool:
	try:
		hash(x)
		return True
	except Exception:
		return False


def _safe_sortable_list(xs: Iterable[Any]) -> List[Any]:
	"""
	Deterministic representation for sets in fingerprinting.
	"""
	try:
		return sorted(xs)
	except Exception:
		return sorted((repr(x) for x in xs))


class MethodProxy:
	"""Proxy that defers method calls to each element in a Vector."""
	def __init__(self, vector, method_name):
		self._vector = vector
		self._method_name = method_name
	
	def __call__(self, *args, **kwargs):
		method = self._method_name
		results = []
		for elem in self._vector._underlying:
			if elem is None:
				results.append(None)
			else:
				results.append(getattr(elem, method)(*args, **kwargs))
		return Vector(results)


# ============================================================
# Main backend
# ============================================================

class Vector():
	""" Iterable vector with optional type safety """
	_dtype = None  # DataType instance (private)
	_underlying = None
	_name = None
	_display_as_row = False
	_wild = False  # Flag for name changes (used by Table column tracking)
	
	# Fingerprint constants for O(1) change detection
	_FP_P = (1 << 61) - 1  # Mersenne prime (2^61 - 1)
	_FP_B = 1315423911     # Base for rolling hash

	def schema(self):
		"""Get the DataType schema of this vector."""
		return self._dtype


	def __new__(cls, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		"""
		Decide what type of Vector to create based on contents.
		"""
		# If 'initial' is an iterator/generator (consumable), we must materialize it ONCE here.
		# Otherwise, infer_dtype(initial) consumes it, leaving nothing for __init__.
		_precomputed_data = None
		
		# Check if iterable but not a reusable container (list/tuple/Vector)
		# Generators and iterators need to be materialized before dtype inference
		if isinstance(initial, Iterator):
			initial = tuple(initial)
			_precomputed_data = initial

		# Check if we're creating a Table (all elements are vectors of same length)
		if initial and all(isinstance(x, Vector) for x in initial):
			if len({len(x) for x in initial}) == 1:
				from .table import Table
				return Table(initial=initial, dtype=dtype, name=name, as_row=as_row)
			warnings.warn('Passing vectors of different length will not produce a Table.')
		
		# Convert Python types to DataType if needed
		if dtype is not None and not isinstance(dtype, DataType):
			dtype = DataType(dtype)
		
		# Infer dtype if not provided
		# (Safe to run now because 'initial' is definitely a tuple/list/reusable)
		if dtype is None and initial:
			dtype = infer_dtype(initial)
		
		# Dispatch to typed subclasses based on inferred dtype
		target_class = cls
		if dtype is not None:
			if dtype.kind is str:
				target_class = _String
			elif dtype.kind is int:
				target_class = _Int
			elif dtype.kind is float:
				target_class = _Float
			elif dtype.kind is date:
				target_class = _Date
		
		# Create instance using object.__new__ (bypasses __new__ dispatch)
		instance = super(Vector, target_class).__new__(target_class)
		
		# Stash dtype on instance
		instance._dtype = dtype
		
		# Attach the materialized data so __init__ doesn't have to re-read (or guess)
		if _precomputed_data is not None:
			instance._precomputed_data = _precomputed_data
		
		return instance


	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
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
			self._underlying = self._precomputed_data
			del self._precomputed_data # Clean up
		else:
			# Standard path: initial was already a list/tuple/dict
			self._underlying = tuple(initial)
		
		# Fingerprint cache + powers
		self._fp: int | None = None
		self._fp_powers: List[int] | None = None
		
		# Register with alias tracker after full initialization
		_ALIAS_TRACKER.register(self, id(self._underlying))


	@property
	def shape(self):
		if not self._underlying:
			return tuple()
		return (len(self),)

	@property
	def name(self):
		"""Get the name of this vector."""
		return self._name
	
	@name.setter
	def name(self, new_name):
		"""Set the name of this vector."""
		self._name = new_name
		self._wild = True  # Mark as wild when renamed

	#-----------------------------------------------------
	# Fingerprinting
	#-----------------------------------------------------

	@staticmethod
	def _hash_element(x: Any) -> int:
		P = Vector._FP_P
		B = Vector._FP_B

		if x is None:
			return 0x9E3779B97F4A7C15
		
		if hasattr(x, "fingerprint") and callable(getattr(x, "fingerprint")):
			return int(x.fingerprint())

		if isinstance(x, float):
			if math.isnan(x):
				return 0xDEADBEEFCAFEBABE
			return hash(x)

		if isinstance(x, set):
			rep = _safe_sortable_list(list(x))
			return Vector._hash_element(tuple(rep))

		if isinstance(x, (list, tuple)):
			h = 0
			for elem in x:
				h = (h * B + Vector._hash_element(elem)) % P
			return h

		if _is_hashable(x):
			return hash(x)

		return hash(repr(x))

	def _ensure_fp_powers(self) -> None:
		n = len(self._underlying)
		if n == 0:
			self._fp_powers = []
			return
		P = self._FP_P
		B = self._FP_B
		pw = [1] * n
		for i in range(n - 2, -1, -1):
			pw[i] = (pw[i + 1] * B) % P
		self._fp_powers = pw

	def _compute_fingerprint_full(self) -> int:
		P = self._FP_P
		B = self._FP_B
		total = 0
		for x in self._underlying:
			h = self._hash_element(x)
			total = (total * B + h) % P
		return total

	def fingerprint(self) -> int:
		if self._fp is None:
			if self._fp_powers is None or len(self._fp_powers) != len(self._underlying):
				self._ensure_fp_powers()
			self._fp = self._compute_fingerprint_full()
		return self._fp

	def _invalidate_fp(self) -> None:
		self._fp = None

	@classmethod
	def new(cls, default_element, length, typesafe=False):
		""" create a new, initialized vector of length * default_element"""
		if length:
			assert isinstance(length, int)
			dtype = infer_dtype([default_element])
			if typesafe:
				dtype = dtype.with_nullable(False)
			return cls([default_element for _ in range(length)], dtype=dtype)
		dtype = infer_dtype([default_element]) if default_element is not None else DataType(object)
		if typesafe:
			dtype = dtype.with_nullable(False).with_default(default_element)
		return cls(dtype=dtype)


	def copy(self, new_values = None, name=...):
		# Preserve name if not explicitly overridden
		# Use sentinel value (...) to distinguish between name=None (clear) and not passing name (preserve)
		use_name = self._name if name is ... else name
		return Vector(list(new_values or self._underlying),
			dtype = self._dtype,
			name = use_name,
			as_row = self._display_as_row)
	
	def to_object(self):
		"""
		Convert this vector to object dtype, allowing mixed types.
		
		Returns a new Vector with dtype=object containing the same values.
		Useful when you need to assign values of different types to a vector.
		
		Example:
			a = Vector([1, 2, 3, 4])   # int vector
			a = a.to_object()            # now object vector
			a[2] = "ryan"                # allowed - can mix types
		"""
		return Vector(list(self._underlying), dtype=object, name=self._name, as_row=self._display_as_row)

	def alias(self, new_name):
		"""
		Assign a name to an unnamed vector (returns self for chaining).
		
		This method only works on unnamed vectors. If the vector already has a name,
		use the .name property directly or .copy(name=...) to create a named copy.
		"""
		if self._name is not None:
			raise SerifValueError(
				"alias() is reserved for unnamed vectors only. "
				"To rename: use .name = 'new'. "
				"To copy with new name: use .copy(name='new')"
			)
		self._name = new_name
		self._wild = True  # Mark as wild when named
		return self

	def rename(self, new_name):
		"""Deprecated: Use .name property or .alias() for unnamed vectors."""
		warnings.warn(
			"rename() is deprecated. Use .name = 'new' to rename, or .alias() for unnamed vectors.",
			DeprecationWarning,
			stacklevel=2
		)
		self.name = new_name
		self._wild = True  # Mark as wild when renamed
		return self
	
	def _mark_tame(self):
		"""Mark this vector as tame (not wild)"""
		self._wild = False

	def __repr__(self):
		return(_printr(self))

	def cast(self, target_type):
		"""
		Convert each element to target_type, recursively if the element is a Vector.
		Preserves None values and infers nullable dtype.
		"""
		py_target_type = target_type  # logical type for dtype / error messages

		# Python Date interceptors
		if target_type is date:
			def caster(x):
				if isinstance(x, date):
					return x
				return date.fromisoformat(x)
		elif target_type is datetime:
			def caster(x):
				if isinstance(x, datetime):
					return x
				return datetime.fromisoformat(x)
		else:
			caster = target_type  # either a type like str/int, or a callable

		out = []
		has_none = False
		for i, elem in enumerate(self._underlying):
			if elem is None:
				out.append(None)
				has_none = True
				continue

			try:
				if isinstance(elem, Vector):
					out.append(elem.cast(target_type))
				else:
					out.append(caster(elem))
			except Exception as exc:
				type_name = getattr(py_target_type, "__name__", repr(py_target_type))
				raise ValueError(
					f"Cast failed at index {i}: {elem!r} cannot be converted to {type_name}"
				) from exc

		# Now decide dtype using the *logical* type, not the callable
		if isinstance(py_target_type, type):
			new_dtype = DataType(py_target_type, nullable=has_none)
		else:
			# user gave a weird callable as target_type, infer from result
			new_dtype = infer_dtype(out)

		return Vector(tuple(out), dtype=new_dtype, name=self._name, as_row=self._display_as_row)

	def fillna(self, value):
		dtype = self.schema()

		# Type check and promotion (same pattern as __setitem__)
		if dtype is not None and value is not None:
			try:
				validate_scalar(value, dtype)
			except TypeError:
				# Value is incompatible - need promotion
				required_dtype = infer_dtype([value])
				try:
					# Create a copy and promote it
					result = self.copy()
					result._promote(required_dtype.kind)
					# Now fill with the value on the promoted vector
					out = tuple(value if x is None else x for x in result._underlying)
					return Vector(
						out,
						dtype=DataType(required_dtype.kind, nullable=False),
						name=self._name,
						as_row=self._display_as_row
					)
				except SerifTypeError:
					raise ValueError(
						f"fillna: value {value!r} (type {type(value).__name__}) "
						f"cannot be used with {dtype.kind.__name__} vector. "
						f"Promotion not supported."
					)

		# Standard path: fill value is compatible with dtype
		out = tuple(value if x is None else x for x in self._underlying)

		# Determine new nullability
		new_nullable = any(x is None for x in out)

		# Construct new dtype
		if dtype is None:
			# Mixed type → leave as None (dtype inference will happen)
			new_dtype = None
		else:
			new_dtype = dtype.with_nullable(nullable=new_nullable)

		return Vector(
			out,
			dtype=new_dtype,
			name=self._name,
			as_row=self._display_as_row
		)

	def dropna(self):
		"""
		Remove None values from the vector.
		
		Returns
		-------
		Vector
			New vector with Nones removed
		
		Examples
		--------
		>>> v = Vector([1, None, 3, None, 5])
		>>> v.dropna()
		Vector([1, 3, 5])
		"""
		return Vector(tuple(elem for elem in self._underlying if elem is not None), dtype=self._dtype.with_nullable(False))

	def isna(self):
		"""
		Return boolean mask of None values.
		
		Returns
		-------
		Vector
			Boolean vector, True where value is None
		
		Examples
		--------
		>>> v = Vector([1, None, 3])
		>>> v.isna()
		Vector([False, True, False])
		"""
		return Vector(tuple(elem is None for elem in self._underlying), dtype=DataType(bool))

	def isinstance(self, types):
		"""
		Check if each element is an instance of the given type(s).
		
		Parameters
		----------
		types : type or tuple of types
			Type or tuple of types to check against (use type(None) for NoneType)
		
		Returns
		-------
		Vector
			Boolean vector, True where element matches type(s)
		
		Examples
		--------
		>>> v = Vector([1, "hello", 3.14, None])
		>>> v.isinstance(int)
		Vector([True, False, False, False])
		>>> v.isinstance((int, float))
		Vector([True, False, True, False])
		>>> v.isinstance(type(None))
		Vector([False, False, False, True])
		"""
		return Vector(
			tuple(b_isinstance(elem, types) for elem in self._underlying),
			dtype=DataType(bool)
		)

	@property
	def _(self):
		""" streamlined display """
		return ''


	def __iter__(self):
		""" iterate over the underlying tuple """
		return iter(self._underlying)

	def __len__(self):
		""" length of the underlying tuple """
		return len(self._underlying)

	@property
	def T(self):
		inverted = self.copy(name = self._name)
		inverted._display_as_row = not self._display_as_row
		return inverted


	def __getitem__(self, key):
		""" Get item(s) from self. Behavior varies by input type:
		The following return a Vector:
			# Vector of bool: Logical indexing (masking). Get all items where the boolean is True
			# List where every element is a bool. See Vector of bool
			# Slice: return the array elements of the slice.

		Special: Indexing a single index returns a value
			# Int: 
		"""
		if isinstance(key, int):
			# Effectively a different input type (single not a list). Returning a value, not a vector.
			return self._underlying[key]

		if isinstance(key, tuple):
			if len(key) != len(self.shape):
				raise SerifKeyError(f"Matrix indexing must provide an index in each dimension: {self.shape}")
			if len(key) == 1:
				return self[key[0]]
			return self._underlying[key[-1]][key[:-1]]

		key = self._check_duplicate(key)
		if isinstance(key, Vector) and key.schema().kind == bool and not key.schema().nullable:
			if len(self) != len(key):
				raise ValueError(f"Boolean mask length mismatch: {len(self)} != {len(key)}")
			return self.copy((x for x, y in zip(self, key, strict=True) if y), name=self._name)
		if isinstance(key, list) and {type(e) for e in key} == {bool}:
			if len(self) != len(key):
				raise ValueError(f"Boolean mask length mismatch: {len(self)} != {len(key)}")
			return self.copy((x for x, y in zip(self, key, strict=True) if y), name=self._name)
		if isinstance(key, slice):
			return self.copy(self._underlying[key], name=self._name)

		# NOT RECOMMENDED
		if isinstance(key, Vector) and key.schema().kind == int and not key.schema().nullable:
			if len(self) > 1000:
				warnings.warn('Subscript indexing is sub-optimal for large vectors; prefer slices or boolean masks')
			return self.copy((self[x] for x in key), name=self._name)

		# NOT RECOMMENDED
		if isinstance(key, list) and {type(e) for e in key} == {int}:
			if len(self) > 1000:
				warnings.warn('Subscript indexing is sub-optimal for large vectors')
			return self.copy((self[x] for x in key), name=self._name)
		raise SerifTypeError(f'Vector indices must be boolean vectors, integer vectors or integers, not {str(type(key))}')


	def __setitem__(self, key, value):
		"""
		Optimized in-place assignment for Vector with:
		- boolean masks
		- slices
		- integer indices
		- index vectors
		- list/tuple index sets

		Includes:
		- dtype validation & promotion
		- copy-on-write
		- alias tracking
		- fingerprint incremental update
		"""

		_alias = _ALIAS_TRACKER
		_alias.check_writable(self, id(self._underlying))

		# === Fast precomputed checks ===
		key = self._check_duplicate(key)
		value = self._check_duplicate(value)

		# Is the incoming value iterable?
		is_seq_val = (
			isinstance(value, Iterable)
			and not isinstance(value, (str, bytes, bytearray))
		)

		n = len(self)
		underlying = self._underlying  # local bind
		append_update = lambda idx, v: updates.append((idx, v))

		updates = []  # list of (idx, new_value)

		# =====================================================================
		# CASE 1 — Boolean mask (fast-path)
		# =====================================================================
		if (
			isinstance(key, Vector)
			and key.schema().kind == bool
			and not key.schema().nullable
		) or (
			isinstance(key, list) and all(isinstance(e, bool) for e in key)
		):
			if len(key) != n:
				raise SerifValueError("Boolean mask length must match vector length.")

			# Precompute true indices (much faster than branch-per-element)
			true_indices = [i for i, flag in enumerate(key) if flag]
			tcount = len(true_indices)

			if is_seq_val:
				if tcount != len(value):
					raise SerifValueError(
						"Iterable length must match number of True mask elements."
					)
				for idx, v in zip(true_indices, value):
					append_update(idx, v)
			else:
				for idx in true_indices:
					append_update(idx, value)

		# =====================================================================
		# CASE 2 — Slice assignment
		# =====================================================================
		elif isinstance(key, slice):
			slice_len = slice_length(key, n)
			start, stop, step = key.indices(n)

			if is_seq_val:
				if slice_len != len(value):
					raise SerifValueError("Slice length and value length must match.")
				values_to_assign = value
			else:
				# repeat the scalar
				values_to_assign = [value] * slice_len

			# faster than enumerate(zip()) for slices
			rng = range(start, stop, step)
			for idx, new_val in zip(rng, values_to_assign):
				append_update(idx, new_val)

		# =====================================================================
		# CASE 3 — Single integer index
		# =====================================================================
		elif isinstance(key, int):
			# normalize negative index
			if key < 0:
				key += n
			if not (0 <= key < n):
				raise SerifIndexError(
					f"Index {key} out of range for vector length {n}"
				)
			append_update(key, value)

		# =====================================================================
		# CASE 4 — Vector of integer indices
		# =====================================================================
		elif (
			isinstance(key, Vector)
			and key.schema().kind == int
			and not key.schema().nullable
		):
			if is_seq_val:
				if len(key) != len(value):
					raise SerifValueError(
						"Index-vector length must match value length."
					)
				for idx, val in zip(key, value):
					if idx < 0:
						idx += n
					if not (0 <= idx < n):
						raise SerifIndexError(f"Index {idx} out of range.")
					append_update(idx, val)
			else:
				for idx in key:
					if idx < 0:
						idx += n
					if not (0 <= idx < n):
						raise SerifIndexError(f"Index {idx} out of range.")
					append_update(idx, value)

		# =====================================================================
		# CASE 5 — List or tuple of integer indices
		# =====================================================================
		elif (
			isinstance(key, (list, tuple))
			and all(isinstance(e, int) for e in key)
		):
			if is_seq_val:
				if len(key) != len(value):
					raise SerifValueError("Index list must match value length.")
				for idx, val in zip(key, value):
					if idx < 0:
						idx += n
					if not (0 <= idx < n):
						raise SerifIndexError(f"Index {idx} out of range.")
					append_update(idx, val)
			else:
				for idx in key:
					if idx < 0:
						idx += n
					if not (0 <= idx < n):
						raise SerifIndexError(f"Index {idx} out of range.")
					append_update(idx, value)

		else:
			raise SerifTypeError(
				f"Invalid key type: {type(key)}. Must be boolean mask, slice, int, "
				"integer vector, or list/tuple of ints."
			)

		# =====================================================================
		# FAST-PATH TYPE CHECK / PROMOTION
		# =====================================================================
		if updates:
			new_values = [v for _, v in updates]

			# Object dtype accepts any type - skip validation
			if self._dtype is not None and self._dtype.kind is not object:
				incompatible = None
				for val in new_values:
					try:
						validate_scalar(val, self._dtype)
					except TypeError:
						incompatible = val
						break

				if incompatible is not None:
					required_dtype = infer_dtype([incompatible])
					try:
						self._promote(required_dtype.kind)
						underlying = self._underlying
					except SerifTypeError:
						raise SerifTypeError(
							f"Cannot set {required_dtype.kind.__name__} in "
							f"{self._dtype.kind.__name__} vector. "
							f"Promotion not supported."
						)
		# =====================================================================
		# MUTATE — copy-on-write + fingerprint updates
		# =====================================================================
		data_list = list(underlying)           # COW materialization

		for idx, new_val in updates:
			old_val = data_list[idx]
			data_list[idx] = new_val

		new_tuple = tuple(data_list)
		old_id = id(underlying)

		_alias.unregister(self, old_id)
		self._underlying = new_tuple
		self._invalidate_fp()
		_alias.register(self, id(new_tuple))



	""" Comparison Operators - equality and hashing
		# __eq__ ==
		# __ge__ >=
		# __gt__ >
		# __lt__ <
		# __le__ <=
		# __ne__ !=
	"""
	def _elementwise_compare(self, other, op):
		other = self._check_duplicate(other)
		
		# CASE A: Self is 2D (Table on Left)
		# T == v -> [C1==v, C2==v, ...]
		if self.ndims() == 2:
			return self.copy(tuple(
				# recursive call: Column == other
				col._elementwise_compare(other, op) 
				for col in self.cols()
			))
		
		# CASE B: Other is 2D (Table on Right)
		# v == T -> [v==C1, v==C2, ...]
		if isinstance(other, Vector) and other.ndims() == 2:
			return other.copy(tuple(
				# recursive call: self == Column
				self._elementwise_compare(col, op) 
				for col in other.cols()
			))
		
		if isinstance(other, Vector):
			# Raise mismatched lengths
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			result_values = tuple(False if (x is None or y is None) else bool(op(x, y)) for x, y in zip(self, other, strict=True))
			return Vector(result_values, dtype=DataType(bool, nullable=False))
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			# Raise mismatched lengths
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			result_values = tuple(False if (x is None or y is None) else bool(op(x, y)) for x, y in zip(self, other, strict=True))
			return Vector(result_values, dtype=DataType(bool, nullable=False))
		# Scalar comparison
		if other is None and op in (operator.eq, operator.ne):
			warnings.warn(
				"Null comparison: `v == None` always returns False for null values. "
				"Use `v.isna()` to test for nulls.",
				stacklevel=2
			)
		result_values = tuple(False if x is None else bool(op(x, other)) for x in self)
		return Vector(result_values, dtype=DataType(bool, nullable=False))	# Now, we can redefine the comparison methods using the helper function
	
	def __eq__(self, other):
		return self._elementwise_compare(other, operator.eq)

	def __ge__(self, other):
		return self._elementwise_compare(other, operator.ge)

	def __gt__(self, other):
		return self._elementwise_compare(other, operator.gt)

	def __le__(self, other):
		return self._elementwise_compare(other, operator.le)

	def __lt__(self, other):
		return self._elementwise_compare(other, operator.lt)

	def __ne__(self, other):
		return self._elementwise_compare(other, operator.ne)

	def __and__(self, other):
		return self._elementwise_compare(other, operator.and_)

	def __or__(self, other):
		return self._elementwise_compare(other, operator.or_)

	def __xor__(self, other):
		return self._elementwise_compare(other, operator.xor)

	def __rand__(self, other):
		return self._elementwise_compare(other, operator.and_)

	def __ror__(self, other):
		return self._elementwise_compare(other, operator.or_)

	def __rxor__(self, other):
		return self._elementwise_compare(other, operator.xor)


	""" Math operations """
	def _elementwise_operation(self, other, op_func, op_name: str, op_symbol: str):
		"""Helper function to handle element-wise operations with broadcasting."""
		other = self._check_duplicate(other)
		
		# CASE A: Self is 2D (Table on Left)
		# T + v -> [C1+v, C2+v, ...]
		if self.ndims() == 2:
			return self.copy(tuple(
				# recursive call: Column + other
				col._elementwise_operation(other, op_func, op_name, op_symbol) 
				for col in self.cols()
			))
		
		# CASE B: Other is 2D (Table on Right)
		# v + T -> [v+C1, v+C2, ...]
		if isinstance(other, Vector) and other.ndims() == 2:
			return other.copy(tuple(
				# recursive call: self + Column
				self._elementwise_operation(col, op_func, op_name, op_symbol) 
				for col in other.cols()
			))
		
		if isinstance(other, Vector):
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			try:
				result_values = tuple(None if (x is None or y is None) else op_func(x, y) for x, y in zip(self, other, strict=True))
			except TypeError:
				# Incompatible types - fall back to object dtype with raw tuples
				result_values = tuple((x, y) for x, y in zip(self, other, strict=True))
				return Vector(result_values, dtype=DataType(object), name=None, as_row=self._display_as_row)
			result_dtype = infer_dtype(result_values)
			return Vector(result_values,
							dtype=result_dtype,
							name=None,
							as_row=self._display_as_row)

		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			try:
				result_values = tuple(None if (x is None or y is None) else op_func(x, y) for x, y in zip(self, other, strict=True))
			except TypeError:
				# Incompatible types - fall back to object dtype with raw tuples
				result_values = tuple((x, y) for x, y in zip(self, other, strict=True))
				return Vector(result_values, dtype=DataType(object), name=None, as_row=self._display_as_row)
			result_dtype = infer_dtype(result_values)
			return Vector(result_values,
				dtype=result_dtype,
				name=None,
				as_row=self._display_as_row
				)	# Scalar operation - let Python handle type compatibility
		try:
			result_values = tuple(None if x is None else op_func(x, other) for x in self._underlying)
			# Infer dtype from result (e.g., int * 0.1 = float)
			result_dtype = infer_dtype(result_values)
			return Vector(result_values,
							dtype=result_dtype,
							name=None,
							as_row=self._display_as_row)
		except TypeError as e:
			raise SerifTypeError(f"Unsupported operand type(s) for '{op_symbol}': '{self._dtype.kind.__name__}' and '{type(other).__name__}'.")

	def _unary_operation(self, op_func, op_name: str):
		"""Helper function to handle unary operations on each element."""
		return Vector(
			tuple(op_func(x) for x in self),
			dtype=self._dtype,
			name=self._name,
			as_row=self._display_as_row
		)
	
	def __add__(self, other):
		return self._elementwise_operation(other, operator.add, '__add__', '+')

	def __mul__(self, other):
		return self._elementwise_operation(other, operator.mul, '__mul__', '*')

	def __sub__(self, other):
		return self._elementwise_operation(other, operator.sub, '__sub__', '-')

	def __neg__(self):
		return self._unary_operation(operator.neg, '__neg__')

	def __pos__(self):
		return self._unary_operation(operator.pos, '__pos__')

	def __abs__(self):
		return self._unary_operation(operator.abs, '__abs__')

	def __invert__(self):
		# For boolean vectors, use logical NOT instead of bitwise NOT
		if self._dtype and self._dtype.kind is bool:
			return Vector(
				tuple(not x for x in self),
				dtype=self._dtype,
				name=self._name,
				as_row=self._display_as_row
			)
		return self._unary_operation(operator.invert, '__invert__')

	def __truediv__(self, other):
		return self._elementwise_operation(other, operator.truediv, '__truediv__', '/')

	def __floordiv__(self, other):
		return self._elementwise_operation(other, operator.floordiv, '__floordiv__', '//')

	def __mod__(self, other):
		return self._elementwise_operation(other, operator.mod, '__mod__', '%')

	def __pow__(self, other):
		return self._elementwise_operation(other, operator.pow, '__pow__', '**')

	def __radd__(self, other):
		"""Reverse addition: other + self (handles strings specially)"""
		other = self._check_duplicate(other)
		
		# Vector + Vector
		if isinstance(other, Vector):
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			vals = []
			for x, y in zip(other, self, strict=True):
				if x is None or y is None:
					vals.append(None)
				else:
					vals.append(x + y)
			return Vector(vals, dtype=self._dtype, name=None, as_row=self._display_as_row)
		
		# Scalar + Vector
		if not isinstance(other, Iterable) or isinstance(other, (str, bytes, bytearray)):
			vals = []
			for x in self:
				if x is None:
					vals.append(None)
				else:
					vals.append(other + x)
			return Vector(vals, dtype=self._dtype, name=None, as_row=self._display_as_row)
		
		# Iterable + Vector
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			vals = []
			for x, y in zip(other, self, strict=True):
				if x is None or y is None:
					vals.append(None)
				else:
					vals.append(x + y)
			return Vector(vals, dtype=self._dtype, name=None, as_row=self._display_as_row)
		
		raise SerifTypeError(f"Unsupported operand type: {type(other).__name__}")

	def __rmul__(self, other):
		return self.__mul__(other)

	def __rsub__(self, other):
		return self._elementwise_operation(other, _reverse_sub, '__rsub__', '-')

	def __rtruediv__(self, other):
		return self._elementwise_operation(other, _reverse_truediv, '__rtruediv__', '/')

	def __rfloordiv__(self, other):
		return self._elementwise_operation(other, _reverse_floordiv, '__rfloordiv__', '//')

	def __rmod__(self, other):
		return self._elementwise_operation(other, _reverse_mod, '__rmod__', '%')

	def __rpow__(self, other):
		return self._elementwise_operation(other, _reverse_pow, '__rpow__', '**')


	def _promote(self, new_dtype):
		""" Check if a vector can change data type (int -> float, float -> complex) """
		# Handle both Python types and DataType instances
		if isinstance(new_dtype, DataType):
			target_kind = new_dtype.kind
		elif isinstance(new_dtype, type):
			# Python type like int, float
			target_kind = new_dtype
		else:
			raise SerifTypeError(f"new_dtype must be a DataType instance or Python type, not {type(new_dtype).__name__}")
			
		# Already the target type
		if self._dtype.kind is target_kind:
			return
		
		# Allow numeric promotions: int -> float, float -> complex
		if target_kind is float and self._dtype.kind is int:
			old_tuple_id = id(self._underlying)
			new_tuple = tuple(float(x) if x is not None else None for x in self._underlying)
			_ALIAS_TRACKER.unregister(self, old_tuple_id)
			self._underlying = new_tuple
			_ALIAS_TRACKER.register(self, id(new_tuple))
			self._dtype = DataType(float, nullable=self._dtype.nullable)
		elif target_kind is complex and self._dtype.kind in (int, float):
			old_tuple_id = id(self._underlying)
			new_tuple = tuple(complex(x) if x is not None else None for x in self._underlying)
			_ALIAS_TRACKER.unregister(self, old_tuple_id)
			self._underlying = new_tuple
			_ALIAS_TRACKER.register(self, id(new_tuple))
			self._dtype = DataType(complex, nullable=self._dtype.nullable)
		elif target_kind is datetime and self._dtype.kind is date:
			old_tuple_id = id(self._underlying)
			new_tuple = tuple(datetime.combine(x, datetime.min.time()) if x is not None else None for x in self._underlying)
			_ALIAS_TRACKER.unregister(self, old_tuple_id)
			self._underlying = new_tuple
			_ALIAS_TRACKER.register(self, id(new_tuple))
			self._dtype = DataType(datetime, nullable=self._dtype.nullable)
		else:
			# For backwards compat, raise error if trying invalid promotion
			raise SerifTypeError(f'Cannot convert Vector from {self._dtype.kind.__name__} to {target_kind.__name__}.')
		return

	def ndims(self):
		return len(self.shape)

	def cols(self, key=None):
		if isinstance(key, int):
			return self._underlying[key]
		if isinstance(key, slice):
			return self._underlying[key]
		return self._underlying

	"""
	Recursive Vector Operations
	"""
	def max(self):
		if self.ndims() == 2:
			return self.copy((c.max() for c in self.cols()), name=None).T
		return max(self)

	def min(self):
		if self.ndims() == 2:
			return self.copy((c.min() for c in self.cols()), name=None).T
		return min(self)

	def sum(self):
		if self.ndims() == 2:
			return self.copy((c.sum() for c in self.cols()), name=None).T
		# Exclude None values from sum
		return sum(v for v in self._underlying if v is not None)

	def all(self):
		"""Return True if all elements are truthy (excluding None)."""
		if self.ndims() == 2:
			return self.copy((c.all() for c in self.cols()), name=None).T
		return all(v for v in self._underlying if v is not None)

	def any(self):
		"""Return True if any element is truthy (excluding None)."""
		if self.ndims() == 2:
			return self.copy((c.any() for c in self.cols()), name=None).T
		return any(v for v in self._underlying if v is not None)

	def mean(self):
		if self.ndims() == 2:
			return self.copy((c.mean() for c in self.cols()), name=None).T
		# Exclude None values from mean
		non_none = [v for v in self._underlying if v is not None]
		return sum(non_none) / len(non_none) if non_none else None

	def stdev(self, population=False):
		if self.ndims() == 2:
			return self.copy((c.stdev(population) for c in self.cols()), name=None).T
		# Exclude None values from stdev
		non_none = [v for v in self._underlying if v is not None]
		if len(non_none) < 2:
			return None
		m = sum(non_none) / len(non_none)
		# use in-place sum over generator for fastness. I AM SPEED!
		# This is still 10x slower than numpy.
		num = sum((x-m)*(x-m) for x in non_none)
		return (num/(len(non_none) - 1 + population))**0.5

	def unique(self):
		seen = set()
		out = []

		# Fast path: hashable
		try:
			for x in self._underlying:
				if x not in seen:
					seen.add(x)
					out.append(x)
			return Vector(out)
		except TypeError:
			pass   # fall through → slow path

		# Slow path: unhashables
		out = []
		for x in self._underlying:
			if not any(x == y for y in out):
				out.append(x)
		return Vector(out)


	def argsort(self):
		return [i for i, _ in sorted(enumerate(self._underlying), key=lambda x: x[1])]

	def pluck(self, key, default=None):
		"""Extract a key/index from each element, returning default if not found.
		
		Works with dicts, lists, tuples, strings, or any subscriptable object.
		"""
		results = []
		for item in self._underlying:
			# If item is None, can't subscript it
			if item is None:
				results.append(default)
				continue
			
			try:
				results.append(item[key])
			except (KeyError, IndexError, TypeError):
				# KeyError: dict key missing
				# IndexError: list/tuple index out of range
				# TypeError: item not subscriptable
				results.append(default)
		
		return Vector(results)

	def sort_by(self, reverse=False, na_last=True):
		"""
		Stable sort. Returns a new Vector.

		Parameters
		----------
		reverse : bool
			Sort in descending order if True
		na_last : bool
			If True, None sorts after all valid values.
			If False, None sorts before all valid values.
		
		Returns
		-------
		Vector
			Sorted vector with same dtype
		"""
		# Build key for each element
		if na_last:
			key_fn = lambda x: (x is None, x if x is not None else 0)
		else:
			key_fn = lambda x: (0 if x is None else 1, x if x is not None else 0)
		
		new_values = tuple(sorted(self._underlying, key=key_fn, reverse=reverse))

		# dtype DOES NOT change — preserving nullability and kind
		new_vector = Vector(new_values, dtype=self._dtype, name=self._name)

		return new_vector


	def _check_duplicate(self, other):
		if id(self) == id(other):
			# If the object references match, we need to copy other
			# return Vector((x for x in other), other._default, other._dtype, other._typesafe)
			return deepcopy(other)
		return other


	def _check_native_typesafe(self, other):
		""" Ensure native type conversions (python) will not affect underlying type """
		dtype_kind = self._dtype.kind
		if not dtype_kind:
			return True
		if dtype_kind == type(other):
			return True
		if dtype_kind == Vector:
			return True
		if not (not self._dtype.nullable or isinstance(other, Iterable)):
			return True
		if dtype_kind == float and type(other) == int: # includes bool since isinstance(True, int) returns True
			return True
		if dtype_kind == complex and type(other) in (int, float): # ditto
			return True
		return False


	def __matmul__(self, other):
		"""
		Universal Matrix Multiplication / Dot Product.
		Logic is centralized here to keep Table lightweight.
		
		Implements:
		1. Vector @ Vector -> Scalar (Dot Product)
		2. Matrix @ Vector -> Vector (Linear Combination)
		3. Matrix @ Matrix -> Matrix (Recursive columns)
		"""
		other = self._check_duplicate(other)
		
		# === CASE 1: SELF IS MATRIX (2D) ===
		if self.ndims() == 2:
			
			# 1a. Matrix @ Matrix
			# Recursive: This Matrix @ Each Column of Other
			if hasattr(other, 'cols') and other.ndims() == 2:
				# Returns a tuple of vectors, wrapped in a new Vector (which becomes Table)
				result = self.copy(tuple(self @ col for col in other.cols()))
				result.name = None
				return result

			# 1b. Matrix @ Vector
			# The "Trick": Linear Combination of Columns
			# Result = sum(Column_i * Scalar_i)
			if isinstance(other, Vector):
				cols = self.cols() 
				
				if len(cols) != len(other):
						raise SerifValueError(f"Dim mismatch: Matrix cols {len(cols)} != Vector len {len(other)}")
				
				# OPTIMIZATION: Access other._underlying directly to avoid index overhead in loop
				scalars = other._underlying
				
				if not cols:
					return Vector([])

				# Start accumulator with first term (Col_0 * Scalar_0)
				acc = cols[0] * scalars[0]
				
				# Add remaining terms
				for i in range(1, len(cols)):
					acc = acc + (cols[i] * scalars[i])
				
				return acc

		# === CASE 2: SELF IS VECTOR (1D) ===
		
		# 2a. Vector @ Matrix
		# Broadcast self against columns of matrix
		if hasattr(other, 'cols') and other.ndims() == 2:
				return Vector(tuple(self @ col for col in other.cols()))

		# 2b. Vector @ Vector (Dot Product)
		# Standard sum of products
		if len(self) != len(other):
			raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
		return sum(x*y for x, y in zip(self._underlying, other._underlying, strict=True))

	def __rmatmul__(self, other):
		other = self._check_duplicate(other)
		if len(self.shape) > 1:
			return Vector(tuple(x @ other for x in self.cols()))
		if len(self) != len(other):
			raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
		return sum(x*y for x, y in zip(self._underlying, other, strict=True))
		raise SerifTypeError(f"Unsupported operand type(s) for '*': '{self._dtype.__name__}' and '{type(other).__name__}'.")


	def __bool__(self):
		"""
		Raises an error because using a vector in a boolean context is ambiguous.
		
		Users often mistakenly use 'if vec' when they mean 'if vec.any()' or 'if vec.all()'.
		Use len(vec) > 0 to check for emptiness.
		"""
		raise TypeError(
			"Vector cannot be used in a boolean context (e.g., 'if vector:'). "
			"Use .any() or .all() for element-wise checks, or len(vector) > 0 to check for emptiness."
		)


	def __lshift__(self, other):
		""" The << operator behavior has been overridden to attempt to concatenate (append) the new array to the end of the first
		"""
		if self._dtype.kind in (bool, int) and isinstance(other, int):
			warnings.warn(f"The behavior of >> and << have been overridden for concatenation. Use .bitshift() to shift bits.")

		if isinstance(other, Vector):
			if not self._dtype.nullable and not other.schema().nullable and self._dtype.kind != other.schema().kind:
				raise SerifTypeError("Cannot concatenate two typesafe Vectors of different types")
			return Vector(self._underlying + other._underlying,
				dtype=self._dtype)
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			return Vector(self._underlying + tuple(other),
				dtype=self._dtype)
		return Vector(self._underlying + (other,),
				dtype=self._dtype)


	def __rshift__(self, other):
		""" The >> operator behavior has been overridden to add the column(s) of other to self
		"""
		if self._dtype.kind in (bool, int) and isinstance(other, int):
			warnings.warn(f"The behavior of >> and << have been overridden for concatenation. Use .bitshift() to shift bits.")

		if type(other).__name__ == 'Table':
			if not self._dtype.nullable and not other.schema().nullable and self._dtype.kind != other.schema().kind:
				raise SerifTypeError("Cannot concatenate two typesafe Vectors of different types")
			return Vector((self,) + other.cols(),
				dtype=self._dtype)
		if isinstance(other, Vector):
			if not self._dtype.nullable and not other.schema().nullable and self._dtype.kind != other.schema().kind:
				raise SerifTypeError("Cannot concatenate two typesafe Vectors of different types")
			return Vector((self,) + (other,),
				dtype=self._dtype)
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			return Vector([self, Vector(tuple(x for x in other))],
				dtype=self._dtype)
		elif not self:
			return Vector((other,),
				dtype=self._dtype)
		raise SerifTypeError("Cannot add a column of constant values. Try using Vector.new(element, length).")

	def __rlshift__(self, other):
		""" The << operator behavior has been overridden to attempt to concatenate (append)
		Handles: other << self (where other is not a Vector)
		"""
		# Convert other to Vector and concatenate with self
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			return Vector(tuple(other) + self._underlying,
				None,  # other doesn't have a default element
				None,
				False)
		# Scalar case: [other] + self
		return Vector((other,) + self._underlying,
			None,
			None,
			False)

	def __rrshift__(self, other):
		""" The >> operator behavior has been overridden to add columns
		Handles: other >> self (where other is not a Vector)
		Creates a table with other as first column(s) and self as additional column(s)
		"""
		# Convert other to Vector and combine column-wise
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			return Vector((Vector(tuple(other)), self),
				None,
				None,
				False)
		# Scalar case: create a single-element vector for other
		return Vector((Vector((other,)), self),
			None,
			None,
			False)

	
	def bit_lshift(self, other):
		"""
		Bitwise left shift (<<).
		Explicit method since '<<' operator is used for concatenation.
		"""
		return self._elementwise_operation(other, operator.lshift, 'bit_lshift', '<<')

	def bit_rshift(self, other):
		"""
		Bitwise right shift (>>).
		Explicit method since '>>' operator is used for column addition.
		"""
		return self._elementwise_operation(other, operator.rshift, 'bit_rshift', '>>')


	def __getattr__(self, name):
		"""Proxy attribute access to underlying dtype.
		
		Distinguishes between properties (like date.year) and methods (like str.replace):
		- Properties are evaluated immediately and return a Vector
		- Methods return a MethodProxy that waits for () to be called
		"""
		# 1. If we are untyped (object), don't guess. Explicit > Implicit.
		# Use __dict__ to avoid recursive __getattr__ calls
		schema = object.__getattribute__(self, 'schema')()
		if schema is None:
			raise AttributeError(f"Empty Vector has no attribute '{name}'")
		dtype_kind = schema.kind
		if dtype_kind is object:
			raise AttributeError(f"Vector[object] has no attribute '{name}'")
		
		# 2. Inspect the class definition of the type we are holding
		# getattr(cls, name) returns the actual class member (method, property, slot)
		cls_attr = getattr(dtype_kind, name, None)
		
		if cls_attr is None:
			# If the class doesn't have it, we definitely don't have it
			raise AttributeError(f"'{dtype_kind.__name__}' object has no attribute '{name}'")
		
		# 3. Check if it's callable at the class level
		# If it's callable, it's a method. If not, it's a property/descriptor.
		if callable(cls_attr):
			# It's a method -> Return the proxy to wait for ()
			return MethodProxy(self, name)
		else:
			# property (non-callable attribute)
			return Vector(tuple(
				getattr(x, name) if x is not None else None
				for x in self._underlying
			))


class _Float(Vector):
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)


class _Int(Vector):
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)


class _String(Vector):
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

	def capitalize(self):
		""" Call the internal capitalize method on string """
		return Vector(tuple((s.capitalize() if s is not None else None) for s in self._underlying))

	def casefold(self):
		""" Call the internal casefold method on string """
		return Vector(tuple((s.casefold() if s is not None else None) for s in self._underlying))

	def center(self, *args, **kwargs):
		""" Call the internal center method on string """
		return Vector(tuple((s.center(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def count(self, *args, **kwargs):
		""" Call the internal count method on string """
		return Vector(tuple((s.count(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def encode(self, *args, **kwargs):
		""" Call the internal encode method on string """
		return Vector(tuple((s.encode(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def endswith(self, *args, **kwargs):
		""" Call the internal endswith method on string """
		return Vector(tuple((s.endswith(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def expandtabs(self, *args, **kwargs):
		""" Call the internal expandtabs method on string """
		return Vector(tuple((s.expandtabs(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def find(self, *args, **kwargs):
		""" Call the internal find method on string """
		return Vector(tuple((s.find(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def format(self, *args, **kwargs):
		""" Call the internal format method on string """
		return Vector(tuple((s.format(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def format_map(self, *args, **kwargs):
		""" Call the internal format_map method on string """
		return Vector(tuple((s.format_map(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def index(self, *args, **kwargs):
		""" Call the internal index method on string """
		return Vector(tuple((s.index(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isalnum(self, *args, **kwargs):
		""" Call the internal isalnum method on string """
		return Vector(tuple((s.isalnum(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isalpha(self, *args, **kwargs):
		""" Call the internal isalpha method on string """
		return Vector(tuple((s.isalpha(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isascii(self, *args, **kwargs):
		""" Call the internal isascii method on string """
		return Vector(tuple((s.isascii(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isdecimal(self, *args, **kwargs):
		""" Call the internal isdecimal method on string """
		return Vector(tuple((s.isdecimal(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isdigit(self, *args, **kwargs):
		""" Call the internal isdigit method on string """
		return Vector(tuple((s.isdigit(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isidentifier(self, *args, **kwargs):
		""" Call the internal isidentifier method on string """
		return Vector(tuple((s.isidentifier(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def islower(self, *args, **kwargs):
		""" Call the internal islower method on string """
		return Vector(tuple((s.islower(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isnumeric(self, *args, **kwargs):
		""" Call the internal isnumeric method on string """
		return Vector(tuple((s.isnumeric(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isprintable(self, *args, **kwargs):
		""" Call the internal isprintable method on string """
		return Vector(tuple((s.isprintable(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isspace(self, *args, **kwargs):
		""" Call the internal isspace method on string """
		return Vector(tuple((s.isspace(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def istitle(self, *args, **kwargs):
		""" Call the internal istitle method on string """
		return Vector(tuple((s.istitle(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isupper(self, *args, **kwargs):
		""" Call the internal isupper method on string """
		return Vector(tuple((s.isupper(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def join(self, *args, **kwargs):
		""" Call the internal join method on string """
		return Vector(tuple((s.join(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def ljust(self, *args, **kwargs):
		""" Call the internal ljust method on string """
		return Vector(tuple((s.ljust(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def lower(self, *args, **kwargs):
		""" Call the internal lower method on string """
		return Vector(tuple((s.lower(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def lstrip(self, *args, **kwargs):
		""" Call the internal lstrip method on string """
		return Vector(tuple((s.lstrip(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def maketrans(self, *args, **kwargs):
		""" Call the internal maketrans method on string """
		return Vector(tuple((s.maketrans(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def partition(self, *args, **kwargs):
		""" Call the internal partition method on string """
		return Vector(tuple((s.partition(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def removeprefix(self, *args, **kwargs):
		""" Call the internal removeprefix method on string """
		return Vector(tuple((s.removeprefix(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def removesuffix(self, *args, **kwargs):
		""" Call the internal removesuffix method on string """
		return Vector(tuple((s.removesuffix(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def replace(self, *args, **kwargs):
		""" Call the internal replace method on string """
		return Vector(tuple((s.replace(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def rfind(self, *args, **kwargs):
		""" Call the internal rfind method on string """
		return Vector(tuple((s.rfind(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def rindex(self, *args, **kwargs):
		""" Call the internal rindex method on string """
		return Vector(tuple((s.rindex(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def rjust(self, *args, **kwargs):
		""" Call the internal rjust method on string """
		return Vector(tuple((s.rjust(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def rpartition(self, *args, **kwargs):
		""" Call the internal rpartition method on string """
		return Vector(tuple((s.rpartition(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def rsplit(self, *args, **kwargs):
		""" Call the internal rsplit method on string """
		return Vector(tuple((s.rsplit(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def rstrip(self, *args, **kwargs):
		""" Call the internal rstrip method on string """
		return Vector(tuple((s.rstrip(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def split(self, *args, **kwargs):
		""" Call the internal split method on string """
		return Vector(tuple((s.split(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def splitlines(self, *args, **kwargs):
		""" Call the internal splitlines method on string """
		return Vector(tuple((s.splitlines(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def startswith(self, *args, **kwargs):
		""" Call the internal startswith method on string """
		return Vector(tuple((s.startswith(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def strip(self, *args, **kwargs):
		""" Call the internal strip method on string """
		return Vector(tuple((s.strip(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def swapcase(self, *args, **kwargs):
		""" Call the internal swapcase method on string """
		return Vector(tuple((s.swapcase(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def title(self, *args, **kwargs):
		""" Call the internal title method on string """
		return Vector(tuple((s.title(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def translate(self, *args, **kwargs):
		""" Call the internal translate method on string """
		return Vector(tuple((s.translate(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def upper(self, *args, **kwargs):
		""" Call the internal upper method on string """
		return Vector(tuple((s.upper(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def zfill(self, *args, **kwargs):
		""" Call the internal zfill method on string """
		return Vector(tuple((s.zfill(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def before(self, sep):
		"""Return the part of each string before the first occurrence of sep."""
		return Vector(tuple((s.partition(sep)[0] if s is not None else None) for s in self._underlying))

	def after(self, sep):
		"""Return the part of each string after the first occurrence of sep."""
		return Vector(tuple((s.partition(sep)[2] if s is not None else None) for s in self._underlying))

	def before_last(self, sep):
		"""Return the part of each string before the last occurrence of sep."""
		return Vector(tuple((s.rpartition(sep)[0] if s is not None else None) for s in self._underlying))

	def after_last(self, sep):
		"""Return the part of each string after the last occurrence of sep."""
		return Vector(tuple((s.rpartition(sep)[2] if s is not None else None) for s in self._underlying))


class _Date(Vector):
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

	def _elementwise_compare(self, other, op):
		other = self._check_duplicate(other)
		if isinstance(other, Vector):
			# Raise mismatched lengths
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			if other.schema().kind == str:
				return Vector(tuple(bool(op(x, date.fromisoformat(y))) for x, y in zip(self, other, strict=True)), dtype=DataType(bool))
			if other.schema().kind == datetime:
				return Vector(tuple(bool(op(datetime.combine(x, datetime.time(0, 0)), y)) for x, y in zip(self, other, strict=True)), dtype=DataType(bool))
		elif isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			# Raise mismatched lengths
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			# If it's not a Vector or Constant, don't apply date compare logic
			return Vector(tuple(bool(op(x, y)) for x, y in zip(self, other, strict=True)), dtype=DataType(bool))
		elif isinstance(other, str):
			return Vector(tuple(bool(op(x, date.fromisoformat(other))) for x in self), dtype=DataType(bool))
		elif isinstance(other, datetime):
			return Vector(tuple(bool(op(datetime.combine(x, datetime.time(0, 0)), other)) for x in self), dtype=DataType(bool))
		# finally, 
		return super()._elementwise_compare(other, op)


	def ctime(self, *args, **kwargs):
		return Vector(tuple((s.ctime(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromisocalendar(self, *args, **kwargs):
		return Vector(tuple((s.fromisocalendar(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromisoformat(self, *args, **kwargs):
		return Vector(tuple((s.fromisoformat(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromordinal(self, *args, **kwargs):
		return Vector(tuple((s.fromordinal(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromtimestamp(self, *args, **kwargs):
		return Vector(tuple((s.fromtimestamp(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isocalendar(self, *args, **kwargs):
		return Vector(tuple((s.isocalendar(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isoformat(self, *args, **kwargs):
		return Vector(tuple((s.isoformat(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isoweekday(self, *args, **kwargs):
		return Vector(tuple((s.isoweekday(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def replace(self, *args, **kwargs):
		return Vector(tuple((s.replace(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def strftime(self, *args, **kwargs):
		return Vector(tuple((s.strftime(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def timetuple(self, *args, **kwargs):
		return Vector(tuple((s.timetuple(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def today(self, *args, **kwargs):
		return Vector(tuple((s.today(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def toordinal(self, *args, **kwargs):
		return Vector(tuple((s.toordinal(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def weekday(self, *args, **kwargs):
		return Vector(tuple((s.weekday(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def __add__(self, other):
		""" adding integers is adding days """
		if isinstance(other, Vector) and other.schema().kind == int:
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			return Vector(tuple(
				(date.fromordinal(s.toordinal() + y) if s is not None and y is not None else None)
				for s, y in zip(self._underlying, other, strict=True)
			))

		if isinstance(other, int):
			return Vector(tuple((date.fromordinal(s.toordinal() + other) if s is not None else None) for s in self._underlying))
		return super().add(other)

	def eomonth(self):
		out = []
		for d in self._underlying:
			if d is None:
				out.append(None)
				continue

			# move to first of next month
			first_next = (d.replace(day=28) + timedelta(days=4)).replace(day=1)

			# back up one day -> last day of original month
			last = first_next - timedelta(days=1)

			out.append(last)

		return Vector(tuple(out))




