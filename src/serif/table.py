import operator
import warnings
from collections.abc import Iterable

from .vector import Vector

from .naming import _sanitize_user_name

from .errors import SerifKeyError
from .errors import SerifValueError
from .errors import SerifTypeError


def _missing_col_error(name, context="Table"):
	return SerifKeyError(f"Column '{name}' not found in {context}")


def _parse_indexed_attr(attr):
	"""
	Parse attribute name for indexed column access pattern.
	
	Returns (base_name, column_index) if attr matches 'name__N' pattern,
	otherwise returns (attr, None).
	
	Examples:
		'total' → ('total', None)
		'total__5' → ('total', 5)
		'total__abc' → ('total__abc', None)
		'__5' → error (no base name)
	"""
	base, sep, suffix = attr.rpartition('__')
	
	# If sep is empty, no '__' found → regular attribute
	# If suffix isn't all digits → regular attribute
	# If base is empty → error (e.g., '__5')
	if sep and suffix.isdigit():
		if not base:
			raise AttributeError(f"Invalid indexed accessor '{attr}': missing base name")
		# re-sanitize for method collisions
		return (_sanitize_user_name(base), int(suffix))
	
	return (attr, None)


def _resolve_binary_name(left_name, right_name):
	"""
	Apply left-biased naming rules for binary operations between columns.
	
	Rules:
	- If right is None or matches left: keep left (even if left is None)
	- If left is None but right is not: drop to None, return warning info
	- If both named but different: drop to None, return warning info
	
	Returns:
		tuple: (result_name, warning_case)
		       warning_case is None, "mismatch", or "right-named-left-unnamed"
	"""
	if right_name is None or right_name == left_name:
		# Keep left name (including if left is None)
		return (left_name, None)
	
	if left_name is None:
		# Case B: left unnamed, right named
		return (None, "right-named-left-unnamed")
	
	# Case A: both named but different
	return (None, "mismatch")


class Row(Vector):
	"""
	Row behaves like a Vector (math, logic, isinstance), but it is a 
	zero-copy view into the Table's columns.
	
	We deliberately bypass Vector.__init__ to avoid O(N) scans, 
	fingerprinting, and alias tracking during iteration.
	"""
	__slots__ = ('_raw_cols', '_column_map', '_index', '_dtype')
	
	def __new__(cls, table, index=0):
		# Bypass Vector.__new__ entirely.
		# This prevents the infinite loop of checking "is the input iterable?"
		return object.__new__(cls)

	def __init__(self, table, index=0):
		# SNAPSHOT: Grab raw column lists for speed
		self._raw_cols = [col._underlying for col in table._underlying]
		self._column_map = table._column_map
		self._index = index
		
		# Smart Dtype Inference (Runs once per table iteration/access)
		# If all columns are the same type, the row is that type.
		# Otherwise, it's an object vector.
		from .typing import DataType
		
		if not table._underlying:
			self._dtype = DataType(object, nullable=True)
		else:
			# Check uniformity of column types
			col_dtypes = [col._dtype for col in table._underlying]
			unique_kinds = {dt.kind for dt in col_dtypes}
			
			if len(unique_kinds) == 1:
				# Homogeneous (Matrix-like)
				kind = unique_kinds.pop()
				# If ANY column is nullable, the row vector must be nullable
				is_nullable = any(dt.nullable for dt in col_dtypes)
				self._dtype = DataType(kind, nullable=is_nullable)
			else:
				# Heterogeneous (DataFrame-like)
				self._dtype = DataType(object, nullable=True)

		# CRITICAL: We DO NOT call super().__init__()
		# calling Vector.__init__ would materialize the data and kill performance.
		# We are a "Hollow" Vector.

	def set_index(self, index):
		""" Mutable iterator pattern for speed """
		self._index = index
		return self

	@property
	def _underlying(self):
		"""
		If a Vector method asks for 'self._underlying', we materialize it on demand.
		This is the "Lazy" part. We don't build the tuple until you do math.
		"""
		return tuple(col[self._index] for col in self._raw_cols)

	@property
	def shape(self):
		"""
		Recursive shape check.
		1. Standard Vector: (len,)
		2. Vector of Vectors/Tables: (len, inner_dims...)
		"""
		my_len = len(self._raw_cols)
		if my_len == 0:
			return (0,)
		
		# Peek at the first element (using raw access to avoid object creation)
		# to see if it has dimensions (is a Vector/Table)
		first_val = self._raw_cols[0][self._index]
		
		if hasattr(first_val, 'shape'):
			return (my_len,) + first_val.shape
		
		return (my_len,)

	def __repr__(self):
		# Custom repr to look like a Row, not a Vector
		idx = self._index
		values = [repr(col[idx]) for col in self._raw_cols]
		return f"Row({idx}: {', '.join(values)})"

	def __getattr__(self, attr):
		# 1. Try column names first (Row behavior)
		col_idx = self._column_map.get(attr.lower())
		if col_idx is not None:
			return self._raw_cols[col_idx][self._index]
			
		# 2. Fall back to Vector methods (sum, mean, cast, etc.)
		return super().__getattr__(attr)

	def __getitem__(self, key):
		# Optimized hot path for loops
		if type(key) is int:
			 return self._raw_cols[key][self._index]
		
		if type(key) is str:
			 return getattr(self, key)
			 
		# Fallback to standard vector slicing/masking
		return super().__getitem__(key)

	def __iter__(self):
		# Fast iteration for unpacking: x, y, z = row
		idx = self._index
		for col in self._raw_cols:
			yield col[idx]

	def __len__(self):
		return len(self._raw_cols)


class Table(Vector):
	""" Multiple columns of the same length """
	_length = None
	_repr_rows = None  # Optional table-specific repr row count override
	
	def __new__(cls, initial=(), dtype=None, name=None, as_row=False):
		return super(Vector, cls).__new__(cls)

	def __init__(self, initial=(), dtype=None, name=None, as_row=False):
		# Handle dict initialization {name: values, ...}
		if isinstance(initial, dict):
			# Create Vectors with names from dict keys
			initial = [Vector(values, name=col_name) for col_name, values in initial.items()]
		
		self._length = len(initial[0]) if initial else 0
		
		# Deep copy columns to enforce value semantics
		# Tables receive snapshots of vectors, preventing aliasing
		# Save original names BEFORE copying
		original_names = [vec._name for vec in initial] if initial else []
		
		# Make copies of the vectors
		if initial:
			initial = tuple(vec.copy() for vec in initial)
		else:
			initial = ()
		
		# Set _dtype to None explicitly since Table bypasses Vector.__new__
		self._dtype = None
		self._column_map = None
		
		# Call parent constructor
		super().__init__(initial, dtype=dtype, name=name)
		
		# Restore column names after parent init
		# The parent Vector.__init__ may have modified self._underlying
		if original_names:
			for i, col_name in enumerate(original_names):
				if i < len(self._underlying):
					self._underlying[i]._name = col_name
		
		# Build column map
		self._column_map = self._build_column_map()

	def __len__(self):
		if len(self._underlying) == 0:
			return 0
		if isinstance(self._underlying[0], Table):
			return len(self._underlying)
		return self._length

	@property
	def shape(self):
		n_rows = len(self)
		if n_rows == 0:
			# Empty table - need to check column count
			n_cols = len(self._underlying) if hasattr(self, '_underlying') else 0
			return (0, n_cols)
		return (n_rows,) + self[0].shape

	def _build_column_map(self):
		"""Build mapping from sanitized column names to column indices.
		
		This is computed once during table initialization and used by
		PyRow for O(1) attribute lookups during iteration.
		"""
		column_map = {}
		seen = {}
		for idx, col in enumerate(self._underlying):
			if col._name is not None:
				base = _sanitize_user_name(col._name)
				if base is None:
					sanitized = f'col{idx}_'
				elif base in seen:
					# Warn only if ambiguity involves a wild column
					other = seen[base]
					if col._wild or other._wild:
						warnings.warn(
							f"Duplicate column name '{base}' detected. "
							"Dot access will be disambiguated with indexed suffixes.",
							UserWarning,
							stacklevel=2
						)
					
					# dont triple underscore if already ends with _
					sep = "" if base.endswith("_") else "_"
					sanitized = f"{base}{sep}_{idx}"
				else:
					sanitized = base
					seen[base] = col
			else:
				sanitized = f'col{idx}_'

			column_map[sanitized] = idx
			col._mark_tame()

		return column_map
	
	def __dir__(self):
		"""Return list of available attributes including sanitized column names."""
		# Use object.__dir__ to get instance attributes, then add column names
		base_attrs = object.__dir__(self)
		return set(list(self._build_column_map().keys()) + base_attrs)
	
	def column_names(self):
		"""Return list of column names (original names, not sanitized).
		
		Returns
		-------
		list
			List of column names. None for unnamed columns.
		
		Examples
		--------
		>>> t = Table({'x': [1, 2], 'y': [3, 4]})
		>>> t.column_names()
		['x', 'y']
		"""
		return [col._name for col in self._underlying]

	def __getattr__(self, attr):
		"""Access columns by sanitized attribute name using pre-computed column map."""
		# Check if any column has been renamed and rebuild map if needed
		if any(col._wild for col in self._underlying or []):
			self._column_map = self._build_column_map()

		# Parse for indexed accessor pattern (e.g., 'total__5')
		base_name, col_idx = _parse_indexed_attr(attr)
		
		if col_idx is not None:
			# Indexed access: validate column index and name match
			if col_idx < 0 or col_idx >= len(self._underlying):
				raise AttributeError(
					f"Column index {col_idx} out of range (table has {len(self._underlying)} columns)"
				)
			
			# Get the actual column at that index
			col = self._underlying[col_idx]
			
			# Validate: does this column's sanitized name match base_name?
			from .naming import _sanitize_user_name
			sanitized = _sanitize_user_name(col._name)
			
			if sanitized != base_name.lower():
				raise AttributeError(
					f"Column {col_idx} is '{col._name}' (sanitizes to '{sanitized}'), not '{base_name}'"
				)
			
			return col

		# Fallback: col<N>_ accessor (e.g., col5_ for column index 5)
		elif attr.startswith('col') and attr.endswith('_'):
			middle = attr[3:-1]  # Extract between 'col' and '_'
			if middle.isdigit():
				idx = int(middle)
				if 0 <= idx < len(self._underlying):
					return self._underlying[idx]
				raise AttributeError(f"Column index {idx} out of range")
		
		else:
			# Regular access: look up by sanitized name
			col_idx_lookup = self._column_map.get(attr) or self._column_map.get(attr.lower())
			if col_idx_lookup is not None:
				return self._underlying[col_idx_lookup]
		
		# Fall back to parent class attributes (e.g., .T for transpose)
		try:
			return super().__getattribute__(attr)
		except AttributeError:
			# Attribute not found - raise AttributeError for Pythonic behavior
			raise AttributeError(f"{self.__class__.__name__!s} object has no attribute '{attr}'")

	def _resolve_column(self, spec):
		"""
		Resolve a column specification to a Vector.
		
		Parameters
		----------
		spec : str | Vector
			Column name (string) or Vector instance
		
		Returns
		-------
		Vector
			Resolved column from this table
		
		Raises
		------
		SerifKeyError
			If column name not found
		SerifTypeError
			If spec is neither str nor Vector
		"""
		if isinstance(spec, str):
			return self[spec]
		elif isinstance(spec, Vector):
			return spec
		else:
			raise SerifTypeError(
				f"Column specification must be string or Vector, got {type(spec).__name__}"
			)

	def __setattr__(self, attr, value):
		"""Intercept column assignments (t.colname = vec) to update underlying columns."""
		# Let instance attributes initialize normally (before __init__ completes)
		if attr in ('_underlying', '_length', '_column_map', '_dtype', '_name', '_display_as_row', '_fp', '_fp_powers', '_wild', '_repr_rows'):
			object.__setattr__(self, attr, value)
			return
		
		# After initialization, check if setting an existing column
		if self._column_map is not None:
			# Parse for indexed accessor pattern (e.g., 'total__5')
			base_name, col_idx_indexed = _parse_indexed_attr(attr)
			
			if col_idx_indexed is not None:
				# Indexed assignment: validate column index and name match
				if col_idx_indexed < 0 or col_idx_indexed >= len(self._underlying):
					raise AttributeError(
						f"Column index {col_idx_indexed} out of range (table has {len(self._underlying)} columns)"
					)
				
				# Validate: does this column's sanitized name match base_name?
				from .naming import _sanitize_user_name
				sanitized = _sanitize_user_name(self._underlying[col_idx_indexed]._name)
				
				if sanitized != base_name.lower():
					raise AttributeError(
						f"Column {col_idx_indexed} is '{self._underlying[col_idx_indexed]._name}' "
						f"(sanitizes to '{sanitized}'), not '{base_name}'"
					)
				
				# Replace the column at validated index
				if not isinstance(value, Vector):
					value = Vector(value)
				
				if self._underlying and len(value) != self._length:
					raise ValueError(
						f"Cannot assign column '{attr}': length {len(value)} != table length {self._length}"
					)
				
				cols = list(self._underlying)
				value._name = self._underlying[col_idx_indexed]._name  # Preserve original name
				cols[col_idx_indexed] = value
				object.__setattr__(self, '_underlying', tuple(cols))
				object.__setattr__(self, '_column_map', self._build_column_map())
				return
			
			# Regular column lookup by name
			col_idx = self._column_map.get(attr) or self._column_map.get(attr.lower())
			if col_idx is not None:
				# Replace the column in _underlying
				if not isinstance(value, Vector):
					value = Vector(value)
				
				# Validate length
				if self._underlying and len(value) != self._length:
					raise ValueError(
						f"Cannot assign column '{attr}': length {len(value)} != table length {self._length}"
					)
				
				# Replace column (tuples are immutable, so rebuild)
				cols = list(self._underlying)
				value._name = self._underlying[col_idx]._name  # Preserve original name
				cols[col_idx] = value
				object.__setattr__(self, '_underlying', tuple(cols))
				
				# Rebuild column map to reflect any structural changes
				object.__setattr__(self, '_column_map', self._build_column_map())
				return
		
		# Reject arbitrary attribute setting - only allow column updates
		raise AttributeError(
			f"Cannot set attribute '{attr}' on Table. "
			f"Column '{attr}' does not exist. Use >>= to add new columns."
		)

	def rename_column(self, old_name, new_name):
		"""Rename a column (modifies in place, returns self for chaining)"""
		for col in self._underlying:
			if col._name == old_name:
				col._name = new_name
				self._column_map = self._build_column_map()
				return self
		raise _missing_col_error(old_name)
	
	def rename_columns(self, old_names, new_names):
		"""
		Atomically rename multiple columns using parallel old_names and new_names lists.

		Rules:
		- old_names and new_names must be same length
		- each list-element renames EXACTLY ONE matching occurrence
		(left-to-right positional matching)
		- if renaming fails (old name not found), no columns are renamed and KeyError is raised
		"""

		if len(old_names) != len(new_names):
			raise SerifValueError("old_names and new_names must have the same length")

		# Simulate renames using a temporary list (avoid mid-state partial renames)
		simulated = [col._name for col in self._underlying]

		for old, new in zip(old_names, new_names):
			try:
				idx = simulated.index(old)
			except ValueError:
				raise _missing_col_error(old)
			simulated[idx] = new  # simulate rename

		# Apply renames for real
		for old, new in zip(old_names, new_names):
			# rename the FIRST matching column in the real table
			for col in self._underlying:
				if col._name == old:
					col._name = new
					break

		self._column_map = self._build_column_map()
		return self

	@property
	def T(self):
		if len(self.shape)==2:
			# Transpose 2D table: columns become rows
			num_rows = self._length
			num_cols = len(self._underlying)
			rows = []
			for row_idx in range(num_rows):
				row = Vector(tuple(col[row_idx] for col in self._underlying))
				rows.append(row)
			return Table(rows)
		return self.copy((tuple(x.T for x in self))) # higher dimensions

	def __getitem__(self, key):
		key = self._check_duplicate(key)
		
		# Handle string indexing for column names
		if isinstance(key, str):
			# Try exact match first
			for col in self._underlying:
				if col._name == key:
					return col
			
			# Try sanitized match (case-insensitive)
			key_lower = key.lower()
			seen = set()
			for idx, col in enumerate(self._underlying):
				if col._name is not None:
					base = _sanitize_user_name(col._name)
					# If sanitization returns None, match system name
					if base is None:
						if f'col{idx}_' == key_lower:
							return col
					elif base == key_lower:
						return col
					else:
						unique_name = f"{base}__{idx}"
						seen.add(unique_name)
						if unique_name == key_lower:
							return col
				else:
					# Unnamed columns: match col{idx}_ pattern
					if f'col{idx}_' == key_lower:
						return col
			
			raise _missing_col_error(key)
		
		# Handle tuple of strings for multi-column selection
		if isinstance(key, tuple) and all(isinstance(k, str) for k in key):
			# Multiple column selection by names
			selected_cols = []
			for col_name in key:
				found = False
				# Try exact match first
				for col in self._underlying:
					if col._name == col_name:
						selected_cols.append(col.copy())  # Copy to preserve original
						found = True
						break
				
				# Try sanitized match (case-insensitive)
				if not found:
					col_name_lower = col_name.lower()
					seen = set()
					for idx, col in enumerate(self._underlying):
						if col._name is not None:
							base = _sanitize_user_name(col._name)
							if base is None:
								if f'col{idx}_' == col_name_lower:
									selected_cols.append(col.copy())
									found = True
									break
							else:
								unique_name = f"{base }__{idx}"
								seen.add(unique_name)
								if unique_name == col_name_lower:
									selected_cols.append(col.copy())
									found = True
									break
						else:
							if f'col{idx}_' == col_name_lower:
								selected_cols.append(col.copy())
								found = True
								break

								if not found:
									raise _missing_col_error(col_name)
			return Table(selected_cols)
		
		if isinstance(key, tuple):
			if len(key) != len(self.shape):
				raise SerifKeyError(f"Matrix indexing must provide an index in each dimension: {self.shape}")

			# Reject 3+ dimensional indexing explicitly
			if len(key) > 2:
				raise SerifKeyError(
					f"Table only supports 2D indexing (row, column); "
					f"got {len(key)} indices."
				)

			# 2D indexing: [row_spec, col_spec]
			# Support both [rows, cols] and [cols, rows] by checking types
			row_spec, col_spec = key
			
			# Determine which is rows and which is columns
			# Rows: int or slice
			# Cols: int, slice, str, or tuple of strings
			row_is_first = isinstance(row_spec, (int, slice))
			
			if not row_is_first:
				# Swap if columns came first: [('a', 'b'), 1:3] -> [1:3, ('a', 'b')]
				row_spec, col_spec = col_spec, row_spec
			
			# Now row_spec is guaranteed to be rows, col_spec is columns
			
			# Get the row-sliced table first
			if isinstance(row_spec, slice):
				row_sliced = self[row_spec]  # Returns Table
			elif isinstance(row_spec, int):
				# Single row -> return PyRow, then index into it
				return self[row_spec][col_spec]
			else:
				raise SerifKeyError(f"Invalid row specifier: {type(row_spec)}")
			
			# Now select columns from the row-sliced table
			if isinstance(col_spec, int):
				# Single column by index
				return row_sliced.cols(col_spec)
			elif isinstance(col_spec, slice):
				# Column slice by index
				selected = row_sliced.cols()[col_spec]
				return Table(selected)
			elif isinstance(col_spec, str):
				# Single column by name
				return row_sliced[col_spec]
			elif isinstance(col_spec, tuple) and all(isinstance(k, str) for k in col_spec):
				# Multiple columns by name
				return row_sliced[col_spec]
			else:
				raise SerifKeyError(f"Invalid column specifier: {type(col_spec)}")

		if isinstance(key, int):
			# Effectively a different input type (single not a list). Returning a value, not a vector.
			if isinstance(self._underlying[0], Table):
				return self._underlying[key]
			return Row(self, key)

		if isinstance(key, Vector) and key.schema().kind == bool and not key.schema().nullable:
			assert (len(self) == len(key))
			return Vector(tuple(x[key] for x in self._underlying),
				dtype = self._dtype
			)
		if isinstance(key, list) and {type(e) for e in key} == {bool}:
			assert (len(self) == len(key))
			return Vector(tuple(x[key] for x in self._underlying),
				dtype = self._dtype
			)
		if isinstance(key, slice):
			return Vector(tuple(x[key] for x in self._underlying), 
				dtype = self._dtype,
				name=self._name
			)

		# NOT RECOMMENDED
		if isinstance(key, Vector) and key.schema().kind == int and not key.schema().nullable:
			if len(self) > 1000:
				warnings.warn('Subscript indexing is sub-optimal for large vectors; prefer slices or boolean masks')
			return Vector(tuple(x[key] for x in self._underlying),
				dtype = self._dtype
			)

	def __setitem__(self, key, value):
		"""
		Support for 2D assignment:
		1. t[row, col] = scalar
		2. t[row_idx, :] = [values]  (Row assignment)
		3. t[row_slice, col_slice] = other_table (Region assignment)
		"""
		row_spec, col_spec = None, None

		# --- 1. Normalize Key ---
		if isinstance(key, tuple):
			# t[row, col]
			if len(key) != 2:
				raise SerifKeyError("Table assignment requires 1D (row) or 2D (row, col) key.")
			row_spec, col_spec = key
		else:
			# t[row] or t[slice] -> implies all columns
			row_spec = key
			col_spec = slice(None)

		# --- 2. Resolve Target Columns ---
		# This replicates the lookup logic from __getitem__
		target_indices = []
		n_cols = len(self._underlying)
		
		if isinstance(col_spec, slice):
			target_indices = list(range(n_cols)[col_spec])
		elif isinstance(col_spec, int):
			target_indices = [col_spec]
		elif isinstance(col_spec, str):
			# Look up by name
			idx = self._column_map.get(col_spec) or self._column_map.get(col_spec.lower())
			if idx is None:
				raise SerifKeyError(f"Column '{col_spec}' not found")
			target_indices = [idx]
		elif isinstance(col_spec, (tuple, list)):
			# Handle list of names/ints
			for c in col_spec:
				if isinstance(c, str):
					idx = self._column_map.get(c) or self._column_map.get(c.lower())
					if idx is None:
						raise SerifKeyError(f"Column '{c}' not found")
					target_indices.append(idx)
				elif isinstance(c, int):
					target_indices.append(c)
		else:
			raise SerifTypeError(f"Invalid column index type: {type(col_spec)}")

		if not target_indices:
			return # No columns selected, nothing to do

		# --- 3. Handle Assignment ---
		
		# CASE A: Scalar Assignment (Broadcast)
		# t[0:5, 'A'] = 10
		if not isinstance(value, Iterable) or isinstance(value, (str, bytes, bytearray)):
			for col_idx in target_indices:
				self._underlying[col_idx][row_spec] = value
			return

		# CASE B: Single Row Assignment
		# t[0, :] = [1, 2, 3]
		if isinstance(row_spec, int):
			# Materialize generator to avoid exhaustion if reused
			val_seq = list(value)
			if len(val_seq) != len(target_indices):
				raise SerifValueError(
					f"Row assignment length mismatch: Table target has {len(target_indices)} columns, "
					f"but value has {len(val_seq)} items."
				)
			
			for i, col_idx in enumerate(target_indices):
				self._underlying[col_idx][row_spec] = val_seq[i]
			return

		# CASE C: Rectangular/Table Assignment
		# t[1:3, 2:4] = other_table
		if isinstance(value, Table):
			if len(value.cols()) != len(target_indices):
				raise SerifValueError(
					f"Column count mismatch: Target has {len(target_indices)} cols, "
					f"source table has {len(value.cols())} cols."
				)
			
			# We delegate row-length validation to the vector.__setitem__ calls below
			for i, col_idx in enumerate(target_indices):
				self._underlying[col_idx][row_spec] = value.cols()[i]
			return

		# CASE D: Raw 2D Iterable Assignment (List of Columns? List of Rows?)
		# Ambiguity Trap: Is [[1,2], [3,4]] two rows of two, or two columns of two?
		# Vector standard: "Iterables usually mean columns". 
		# If you pass a list of lists, we treat it as list-of-columns to match Table structure.
		# SPECIAL CASE: If we have a single target column and value is a flat list,
		# treat it as values for that column, not as multiple columns.
		if isinstance(value, (list, tuple)):
			# Single column slice assignment: t[:, 'x'] = [1, 2, 3]
			if len(target_indices) == 1:
				# Check if it's a flat list (not nested)
				if not value or not isinstance(value[0], (list, tuple, Vector)):
					# Flat list -> assign to the single column
					self._underlying[target_indices[0]][row_spec] = value
					return
			
			if len(value) != len(target_indices):
				raise SerifValueError(f"Shape mismatch: expected {len(target_indices)} columns/items.")
			
			# Assume value[i] corresponds to target_indices[i]
			for i, col_idx in enumerate(target_indices):
				self._underlying[col_idx][row_spec] = value[i]
			return

		raise SerifTypeError(f"Unsupported assignment value type: {type(value)}")

	def __iter__(self):
		"""
		Iterate over rows using the Fast View.
		Snapshots data state at start of iteration for performance.
		"""
		# Use the WET/Optimized view for loops
		row_view = Row(self, 0)
		
		# Cache length locally to avoid self.__len__() call in loop
		n = len(self)
		
		for i in range(n):
			# No object creation in loop - just index update
			yield row_view.set_index(i)

	def __repr__(self):
		from .display import _printr
		return _printr(self)

	def _elementwise_compare(self, other, op):
		other = self._check_duplicate(other)
		if isinstance(other, Vector):
			# Raise mismatched column counts
			if len(self.cols()) != len(other.cols()):
				raise ValueError(f"Column count mismatch: {len(self.cols())} != {len(other.cols())}")
			return Vector(tuple(op(x, y) for x, y in zip(self.cols(), other.cols(), strict=True)), False, bool, True)
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			# Raise mismatched row counts
			if len(self) != len(other):
				raise ValueError(f"Row count mismatch: {len(self)} != {len(other)}")
			return Vector(tuple(op(x, y) for x, y in zip(self, other, strict=True)), False, bool, True).T
		return Vector(tuple(op(x, other) for x in self.cols()), False, bool, True)

	def __rshift__(self, other):
		""" The >> operator behavior has been overridden to add the column(s) of other to self
		"""
		if self._dtype is not None and self._dtype.kind in (bool, int) and isinstance(other, int):
			warnings.warn(f"The behavior of >> and << have been overridden. Use .bitshift() to shift bits.")

		# Dict syntax: {name: values, ...}
		if isinstance(other, dict):
			# Convert dict to named Vectors
			named_cols = []
			for col_name, values in other.items():
				# Convert to Vector if needed
				if isinstance(values, Vector):
					col = values.copy()  # Copy to prevent aliasing
				elif isinstance(values, Iterable) and not isinstance(values, (str, bytes, bytearray)):
					col = Vector(values)
				else:
					# Reject scalars - user must be explicit
					raise ValueError(
						f"Column '{col_name}' value must be iterable (list, Vector, etc.), not scalar. "
						f"Use Vector.new({values!r}, {len(self)}) for scalar broadcast."
					)
				
				# Validate length
				if self._underlying and len(col) != self._length:
					raise ValueError(
						f"Column '{col_name}' has length {len(col)}, expected {self._length}"
					)
				
				# Set name
				col._name = col_name

				if _sanitize_user_name(col_name) in self._column_map:
					warnings.warn(f"Adding column with name '{col_name}' which already exists in the table. Consider renaming to avoid confusion.", UserWarning, stacklevel=2)
				named_cols.append(col)
			
			# Return new table with appended columns
			return Table(tuple(self._underlying) + tuple(named_cols))

		if isinstance(other, Table):
			if self._dtype is not None and not self._dtype.nullable and other.schema() is not None and not other.schema().nullable and self._dtype.kind != other.schema().kind:
				raise SerifTypeError("Cannot concatenate two typesafe Vectors of different types")
			# complicated typesafety rules here - what if a whole bunch of things.
			return Vector(self.cols() + other.cols(),
				dtype=self._dtype)
		if isinstance(other, Vector):
			# Adding a column to a table - tables can have mixed-type columns
			return Vector(self.cols() + (other,),
				dtype=self._dtype)
		if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			# Convert iterable to Vector and add as column (let Vector infer dtype)
			return Vector(self.cols() + (Vector(other),),
				dtype=self._dtype)
		elif not self:
			return Vector((other,),
				dtype=self._dtype)
		raise SerifTypeError("Cannot add a column of constant values. Try using Vector.new(element, length).")


	def __lshift__(self, other):
		""" The << operator behavior has been overridden to attempt to concatenate (append) the new array to the end of the first
		"""
		if isinstance(other, Table):
			if len(self.cols()) != len(other.cols()):
				raise ValueError(f"Column count mismatch: {len(self.cols())} != {len(other.cols())}")
			return Vector(tuple(x << y for x, y in zip(self.cols(), other.cols(), strict=True)))
		if len(self.cols()) != len(other):
			raise ValueError(f"Column count mismatch: {len(self.cols())} != {len(other)}")
		return Vector(tuple(x << y for x, y in zip(self.cols(), other, strict=True)))

	def _table_elementwise_operation(self, other, op_func, op_name: str, op_symbol: str):
		"""
		Handle Table-specific arithmetic with column name preservation.
		
		Rules:
		- Table + scalar: preserve all column names
		- Table + Table: left-biased naming with warnings for mismatches
		"""		
		# Scalar operation: preserve all column names
		if not isinstance(other, Table):
			result_cols = tuple(
				op_func(col, other) for col in self.cols()
			)
			# Restore original column names
			for orig_col, result_col in zip(self.cols(), result_cols):
				result_col._name = orig_col._name
				result_col._wild = orig_col._wild
			return Table(result_cols)
		
		# Table + Table: left-biased naming with warnings
		if len(self.cols()) != len(other.cols()):
			raise ValueError(f"Table width mismatch: {len(self.cols())} != {len(other.cols())}")
		
		result_cols = []
		warnings_to_emit = []
		
		for idx, (left_col, right_col) in enumerate(zip(self.cols(), other.cols())):
			result_col = op_func(left_col, right_col)
			
			# Apply naming rules
			result_name, warning_case = _resolve_binary_name(left_col._name, right_col._name)
			result_col._name = result_name
			result_col._wild = False  # Result is tame (part of new table structure)
			
			# Track warnings
			if warning_case is not None:
				warnings_to_emit.append((idx, left_col._name, right_col._name, warning_case))
			
			result_cols.append(result_col)
		
		# Emit consolidated warning if needed
		if warnings_to_emit:
			lines = [f"Table operation ({op_symbol}) produced unusual column naming in {len(warnings_to_emit)} column(s):"]
			for idx, left_name, right_name, case in warnings_to_emit:
				if case == "mismatch":
					lines.append(f"  idx {idx}: left={repr(left_name)} right={repr(right_name)} → dropped")
				else:  # right-named-left-unnamed
					lines.append(f"  idx {idx}: left=None right={repr(right_name)} → kept None (left-biased)")
			warnings.warn("\n".join(lines), UserWarning, stacklevel=2)
		
		return Table(tuple(result_cols))
	
	def __add__(self, other):
		return self._table_elementwise_operation(other, operator.add, '__add__', '+')
	
	def __sub__(self, other):
		return self._table_elementwise_operation(other, operator.sub, '__sub__', '-')
	
	def __mul__(self, other):
		return self._table_elementwise_operation(other, operator.mul, '__mul__', '*')
	
	def __truediv__(self, other):
		return self._table_elementwise_operation(other, operator.truediv, '__truediv__', '/')
	
	def __floordiv__(self, other):
		return self._table_elementwise_operation(other, operator.floordiv, '__floordiv__', '//')
	
	def __mod__(self, other):
		return self._table_elementwise_operation(other, operator.mod, '__mod__', '%')
	
	def __pow__(self, other):
		return self._table_elementwise_operation(other, operator.pow, '__pow__', '**')

	@staticmethod
	def _validate_key_tuple_hashable(key_tuple, key_cols, row_idx):
		"""
		Validate that a join key tuple is hashable (for object dtype columns).
		
		Args:
			key_tuple: The tuple of key values to validate
			key_cols: List of key column Vectors
			row_idx: Row index for error messages
		
		Raises:
			SerifTypeError: If any key component is not hashable
		"""
		try:
			hash(key_tuple)
		except TypeError as e:
			# Find which component failed
			for i, (component, col) in enumerate(zip(key_tuple, key_cols)):
				try:
					hash(component)
				except TypeError:
					col_name = col._name or f"key_{i}"
					raise SerifTypeError(
						f"Join key value in '{col_name}' at row {row_idx} is not hashable: "
						f"{type(component).__name__}. Join keys must be hashable."
					) from e
			# If we can't find the specific component, raise generic error
			raise SerifTypeError(
				f"Join key at row {row_idx} is not hashable."
			) from e

	def _validate_join_keys(self, other, left_on, right_on):
		"""
		Validate and normalize join key specification.
		
		Args:
			other: Right table to join with
			left_on: Column name(s) or Vector(s) from left table
			right_on: Column name(s) or Vector(s) from right table
		
		Returns:
			List of (left_col, right_col) tuples (Vector objects)
		
		Raises:
			SerifValueError: For malformed specs or validation failures
			SerifTypeError: For invalid dtypes or unhashable values
		"""
		from datetime import date, datetime
		
		# Helper: Resolve column from name or Vector
		def get_column(table, col_spec, side_name):
			try:
				return table._resolve_column(col_spec)
			except (SerifKeyError, ValueError):
				raise _missing_col_error(
					col_spec if isinstance(col_spec, str) else "column",
					context=f"{side_name} table"
				)
		
		# Helper: Validate column dtype for join keys (static type check)
		def validate_key_dtype(col, side_name, idx):
			schema = col.schema()
			if schema is None:
				# Empty/untyped vectors - validate at runtime below
				return
			
			kind = schema.kind
			
			# Floats are NOT allowed — non-deterministic equality
			if kind is float:
				raise SerifTypeError(
					f"Invalid join key dtype 'float' at position {idx} on {side_name} side. "
					"Floating-point columns cannot be used as join keys due to precision issues."
				)
			
			# Allowed types: hashable and have stable equality
			# complex is excluded (not typically used for joins, can be added if needed)
			allowed_types = (int, str, bool, date, datetime, object)
			if kind not in allowed_types:
				raise SerifTypeError(
					f"Invalid join key dtype '{kind.__name__}' at position {idx} on {side_name} side. "
					"Join keys must support stable equality and hashing."
				)
		
		# Normalize to lists
		if isinstance(left_on, (str, Vector)):
			left_on = [left_on]
		if isinstance(right_on, (str, Vector)):
			right_on = [right_on]
		
		if not (isinstance(left_on, list) and isinstance(right_on, list)):
			raise SerifValueError("left_on and right_on must be strings, Vectors, or lists")
		
		if not left_on or not right_on:
			raise SerifValueError("Must specify at least 1 join key")
		
		if len(left_on) != len(right_on):
			raise SerifValueError(
				f"left_on and right_on must have same length: "
				f"got {len(left_on)} and {len(right_on)}"
			)
		
		# Build final list of join key pairs
		normalized = []
		for i, (left_spec, right_spec) in enumerate(zip(left_on, right_on)):
			left_col = get_column(self, left_spec, "left")
			right_col = get_column(other, right_spec, "right")
			
			# Length validation
			if len(left_col) != len(self):
				raise SerifValueError(
					f"Left join key at index {i} has length {len(left_col)}, "
					f"but left table has {len(self)} rows"
				)
			if len(right_col) != len(other):
				raise SerifValueError(
					f"Right join key at index {i} has length {len(right_col)}, "
					f"but right table has {len(other)} rows"
				)
			
			# Dtype validation
			validate_key_dtype(left_col, "left", i)
			validate_key_dtype(right_col, "right", i)
			
			# Matching dtype validation (both must have schemas and same kind)
			left_schema = left_col.schema()
			right_schema = right_col.schema()
			if left_schema is not None and right_schema is not None:
				if left_schema.kind is not right_schema.kind:
					raise SerifTypeError(
						f"Join key at index {i} has mismatched dtypes: "
						f"{left_schema.kind.__name__} (left) vs {right_schema.kind.__name__} (right)"
					)
			
			normalized.append((left_col, right_col))
		
		return normalized

	def inner_join(self, other, left_on, right_on, expect='many_to_one'):
		"""
		Inner join two Tables on specified key columns.
		Only returns rows where keys match in both tables.
		
		Args:
			other: Table to join with
			left_on: Column name(s) or Vector(s) from left table
			right_on: Column name(s) or Vector(s) from right table
			expect: Cardinality expectation - 'one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'
		
		Returns:
			Table with joined results
		"""
		# Validate cardinality flag early
		if expect not in ('one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'):
			raise SerifValueError(
				f"Invalid expect='{expect}'. "
				"Must be one of 'one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'."
			)
		
		# ------------------------------------------------------------------
		# 1. Validate and extract join keys
		# ------------------------------------------------------------------
		pairs = self._validate_join_keys(other, left_on, right_on)
		left_keys = [lk for lk, _ in pairs]
		right_keys = [rk for _, rk in pairs]
		
		# Determine if we need to validate hashability (only for object dtype columns)
		validate_hashable = any(
			(col.schema() is None or col.schema().kind is object)
			for col in (left_keys + right_keys)
		)
		
		# Pre-bind lengths and columns
		left_nrows = len(self)
		right_nrows = len(other)
		left_cols = self._underlying
		right_cols = other._underlying
		n_left_cols = len(left_cols)
		n_right_cols = len(right_cols)
		
		# ------------------------------------------------------------------
		# 2. Build hash map on right side
		# ------------------------------------------------------------------
		right_index = {}
		right_index_get = right_index.get
		
		check_right_unique = expect in ('one_to_one', 'many_to_one')
		if check_right_unique:
			duplicates = {}
		
		# Build key → [indices]
		for row_idx in range(right_nrows):
			key = tuple(col[row_idx] for col in right_keys)
			
			# Validate hashability for object dtype columns
			if validate_hashable:
				Table._validate_key_tuple_hashable(key, right_keys, row_idx)
			
			bucket = right_index_get(key)
			if bucket is None:
				right_index[key] = [row_idx]
			else:
				bucket.append(row_idx)
				if check_right_unique:
					duplicates[key] = bucket
		
		# Cardinality check on right (one-to-one, many-to-one)
		if check_right_unique and duplicates:
			example_key, example_indices = next(iter(duplicates.items()))
			raise SerifValueError(
				f"Join expectation '{expect}' violated: Right side has duplicate keys.\n"
				f"Example: {example_key} appears {len(example_indices)} times."
			)
		
		# ------------------------------------------------------------------
		# 3. Left-side uniqueness enforcement
		# ------------------------------------------------------------------
		check_left_unique = expect in ('one_to_one', 'one_to_many')
		if check_left_unique:
			left_keys_seen = set()
		
		# ------------------------------------------------------------------
		# 4. Build RESULT in column-major order
		# ------------------------------------------------------------------
		result_data = [[] for _ in range(n_left_cols + n_right_cols)]
		append_cols = [col.append for col in result_data]
		
		# Perform join
		for left_idx in range(left_nrows):
			key = tuple(col[left_idx] for col in left_keys)
			
			# Validate hashability for object dtype columns
			if validate_hashable:
				Table._validate_key_tuple_hashable(key, left_keys, left_idx)
			
			# Enforce left-side cardinality (if needed)
			if check_left_unique:
				if key in left_keys_seen:
					raise SerifValueError(
						f"Join expectation '{expect}' violated: Left side has duplicate key {key}"
					)
				left_keys_seen.add(key)
			
			matches = right_index_get(key)
			if not matches:
				continue  # INNER JOIN → skip non-matches
			
			# Emit each match
			for right_idx in matches:
				# Left columns
				for c_idx, col in enumerate(left_cols):
					append_cols[c_idx](col[left_idx])
				
				# Right columns
				base = n_left_cols
				for offset, col in enumerate(right_cols):
					append_cols[base + offset](col[right_idx])
		
		# Handle empty result
		if all(len(col) == 0 for col in result_data):
			return Table(())
		
		# ------------------------------------------------------------------
		# 5. Wrap result_data in Vectors
		# ------------------------------------------------------------------
		result_cols = []
		
		# Left columns (preserve name)
		for col_idx, orig_col in enumerate(left_cols):
			result_cols.append(Vector(result_data[col_idx], name=orig_col._name))
		
		# Right columns (preserve name)
		base = n_left_cols
		for offset, orig_col in enumerate(right_cols):
			result_cols.append(Vector(result_data[base + offset], name=orig_col._name))
		
		return Table(result_cols)

	def join(self, other, left_on, right_on, expect='many_to_one'):
		"""
		Left join two Tables on specified key columns.
		Returns all rows from left table, with matching rows from right (or None for no match).
		
		Args:
			other: Table to join with
			left_on: Column name(s) or Vector(s) from left table
			right_on: Column name(s) or Vector(s) from right table
			expect: Cardinality expectation - 'one_to_one', 'many_to_one',
					'one_to_many', or 'many_to_many'
		
		Returns:
			Table with joined results
		"""
		# Validate expectation value early
		if expect not in ('one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'):
			raise SerifValueError(
				f"Invalid expect value '{expect}'. "
				"Must be one of 'one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'."
			)
		
		# Validate and normalize join keys
		pairs = self._validate_join_keys(other, left_on, right_on)
		
		# Extract join key columns (Vectors)
		left_keys = [lk for lk, _ in pairs]
		right_keys = [rk for _, rk in pairs]
		
		# Check if any key columns have object dtype (need runtime validation)
		validate_hashable = any(
			(col.schema() is None or col.schema().kind is object)
			for col in (left_keys + right_keys)
		)
		
		left_nrows = len(self)
		right_nrows = len(other)
		left_cols = self._underlying
		right_cols = other._underlying
		n_left_cols = len(left_cols)
		n_right_cols = len(right_cols)
		
		# Build hash map on right: key_tuple -> list of row indices
		right_index = {}
		check_right_unique = expect in ('one_to_one', 'many_to_one')
		if check_right_unique:
			duplicates = {}
		
		for row_idx in range(right_nrows):
			key = tuple(col[row_idx] for col in right_keys)
			
			# Validate hashability for object dtype columns
			if validate_hashable:
				self._validate_key_tuple_hashable(key, right_keys, row_idx)
			
			bucket = right_index.get(key)
			if bucket is None:
				right_index[key] = [row_idx]
			else:
				bucket.append(row_idx)
				if check_right_unique and key not in duplicates:
					duplicates[key] = bucket
		
		# Enforce right-side uniqueness if required
		if check_right_unique and duplicates:
			example_key, example_indices = next(iter(duplicates.items()))
			raise SerifValueError(
				f"Join expectation '{expect}' violated: Right side has duplicate keys.\n"
				f"Found at least {len(duplicates)} duplicate key(s), e.g., {example_key} "
				f"appears {len(example_indices)} times."
			)
		
		# Prepare left-side uniqueness tracking if needed
		check_left_unique = expect in ('one_to_one', 'many_to_one')
		if check_left_unique:
			left_keys_seen = set()
		
		# Perform left join, building result in COLUMN-MAJOR form
		total_cols = n_left_cols + n_right_cols
		result_data = [[] for _ in range(total_cols)]
		
		# Local binds for speed
		result_append_cols = [col.append for col in result_data]
		right_index_get = right_index.get
		
		for left_idx in range(left_nrows):
			key = tuple(col[left_idx] for col in left_keys)
			
			# Validate hashability for object dtype columns
			if validate_hashable:
				self._validate_key_tuple_hashable(key, left_keys, left_idx)
			
			# Enforce left-side uniqueness if needed
			if check_left_unique:
				if key in left_keys_seen:
					raise SerifValueError(
						f"Join expectation '{expect}' violated: Left side has duplicate key {key}"
					)
				left_keys_seen.add(key)
			
			matches = right_index_get(key)
			
			if matches:
				# For each matching right row, append combined row
				for right_idx in matches:
					# Append left columns
					for c_idx, col in enumerate(left_cols):
						result_append_cols[c_idx](col[left_idx])
					
					# Append right columns
					base = n_left_cols
					for offset, col in enumerate(right_cols):
						result_append_cols[base + offset](col[right_idx])
			else:
				# No match: left row with None for all right columns
				for c_idx, col in enumerate(left_cols):
					result_append_cols[c_idx](col[left_idx])
				
				base = n_left_cols
				for offset in range(n_right_cols):
					result_append_cols[base + offset](None)
		
		# Handle completely empty result
		if left_nrows == 0:
			return Table(())
		
		# Wrap result_data into Vectors, preserving column names
		result_cols = []
		
		# Left table columns
		for col_idx, orig_col in enumerate(left_cols):
			col_data = result_data[col_idx]
			result_cols.append(Vector(col_data, name=orig_col._name))
		
		# Right table columns
		for j, orig_col in enumerate(right_cols):
			col_data = result_data[n_left_cols + j]
			result_cols.append(Vector(col_data, name=orig_col._name))
		
		return Table(result_cols)

	def full_join(self, other, left_on, right_on, expect='many_to_many'):
		"""
		Full outer join of two Tables. Includes:
			- All rows from left table
			- All rows from right table
			- Matching rows combined
			- None where no match exists
		
		Args:
			other: Table to join with
			left_on: Column name(s) or Vector(s) from left table
			right_on: Column name(s) or Vector(s) from right table
			expect: Cardinality expectation - 'one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'
		
		Returns:
			Table with joined results
		"""
		# Validate expectation string
		if expect not in ('one_to_one', 'many_to_one', 'one_to_many', 'many_to_many'):
			raise SerifValueError(
				f"Invalid expect='{expect}'. "
				"Must be 'one_to_one', 'many_to_one', 'one_to_many', or 'many_to_many'."
			)
		
		# ------------------------------------------------------------------
		# 1. Validate join keys and extract columns
		# ------------------------------------------------------------------
		pairs = self._validate_join_keys(other, left_on, right_on)
		left_keys = [lk for lk, _ in pairs]
		right_keys = [rk for _, rk in pairs]
		
		# Determine if we need to validate hashability (only for object dtype columns)
		validate_hashable = any(
			(col.schema() is None or col.schema().kind is object)
			for col in (left_keys + right_keys)
		)
		
		left_nrows = len(self)
		right_nrows = len(other)
		
		left_cols = self._underlying
		right_cols = other._underlying
		n_left_cols = len(left_cols)
		n_right_cols = len(right_cols)
		
		# ------------------------------------------------------------------
		# 2. Build hash index for right table
		# ------------------------------------------------------------------
		right_index = {}
		right_index_get = right_index.get
		
		check_right_unique = expect in ('one_to_one', 'many_to_one')
		if check_right_unique:
			duplicates = {}
		
		for right_idx in range(right_nrows):
			key = tuple(col[right_idx] for col in right_keys)
			
			# Validate hashability for object dtype columns
			if validate_hashable:
				Table._validate_key_tuple_hashable(key, right_keys, right_idx)
			
			bucket = right_index_get(key)
			if bucket is None:
				right_index[key] = [right_idx]
			else:
				bucket.append(right_idx)
				if check_right_unique:
					duplicates[key] = bucket
		
		# Enforce right-side cardinality if necessary
		if check_right_unique and duplicates:
			example_key, example_inds = next(iter(duplicates.items()))
			raise SerifValueError(
				f"Join expectation '{expect}' violated: Right side has duplicate keys.\n"
				f"Example: {example_key} appears {len(example_inds)} times."
			)
		
		# ------------------------------------------------------------------
		# 3. Prepare left-side cardinality tracking
		# ------------------------------------------------------------------
		check_left_unique = expect in ('one_to_one', 'one_to_many')
		if check_left_unique:
			left_keys_seen = set()
		
		# Track which right rows are matched
		matched_right_rows = set()
		matched_right_add = matched_right_rows.add
		
		# ------------------------------------------------------------------
		# 4. Build RESULT in column-major form
		# ------------------------------------------------------------------
		total_cols = n_left_cols + n_right_cols
		result_data = [[] for _ in range(total_cols)]
		append_cols = [col.append for col in result_data]
		
		# ------------------------------------------------------------------
		# 5. Process LEFT table (major phase of full join)
		# ------------------------------------------------------------------
		for left_idx in range(left_nrows):
			key = tuple(col[left_idx] for col in left_keys)
			
			# Validate hashability for object dtype columns
			if validate_hashable:
				Table._validate_key_tuple_hashable(key, left_keys, left_idx)
			
			# Enforce left-side cardinality
			if check_left_unique:
				if key in left_keys_seen:
					raise SerifValueError(
						f"Join expectation '{expect}' violated: Left side has duplicate key {key}"
					)
				left_keys_seen.add(key)
			
			matches = right_index_get(key)
			if matches:
				# Emit matched combinations
				for right_idx in matches:
					matched_right_add(right_idx)
					
					# Left
					for c_idx, col in enumerate(left_cols):
						append_cols[c_idx](col[left_idx])
					
					# Right
					base = n_left_cols
					for offset, col in enumerate(right_cols):
						append_cols[base + offset](col[right_idx])
			else:
				# No match → left row + None right
				for c_idx, col in enumerate(left_cols):
					append_cols[c_idx](col[left_idx])
				
				base = n_left_cols
				for offset in range(n_right_cols):
					append_cols[base + offset](None)
		
		# ------------------------------------------------------------------
		# 6. Add unmatched RIGHT rows
		# ------------------------------------------------------------------
		for right_idx in range(right_nrows):
			if right_idx not in matched_right_rows:
				# Left side: all None
				for c_idx in range(n_left_cols):
					append_cols[c_idx](None)
				
				# Right side: real values
				base = n_left_cols
				for offset, col in enumerate(right_cols):
					append_cols[base + offset](col[right_idx])
		
		# ------------------------------------------------------------------
		# 7. If empty, return empty table
		# ------------------------------------------------------------------
		if left_nrows == 0 and right_nrows == 0:
			return Table(())
		
		# ------------------------------------------------------------------
		# 8. Wrap into Vectors with names preserved
		# ------------------------------------------------------------------
		result_cols = []
		
		# Left columns
		for col_idx, orig_col in enumerate(left_cols):
			result_cols.append(Vector(result_data[col_idx], name=orig_col._name))
		
		# Right columns
		base = n_left_cols
		for offset, orig_col in enumerate(right_cols):
			result_cols.append(Vector(result_data[base + offset], name=orig_col._name))
		
		return Table(result_cols)
	
	def aggregate(
		self,
		# --- Partition keys ---
		over,
		
		# --- Built-in aggregations ---
		sum_over=None,
		mean_over=None,
		min_over=None,
		max_over=None,
		stdev_over=None,
		count_over=None,
		
		# --- Escape hatch ---
		apply=None,
	):
		"""
		Group rows by partition keys and compute aggregations.
		
		Args:
			over: Vector(s) to partition/group by
			sum_over: Vector(s) to sum within each group
			mean_over: Vector(s) to average within each group
			min_over: Vector(s) to find minimum within each group
			max_over: Vector(s) to find maximum within each group
			stdev_over: Vector(s) to compute standard deviation within each group
			count_over: Vector(s) to count non-None values within each group
			apply: Dict of {name: (column, function)} for custom aggregations
		
		Returns:
			Table with one row per unique partition key combination
		
		Examples:
			# Group by customer_id, sum orders
			table.aggregate(over=table.customer_id, sum_over=table.order_total)
			
			# Multiple partition keys and aggregations
			table.aggregate(
				over=[table.year, table.month],
				sum_over=table.revenue,
				mean_over=table.score,
				count_over=table.transaction_id
			)
		"""
		# ------------------------------------------------------------------
		# 1. Normalize inputs
		# ------------------------------------------------------------------
		if isinstance(over, (str, Vector)):
			over = [over]
		
		# Resolve string column names to Vectors
		over = [self._resolve_column(col) for col in over]
		
		# Normalize aggregation lists and resolve column names
		def normalize(v):
			if v is None:
				return None
			if isinstance(v, (str, Vector)):
				return [self._resolve_column(v)]
			# It's a list/tuple
			return [self._resolve_column(col) for col in v]
		
		sum_over   = normalize(sum_over)
		mean_over  = normalize(mean_over)
		min_over   = normalize(min_over)
		max_over   = normalize(max_over)
		stdev_over = normalize(stdev_over)
		count_over = normalize(count_over)
		
		# ------------------------------------------------------------------
		# 2. Validate partition key lengths
		# ------------------------------------------------------------------
		nrows = len(self)
		for i, col in enumerate(over):
			if len(col) != nrows:
				raise SerifValueError(
					f"Partition key at index {i} has length {len(col)}, "
					f"but table has {nrows} rows."
				)
		
		# ------------------------------------------------------------------
		# 3. Build partition index: key_tuple -> list of row indices
		# ------------------------------------------------------------------
		partition_index = {}
		pk_len = len(over)
		
		# Prebind key-cols for speed
		over_data = [c._underlying for c in over]
		
		for row_idx in range(nrows):
			key = tuple(over_data[i][row_idx] for i in range(pk_len))
			# Small fast-path
			bucket = partition_index.get(key)
			if bucket is None:
				partition_index[key] = [row_idx]
			else:
				bucket.append(row_idx)
		
		# Prebind items iteration
		group_items = list(partition_index.items())
		
		# ------------------------------------------------------------------
		# 4. Name sanitization helpers
		# ------------------------------------------------------------------
		def make_agg_name(col, suffix):
			base = col._name or "col"
			s = _sanitize_user_name(base)
			if s is None:
				s = "col"
			return f"{s}_{suffix}"
		
		used_names = set()
		
		def uniquify(name):
			if name not in used_names:
				used_names.add(name)
				return name
			i = 2
			while f"{name}{i}" in used_names:
				i += 1
			new = f"{name}{i}"
			used_names.add(new)
			return new
		
		# ------------------------------------------------------------------
		# 5. Build result columns array
		# ------------------------------------------------------------------
		result_cols = []
		
		# --- Partition key columns (one per unique group) ---
		# Pre-bind: this is fast because group_items holds (key, rows)
		for idx, col in enumerate(over):
			values = [key[idx] for key, _ in group_items]
			result_cols.append(Vector(values, name=uniquify(col._name or "key")))
		
		# ------------------------------------------------------------------
		# 6. Column-major helper: aggregate one column for all groups
		# ------------------------------------------------------------------
		def aggregate_col(col, func, suffix):
			data = col._underlying
			out = []
			
			for key, row_indices in group_items:
				# column-major group extraction
				vals = [data[i] for i in row_indices]
				
				res = func(vals)
				out.append(res)
			
			name = uniquify(make_agg_name(col, suffix))
			result_cols.append(Vector(out, name=name))
		
		# ------------------------------------------------------------------
		# 7. Built-in aggregations (fast, no repeated scans)
		# ------------------------------------------------------------------
		
		# SUM
		if sum_over:
			for col in sum_over:
				if len(col) != nrows:
					raise SerifValueError(f"Aggregation column has wrong length")
				d = col._underlying
				aggregate_col(
					col,
					lambda vals, d=d: sum(v for v in vals if v is not None),
					"sum"
				)
		
		# MEAN
		if mean_over:
			for col in mean_over:
				if len(col) != nrows:
					raise SerifValueError(f"Aggregation column has wrong length")
				d = col._underlying
				def mean_func(vals, d=d):
					clean = [v for v in vals if v is not None]
					return sum(clean) / len(clean) if clean else None
				
				aggregate_col(col, mean_func, "mean")
		
		# MIN
		if min_over:
			for col in min_over:
				if len(col) != nrows:
					raise SerifValueError(f"Aggregation column has wrong length")
				d = col._underlying
				def min_func(vals, d=d):
					clean = [v for v in vals if v is not None]
					return min(clean) if clean else None
				
				aggregate_col(col, min_func, "min")
		
		# MAX
		if max_over:
			for col in max_over:
				if len(col) != nrows:
					raise SerifValueError(f"Aggregation column has wrong length")
				d = col._underlying
				def max_func(vals, d=d):
					clean = [v for v in vals if v is not None]
					return max(clean) if clean else None
				
				aggregate_col(col, max_func, "max")
		
		# COUNT
		if count_over:
			for col in count_over:
				if len(col) != nrows:
					raise SerifValueError(f"Aggregation column has wrong length")
				d = col._underlying
				aggregate_col(
					col,
					lambda vals, d=d: sum(1 for v in vals if v is not None),
					"count"
				)
		
		# STDEV (sample standard deviation)
		if stdev_over:
			for col in stdev_over:
				if len(col) != nrows:
					raise SerifValueError(f"Aggregation column has wrong length")
				d = col._underlying
				
				def stdev_func(vals, d=d):
					clean = [v for v in vals if v is not None]
					n = len(clean)
					if n <= 1:
						return None
					mean_val = sum(clean) / n
					variance = sum((v - mean_val) ** 2 for v in clean) / (n - 1)
					return variance ** 0.5
				
				aggregate_col(col, stdev_func, "stdev")
		
		# ------------------------------------------------------------------
		# 8. Custom apply aggregations
		# ------------------------------------------------------------------
		if apply is not None:
			for agg_name, (col, func) in apply.items():
				resolved_col = self._resolve_column(col)
				if len(resolved_col) != nrows:
					raise SerifValueError(f"Custom aggregation column '{agg_name}' has wrong length")
				d = resolved_col._underlying
				out = []
				for key, row_indices in group_items:
					vals = [d[i] for i in row_indices]
					out.append(func(vals))
				
				result_cols.append(Vector(out, name=uniquify(agg_name)))
		
		# ------------------------------------------------------------------
		# 9. Final table
		# ------------------------------------------------------------------
		return Table(result_cols)

	def window(
		self,
		# --- Partition keys ---
		over,
		
		# --- Built-in aggregations ---
		sum_over=None,
		mean_over=None,
		min_over=None,
		max_over=None,
		stdev_over=None,
		count_over=None,
		
		# --- Custom aggregations ---
		apply=None,
	):
		"""
		Compute window functions over partitions, returning the same number of rows.
		
		Similar to aggregate(), but repeats the aggregated value for each row in the group.
		
		Args:
			over: Vector(s) to partition/group by
			sum_over: Vector(s) to sum within each group
			mean_over: Vector(s) to average within each group
			min_over: Vector(s) to find minimum within each group
			max_over: Vector(s) to find maximum within each group
			stdev_over: Vector(s) to compute standard deviation within each group
			count_over: Vector(s) to count non-None values within each group
			apply: Dict of {name: (column, function)} for custom aggregations
		
		Returns:
			Table with same number of rows as input, with aggregated values repeated
		
		Examples:
			# Add running total per customer
			table.window(over=table.customer_id, sum_over=table.order_total)
			
			# Multiple window functions
			table.window(
				over=[table.year, table.month],
				sum_over=table.revenue,
				count_over=table.transaction_id
			)
		"""
		# ----------------------------------------------------------------------
		# 1. Normalize inputs
		# ----------------------------------------------------------------------
		if isinstance(over, (str, Vector)):
			over = [over]
		
		# Resolve string column names to Vectors
		over = [self._resolve_column(col) for col in over]
		
		# Normalize aggregation lists and resolve column names
		def norm(v):
			if v is None:
				return None
			if isinstance(v, (str, Vector)):
				return [self._resolve_column(v)]
			# It's a list/tuple
			return [self._resolve_column(col) for col in v]
		
		sum_over   = norm(sum_over)
		mean_over  = norm(mean_over)
		min_over   = norm(min_over)
		max_over   = norm(max_over)
		stdev_over = norm(stdev_over)
		count_over = norm(count_over)
		
		# ----------------------------------------------------------------------
		# 2. Validate column lengths
		# ----------------------------------------------------------------------
		nrows = len(self)
		for i, col in enumerate(over):
			if len(col) != nrows:
				raise ValueError(
					f"Partition key at index {i} has length {len(col)}, "
					f"but table has {nrows} rows."
				)
		
		# ----------------------------------------------------------------------
		# 3. Build partition index: key -> list[row indices]
		# ----------------------------------------------------------------------
		partition_index = {}
		pk_len = len(over)
		over_data = [c._underlying for c in over]
		
		# Build once; reused everywhere
		row_keys = [None] * nrows
		
		for i in range(nrows):
			key = tuple(over_data[k][i] for k in range(pk_len))
			row_keys[i] = key
			bucket = partition_index.get(key)
			if bucket is None:
				partition_index[key] = [i]
			else:
				bucket.append(i)
		
		group_items = list(partition_index.items())
		
		# ----------------------------------------------------------------------
		# 4. Name logic
		# ----------------------------------------------------------------------
		used = set()
		
		def sanitize(col, suffix):
			base = col._name or "col"
			s = _sanitize_user_name(base) or "col"
			return f"{s}_{suffix}"
		
		def uniquify(name):
			if name not in used:
				used.add(name)
				return name
			i = 2
			while f"{name}{i}" in used:
				i += 1
			final = f"{name}{i}"
			used.add(final)
			return final
		
		# ----------------------------------------------------------------------
		# 5. Start with partition key columns (copy directly)
		# ----------------------------------------------------------------------
		result_cols = []
		for col in over:
			result_cols.append(
				Vector(list(col), name=uniquify(col._name or "key"))
			)
		
		# ----------------------------------------------------------------------
		# 6. Helper: compute group-level aggregation for one column
		# ----------------------------------------------------------------------
		def compute_group_values(col, fn):
			data = col._underlying
			out = {}
			for key, rows in group_items:
				vals = [data[i] for i in rows]
				out[key] = fn(vals)
			return out
		
		# ----------------------------------------------------------------------
		# 7. Helper: expand group-level values back to all rows
		# ----------------------------------------------------------------------
		def expand_to_rows(group_map):
			return [group_map[row_keys[i]] for i in range(nrows)]
		
		# ----------------------------------------------------------------------
		# 8. Built-in aggregations
		# ----------------------------------------------------------------------
		# Each aggregator: compute group-level -> expand -> append column
		
		# SUM
		if sum_over:
			for col in sum_over:
				if len(col) != nrows:
					raise ValueError(f"Aggregation column has wrong length")
				def fn(vals):
					return sum(v for v in vals if v is not None)
				gm = compute_group_values(col, fn)
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(sanitize(col, "sum")))
				)
		
		# MEAN
		if mean_over:
			for col in mean_over:
				if len(col) != nrows:
					raise ValueError(f"Aggregation column has wrong length")
				def fn(vals):
					clean = [v for v in vals if v is not None]
					return sum(clean) / len(clean) if clean else None
				gm = compute_group_values(col, fn)
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(sanitize(col, "mean")))
				)
		
		# MIN
		if min_over:
			for col in min_over:
				if len(col) != nrows:
					raise ValueError(f"Aggregation column has wrong length")
				def fn(vals):
					clean = [v for v in vals if v is not None]
					return min(clean) if clean else None
				gm = compute_group_values(col, fn)
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(sanitize(col, "min")))
				)
		
		# MAX
		if max_over:
			for col in max_over:
				if len(col) != nrows:
					raise ValueError(f"Aggregation column has wrong length")
				def fn(vals):
					clean = [v for v in vals if v is not None]
					return max(clean) if clean else None
				gm = compute_group_values(col, fn)
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(sanitize(col, "max")))
				)
		
		# COUNT
		if count_over:
			for col in count_over:
				if len(col) != nrows:
					raise ValueError(f"Aggregation column has wrong length")
				def fn(vals):
					return sum(1 for v in vals if v is not None)
				gm = compute_group_values(col, fn)
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(sanitize(col, "count")))
				)
		
		# STDEV
		if stdev_over:
			for col in stdev_over:
				if len(col) != nrows:
					raise ValueError(f"Aggregation column has wrong length")
				def fn(vals):
					clean = [v for v in vals if v is not None]
					n = len(clean)
					if n <= 1:
						return None
					mean_val = sum(clean) / n
					return (sum((v - mean_val)**2 for v in clean) / (n - 1)) ** 0.5
				
				gm = compute_group_values(col, fn)
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(sanitize(col, "stdev")))
				)
		
		# ----------------------------------------------------------------------
		# 9. Custom aggregation(s)
		# ----------------------------------------------------------------------
		if apply:
			for name, (col, fn) in apply.items():
				resolved_col = self._resolve_column(col)
				if len(resolved_col) != nrows:
					raise ValueError(f"Custom aggregation column '{name}' has wrong length")
				data = resolved_col._underlying
				gm = {
					key: fn([data[i] for i in rows])
					for key, rows in group_items
				}
				result_cols.append(
					Vector(expand_to_rows(gm), name=uniquify(name))
				)
		
		# ----------------------------------------------------------------------
		# 10. Final table
		# ----------------------------------------------------------------------
		return Table(result_cols)

	def sort_by(self, by, reverse=False, na_last=True):
		"""
		Return a new Table sorted by one or more keys.

		Parameters
		----------
		by : Vector | str | sequence[Vector | str]
			Sort key(s). Each key may be:
			- a Vector (typically a column from this table), or
			- a column name (string), resolved via self[<name>].
		reverse : bool | sequence[bool], default False
			Sort order for each key:
			- bool: same order for all keys
			- sequence[bool]: per-key reverse flag, must match length of `by`.
		na_last : bool, default True
			If True, None sorts after all valid values.
			If False, None sorts before all valid values.

		Notes
		-----
		- Sorting is stable.
		- The table is not modified in place; a new Table is returned.
		
		Examples
		--------
		>>> t.sort_by(t.name)  # ascending
		>>> t.sort_by(t.name, reverse=True)  # descending
		>>> t.sort_by([t.name, t.age], reverse=[False, True])  # mixed
		>>> t.sort_by((t.name, t.age), reverse=True)  # both descending
		>>> t.sort_by(t.score, na_last=False)  # None values first
		"""
		# --- 1. Normalize `by` into a list of specs ---
		if isinstance(by, (str, Vector)):
			keys = [by]
		elif isinstance(by, (list, tuple)):
			if not by:
				raise SerifValueError("sort_by() requires at least one sort key")
			keys = list(by)
		else:
			raise SerifTypeError(
				f"sort_by() expects a Vector, column name, or sequence of these; "
				f"got {type(by).__name__}"
			)

		# --- 2. Normalize `reverse` to list[bool] ---
		if isinstance(reverse, bool):
			rev_flags = [reverse] * len(keys)
		elif isinstance(reverse, (list, tuple)):
			if len(reverse) != len(keys):
				raise SerifValueError(
					f"reverse has length {len(reverse)}, but sort keys have length {len(keys)}"
				)
			rev_flags = [bool(x) for x in reverse]
		else:
			raise SerifTypeError(
				f"reverse must be bool or sequence[bool], got {type(reverse).__name__}"
			)

		# --- 3. Resolve all keys to Vector columns from this table ---
		resolved = []
		nrows = len(self)

		for spec in keys:
			col = self._resolve_column(spec)
			if len(col) != nrows:
				raise SerifValueError(
					f"Sort key has length {len(col)}, but table has {nrows} rows"
				)
			resolved.append(col)

		# --- 4. Edge case: empty table ---
		if nrows == 0:
			# Preserve columns / names but with no rows
			new_cols = [Vector([], name=col._name) for col in self._underlying]
			return Table(new_cols)

		# --- 5. Build sorted row index using stable multi-key sort ---
		indices = list(range(nrows))

		# Stable sort: apply keys from last to first
		for col, rev in reversed(list(zip(resolved, rev_flags))):
			data = col._underlying

			def key_fn(i, data=data, rev=rev, na_last=na_last):
				v = data[i]
				is_none = (v is None)

				if na_last:
					# Nones should be last for BOTH rev=False and rev=True
					# rev=False -> flag = True for None, False for non-None
					# rev=True  -> flip so None is still "worse" after reversal
					flag = is_none if not rev else (not is_none)
				else:
					# Nones should be first for BOTH rev=False and rev=True
					# rev=False -> flag = False for None, True for non-None
					# rev=True  -> flip so None is still "better" after reversal
					flag = (not is_none) if not rev else is_none

				# Compare on (flag, value). Bool is orderable; `v` is only compared
				# among non-None values, which is what you require for the column.
				return (flag, v)

			indices.sort(key=key_fn, reverse=rev)

		# --- 6. Rebuild columns in sorted order ---
		new_cols = []
		for col in self._underlying:
			src = col._underlying
			new_data = [src[i] for i in indices]
			new_cols.append(Vector(new_data, name=col._name))

		return Table(new_cols)

	def peek(self, sample=1000, top_k=3):
		"""
		Summarize columns: one row per column, with dtype, null %, and top values.

		Parameters
		----------
		sample : int | float | None, default 1000
			Row sampling strategy for computing stats:
			  - None       : scan all rows
			  - int >= 1   : use up to this many rows (evenly spaced)
			  - 0 < float <= 1 : use this fraction of total rows (evenly spaced)
		top_k : int, default 3
			Maximum number of distinct values to show (creates top_1, top_2, top_3 columns).

		Returns
		-------
		Table
			A narrow Table with one row per column. Repr is configured to show
			up to 200 rows (no truncation for typical column counts).
		"""
		nrows = len(self)
		ncols = len(self._underlying)

		# --- Edge case: empty table ---
		if ncols == 0:
			summary = Table({})
			summary._repr_rows = 200
			return summary

		# --- 1. Decide which row indices to sample ---
		if nrows == 0:
			indices = []
		else:
			if sample is None:
				# Full scan
				indices = list(range(nrows))
			elif isinstance(sample, float):
				if not (0.0 < sample <= 1.0):
					raise SerifValueError(
						"peek(sample=float) requires 0.0 < sample <= 1.0"
					)
				target = max(1, int(nrows * sample))
				if target >= nrows:
					indices = list(range(nrows))
				else:
					step = max(1, nrows // target)
					indices = list(range(0, nrows, step))[:target]
			elif isinstance(sample, int):
				if sample <= 0:
					indices = []
				elif sample >= nrows:
					indices = list(range(nrows))
				else:
					step = max(1, nrows // sample)
					indices = list(range(0, nrows, step))[:sample]
			else:
				raise SerifTypeError(
					f"peek(sample=...) must be int, float, None; got {type(sample).__name__}"
				)

		# --- 2. Prepare metadata columns ---
		names       = []
		attr_names  = []
		dtypes      = []
		nullables   = []
		null_pcts   = []
		# Create top_k columns dynamically
		top_columns = [[] for _ in range(top_k)]

		max_value_str_len = 40  # mild truncation for display only

		for idx, col in enumerate(self._underlying):
			data = col._underlying
			col_name = col._name

			# Original name
			names.append(col_name)

			# Sanitized attribute name with dot prefix
			if col_name is not None:
				base = _sanitize_user_name(col_name)
				if base is None:
					attr = f".col{idx}_"
				else:
					attr = f".{base}"
			else:
				attr = f".col{idx}_"
			attr_names.append(attr)

			# Dtype and nullable from schema(), if available
			schema = None
			if hasattr(col, "schema"):
				try:
					schema = col.schema()
				except Exception:
					schema = None

			if schema is not None and getattr(schema, "kind", None) is not None:
				kind = schema.kind
				dtype_str = getattr(kind, "__name__", str(kind))
				nullable = bool(getattr(schema, "nullable", True))
			else:
				dtype_str = "unknown"
				nullable = True  # conservative default

			dtypes.append(dtype_str)
			nullables.append(nullable)

			# --- 3. Scan sampled values for nulls & value frequencies ---
			null_count = 0
			value_counts = {}

			for i in indices:
				v = data[i]
				if v is None:
					null_count += 1
				else:
					value_counts[v] = value_counts.get(v, 0) + 1

			sampled_total = len(indices)
			if sampled_total == 0:
				null_pct = 0.0
			else:
				null_pct = (null_count / sampled_total) * 100.0

			# Keep as float; repr will show with normal float formatting
			null_pcts.append(round(null_pct, 1))

			# --- 4. Top-k values, sorted descending by frequency ---
			# Sort by count descending, then by value str for stability
			items = sorted(
				value_counts.items(),
				key=lambda kv: (-kv[1], str(kv[0])),
			)[:top_k]

			non_null_sample = sampled_total - null_count

			# Fill each top_k column
			for k in range(top_k):
				if k < len(items) and non_null_sample > 0:
					val, count = items[k]
					pct = (count / non_null_sample) * 100.0
					val_str = str(val)
					if len(val_str) > max_value_str_len:
						val_str = val_str[: max_value_str_len - 1] + "…"
					top_columns[k].append(f"{val_str} ({pct:.1f}%)")
				else:
					top_columns[k].append("")

		# --- 5. Build summary table (one row per column) ---
		table_dict = {
			"name":       names,
			"attr":       attr_names,
			"dtype":      dtypes,
			"nullable":   nullables,
			"null_pct":   null_pcts,
		}
		
		# Add top_k columns dynamically
		for k in range(top_k):
			table_dict[f"top_{k+1}"] = top_columns[k]
		
		summary = Table(table_dict)

		# Ensure peek output does not get truncated in repr
		summary._repr_rows = 200
		return summary
