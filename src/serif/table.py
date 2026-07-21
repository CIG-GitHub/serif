from .vector import Vector
from ._table import columns as _columns
from ._table import lifting as _lifting
from ._table import mutation as _mutation
from ._table import rows as _rows
from ._table import selection as _selection
from ._table import sort as _sort
from ._table.row import Row
from ._table.row import iter_rows as _iter_rows
from ._vector import Schema
from ._accel.api import _accel_take
from ._accel.api import _accel_take_pad
from ._accel.api import _accel_popcount
from ._accel.api import _accel_group
from ._accel.api import _accel_join_probe

from ._vector.storage import TupleStorage

from .errors import SerifKeyError
from .errors import SerifValueError
from .errors import SerifTypeError
from .errors import SerifEmptyReductionError


class Table(Vector):
    """ Multiple columns of the same length """
    _length = None
    _repr_rows = None  # Optional table-specific repr row count override
    _ndims = 2
    _unlocked = False  # True only inside a batch() scope
    
    def __new__(cls, initial=(), dtype=None, name=None):
        return super(Vector, cls).__new__(cls)

    def __init__(self, initial=(), dtype=None, name=None):
        # Handle dict initialization {name: values, ...}
        if isinstance(initial, dict):
            # An existing 1-D vector snapshots via copy() — O(1) storage
            # share with the dict key as its name, schema and backend
            # preserved (a _Category stays categorical), matching how the
            # list path treats vectors. Re-wrapping through Vector(...)
            # would re-walk and re-infer the whole column. Everything else
            # builds through the constructor as before.
            initial = [
                values.copy(name=col_name)
                if isinstance(values, Vector) and values.ndims() == 1
                else Vector(values, name=col_name)
                for col_name, values in initial.items()
            ]

        # Handle list-of-lists (or list-of-iterables that aren't Vectors/Tables)
        elif (
            isinstance(initial, (list, tuple))
            and initial
            and all(isinstance(row, (list, tuple)) and not isinstance(row, Vector) for row in initial)
        ):
            inner_lengths = [len(row) for row in initial]
            if len(set(inner_lengths)) == 1:
                # Uniform inner lengths → treat as row-major, transpose to columns
                initial = [Vector(list(col)) for col in zip(*initial)]
            else:
                # Jagged → treat as columns (invariant check will fire if lengths differ)
                initial = [Vector(col) for col in initial]

        # Enforce invariant #2 (docs/invariants.md): all columns equal length.
        # Without this, _length silently truncates to the first column.
        if initial:
            lengths = {len(col) for col in initial}
            if len(lengths) > 1:
                raise SerifValueError(
                    f"Table columns must all have the same length; "
                    f"got lengths {sorted(lengths)}."
                )

        self._length = len(initial[0]) if initial else 0
        
        # Snapshot columns to enforce value semantics. copy() shares the
        # frozen storage (rebuild-on-write makes a share a true snapshot),
        # so this is O(1) per column — no aliasing: a write to either side
        # rebinds that side a new storage. copy() preserves names on every
        # Vector flavor, so no separate name save/restore is needed.
        if initial:
            initial = tuple(vec.copy() for vec in initial)
        else:
            initial = ()
        
        # Set _dtype to None explicitly since Table bypasses Vector.__new__
        self._dtype = None
        self._column_map = None
        # Names already warned about (reserved-method collisions) — warn once
        # per name per table, since _build_column_map reruns on rename.
        self._warned_collisions = set()
        
        # Call parent constructor
        super().__init__(initial, dtype=dtype, name=name)
        
        # Build column map
        self._column_map = self._build_column_map()

    def __len__(self):
        # Nested tables are forbidden (docs/invariants.md #2), so length is
        # always the shared column length.
        if len(self._storage) == 0:
            return 0
        return self._length

    @property
    def shape(self):
        # Always 2-D: (rows, columns). No Row construction needed.
        n_cols = len(self._storage) if hasattr(self, '_storage') else 0
        return (len(self) if n_cols else 0, n_cols)

    @property
    def table_name(self):
        """Get the table's name (None if unnamed). Set via `t.table_name = ...`.

        A Table is structurally a Vector of columns, but its name is a TABLE
        name — so the Vector-level `.vector_name` is de-linked here (it raises)
        and this is the accessor to use.
        """
        return self._name

    @property
    def vector_name(self):
        # De-linked from Vector: a Table's name is a table_name.
        raise AttributeError(
            "Table has no 'vector_name' — use '.table_name'."
        )

    def _build_column_map(self):
        """Build mapping from sanitized column names to column indices.
        
        This is computed once during table initialization and used by
        Row for O(1) attribute lookups during iteration.
        """
        return _columns.build_column_map(self)
    
    def __dir__(self):
        """Return list of available attributes including sanitized column names."""
        return _columns.attribute_names(self)
    
    def cols(self, key=None):
        """Access columns positionally: cols() → tuple of all columns,
        cols(i) → single column, cols(slice) → tuple of columns."""
        return _columns.columns(self, key)

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
        return _columns.column_names(self)

    def _schema_columns(self):
        """Internal metadata-only column path used by ``t._``."""
        return _columns.schema_columns(self)

    def to_dict(self):
        """Serialize table to a column-oriented dict of plain Python lists.

        Intended for transport/export only — not a lossless round-trip.
        Column names fall back to positional keys ('col0_', 'col1_', ...)
        for unnamed columns. Export keys must be unique; a collision raises
        instead of silently dropping an earlier column.

        Returns
        -------
        dict
            {'column_name': [v0, v1, ...], ...}

        Examples
        --------
        >>> t = Table({'x': [1, 2], 'y': [3, 4]})
        >>> t.to_dict()
        {'x': [1, 2], 'y': [3, 4]}
        """
        return _columns.to_dict(self)

    # ------------------------------------------------------------------
    # Vector-surface conformance
    # ------------------------------------------------------------------

    def _map_columns(self, fn):
        """Apply a value-producing Vector operation to every column.

        Table subclasses Vector so the useful element-wise Vector surface is
        available on tables too.  The result must still be a structurally
        complete Table, with the source column names restored explicitly.
        """
        return _lifting.map_columns(self, fn)

    @classmethod
    def filled(cls, value, length, typesafe=False):
        raise SerifTypeError(
            "Table.filled() is ambiguous because a table has columns. "
            "Build named filled columns instead, e.g. "
            "Table({'x': Vector.filled(value, length)})."
        )

    def cast(self, target_type):
        """Cast every column to *target_type*, preserving table structure."""
        return _lifting.cast(self, target_type)

    def to_object(self):
        """Return a table whose columns all use object dtype."""
        return _lifting.to_object(self)

    def fillna(self, value):
        """Fill null cells in every column with *value*."""
        return _lifting.fillna(self, value)

    def is_na(self):
        """Return a same-shaped bool Table marking null cells."""
        return _lifting.is_na(self)

    def is_type(self, types):
        """Return a same-shaped bool Table applying ``isinstance`` per cell."""
        return _lifting.is_type(self, types)

    def dropna(self):
        """Return rows having no null cells (complete-case filtering)."""
        return _rows.dropna(self)

    def unique(self):
        """Return the first occurrence of each distinct row, in source order."""
        return _rows.unique(self)

    def __getattr__(self, attr):
        """Access columns by sanitized attribute name using pre-computed column map."""
        return _columns.get_attribute(
            self,
            attr,
            super().__getattribute__,
        )

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
        return _columns.resolve_column(self, spec)

    def __setattr__(self, attr, value):
        """Intercept column assignments (t.colname = vec) to update underlying columns."""
        return _mutation.setattr(self, attr, value)

    def rename(self, mapping):
        """Return a NEW Table with columns renamed per {old: new}.

        Non-mutating (matches drop / pandas / polars). Keys may be:
          - a column NAME (str): must match exactly ONE column. A name shared
            by several columns is ambiguous and raises — rename it by index.
          - a column INDEX (int): renames the column at that position; the
            unambiguous way to target one of several same-named columns (and
            the only way to reach the first of a duplicated name).

        Renames resolve against the original layout (simultaneous), so
        {'a': 'b', 'b': 'c'} does not cascade. Raises SerifKeyError for a
        missing name, an ambiguous name, or an out-of-range index.
        """
        return _columns.rename(self, mapping)

    def drop(self, *names):
        """Return a NEW Table without the named column(s).

        Non-mutating — the original table is unchanged (like rename). Names
        may be passed as varargs or a single
        list/tuple: `t.drop('a')`, `t.drop('a', 'b')`, `t.drop(['a', 'b'])`.
        Raises SerifKeyError if any name is not a column.
        """
        return _columns.drop(self, *names)

    @property
    def T(self):
        # Transpose 2D table: columns become rows. Tables are always 2-D
        # (docs/invariants.md #2 forbids nesting).
        num_rows = self._length
        num_cols = len(self._storage)
        rows = []
        for row_idx in range(num_rows):
            row = Vector(tuple(col[row_idx] for col in self._storage))
            rows.append(row)
        return Table(rows)

    def __getitem__(self, key):
        return _selection.getitem(self, key)

    def __setitem__(self, key, value):
        """
        Support for 2D assignment:
        1. t[row, col] = scalar
        2. t[row_idx, :] = [values]  (Row assignment)
        3. t[row_slice, col_slice] = other_table (Region assignment)

        Owner-addressed mutation: read through the column, write through
        the table. Outside a batch() scope each write REPLACES the target
        column with a freshly rebuilt one, so everything already read out
        — copies, slices, filtered results, and the column objects
        themselves — keeps its value. Inside a batch() scope, writes land
        in place on the scope's private buffers.
        """
        return _mutation.setitem(self, key, value)

    def _validate_assignment_rows(self, row_spec):
        """Validate every row coordinate before a multi-column write starts."""
        return _mutation.validate_assignment_rows(self, row_spec)

    def _write_column(self, col_idx, row_spec, value):
        """
        Land one column write, owner-addressed.

        Inside a batch() scope: write on the thawed column directly —
        _setitem_impl takes the raw in-place path on the scope's private
        buffers.

        Outside: SWAP-ON-WRITE. The write is applied to a storage-sharing
        clone which then replaces the column in the table, so the original
        column OBJECT is never touched — a previously read-out `v = t.v`
        is a stable snapshot, not a live view of later table writes (the
        same guarantee column replacement via __setattr__ already gives).
        """
        return _mutation.write_column(self, col_idx, row_spec, value)

    def batch(self):
        """
        Bulk-edit scope — the fast path for imperative point-write loops.

        Everyday mutation doesn't need this. Write through the table:

            t[t.v == 'old', 'v'] = 'new'     # conditional update
            t[3, 'v'] = 5                    # one cell
            t.v = t.v.fillna(0)              # column replacement

        Each such statement rebuilds the column once (O(n)) — fine for
        any number of rows, quadratic only if you WRITE IN A LOOP. That
        loop is what batch() is for: entering copies each column's
        buffers once (un-sharing), then every write inside lands raw and
        O(1), and thawed columns accept vector-addressed writes:

            with t.batch() as m:
                for i in hot_indices:
                    m.v[i] = fix(m.v[i])     # read-modify-write, O(1) each

        Observable semantics are identical to table-addressed writes —
        snapshots taken before the scope (copies, slices, filtered
        results, read-out columns) are untouched by construction; only
        the speed differs. Exiting refreezes everything, including column
        refs that escaped the scope. Nesting raises; an exception
        mid-scope leaves the table partially mutated (no rollback).
        """
        return _mutation.batch(self)

    def __iter__(self):
        """
        Iterate over rows using the Fast View.
        Snapshots data state at start of iteration for performance.
        """
        return _iter_rows(self)

    def __repr__(self):
        from .display import _printr
        return _printr(self)

    def _elementwise_compare(self, other, op):
        """
        Table comparisons return a Table of bool columns, same shape as self.

        Table vs Table pairs columns positionally; anything else (scalar,
        Vector, plain iterable) broadcasts to every column — mirroring how
        Table arithmetic broadcasts. Column names are preserved from the
        left side, matching _table_elementwise_operation.
        """
        return _lifting.compare(self, other, op)

    def _lift_comparison_from(self, left, op):
        """Lift ``left op self`` for a nested right-hand operand."""
        return _lifting.compare_from(self, left, op)

    def _lift_operation_from(
        self,
        left,
        op_func,
        op_name,
        op_symbol,
    ):
        """Lift ``left op self`` for a nested right-hand operand."""
        return _lifting.operation_from(
            self,
            left,
            op_func,
            op_name,
            op_symbol,
        )

    def _lift_logical_from(self, left, kleene_func):
        """Lift a logical operation from a scalar Vector into this Table."""
        return _lifting.logical_from(self, left, kleene_func)

    def __rshift__(self, other):
        """ The >> operator behavior has been overridden to add the column(s) of other to self
        """
        return _columns.compose(self, other)


    def __lshift__(self, other):
        """ The << operator behavior has been overridden to attempt to concatenate (append) the new array to the end of the first
        """
        return _rows.concatenate(self, other)

    def _table_elementwise_operation(self, other, op_func, op_name: str, op_symbol: str):
        """
        Handle Table-specific arithmetic with column name preservation.
        
        Rules:
        - Table + scalar: preserve all column names
        - Table + Table: left-biased naming with warnings for mismatches
        """
        return _lifting.binary_operation(
            self,
            other,
            op_func,
            op_name,
            op_symbol,
        )

    def _table_reverse_scalar_operation(self, other, op_func):
        """Apply ``other op column`` while retaining the table schema."""
        return _lifting.reverse_scalar_operation(self, other, op_func)

    def __neg__(self):
        return _lifting.neg(self)

    def __pos__(self):
        return _lifting.pos(self)

    def __abs__(self):
        return _lifting.abs(self)

    def __invert__(self):
        return _lifting.invert(self)

    def _tablewise_bitwise(self, other, op_dunder):
        """Lift a bitwise/logical operation through the Table columns."""
        return _lifting.bitwise(self, other, op_dunder)

    def __and__(self, other):
        return _lifting.bit_and(self, other)

    def __or__(self, other):
        return _lifting.bit_or(self, other)

    def __xor__(self, other):
        return _lifting.bit_xor(self, other)

    def __rand__(self, other):
        return _lifting.rbit_and(self, other)

    def __ror__(self, other):
        return _lifting.rbit_or(self, other)

    def __rxor__(self, other):
        return _lifting.rbit_xor(self, other)
    
    def __add__(self, other):
        return _lifting.add(self, other)
    
    def __sub__(self, other):
        return _lifting.sub(self, other)
    
    def __mul__(self, other):
        return _lifting.mul(self, other)
    
    def __truediv__(self, other):
        return _lifting.truediv(self, other)
    
    def __floordiv__(self, other):
        return _lifting.floordiv(self, other)
    
    def __mod__(self, other):
        return _lifting.mod(self, other)
    
    def __pow__(self, other):
        return _lifting.pow(self, other)

    def __radd__(self, other):
        return _lifting.radd(self, other)

    def __rmul__(self, other):
        return _lifting.rmul(self, other)

    def __rsub__(self, other):
        return _lifting.rsub(self, other)

    def __rtruediv__(self, other):
        return _lifting.rtruediv(self, other)

    def __rfloordiv__(self, other):
        return _lifting.rfloordiv(self, other)

    def __rmod__(self, other):
        return _lifting.rmod(self, other)

    def __rpow__(self, other):
        return _lifting.rpow(self, other)

    def bit_lshift(self, other):
        return _lifting.bit_lshift(self, other)

    def bit_rshift(self, other):
        return _lifting.bit_rshift(self, other)

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
                raise _columns.missing_column_error(
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

    def _join_impl(self, other, left_on, right_on, *,
                   expect_left_unique, expect_right_unique,
                   keep_unmatched_left, keep_unmatched_right):
        """
        Shared core for inner_join / join / full_join.

        The three joins differ only in which unmatched rows they keep:
        inner keeps none, left join keeps unmatched left rows (None-padded
        right side), full join keeps unmatched rows from both sides.
        Everything else -- key validation, hashability checks, cardinality
        enforcement, column-major emission, name-preserving wrap-up -- is
        identical and lives here so it cannot drift between join flavors.
        """
        # ------------------------------------------------------------------
        # 1. Validate and extract join keys
        # ------------------------------------------------------------------
        pairs = self._validate_join_keys(other, left_on, right_on)
        left_keys = [lk for lk, _ in pairs]
        right_keys = [rk for _, rk in pairs]

        # Hashability needs runtime validation only for object/untyped key
        # columns; typed keys are already guaranteed hashable by dtype rules.
        validate_hashable = any(
            (col.schema() is None or col.schema().kind is object)
            for col in (left_keys + right_keys)
        )

        right_nrows = len(other)
        left_cols = self._storage
        right_cols = other._storage

        # Drop a right column only when it IS one of the join key columns
        # (identity, not name) whose left key shares its name. Matching by
        # name alone would also drop an unrelated right column that merely
        # shares the key's name (duplicate names are legal, invariant #6).
        drop_right_idx = {
            idx for idx, col in enumerate(right_cols)
            if any(col is rk and lk._name == rk._name for lk, rk in pairs)
        }

        # ------------------------------------------------------------------
        # 2. Match rows → (left_take, right_take) gather index lists, with
        #    -1 as the pad sentinel. A single int64 key pair probes
        #    entirely in numpy (sort + searchsorted + ragged expand), and
        #    a single string key pair rides arrow codes into the same
        #    probe; cardinality violations come back as tags so the error
        #    is raised HERE with the exact text the pure matcher uses.
        #    Any decline runs the pure matcher: hash index + row loop.
        # ------------------------------------------------------------------
        probed = (_accel_join_probe(
                      left_keys[0]._storage, right_keys[0]._storage,
                      expect_left_unique, expect_right_unique,
                      keep_unmatched_left, keep_unmatched_right)
                  if len(pairs) == 1 else None)
        if probed is None:
            left_take, right_take = self._join_probe_pure(
                left_keys, right_keys, right_nrows,
                validate_hashable=validate_hashable,
                expect_left_unique=expect_left_unique,
                expect_right_unique=expect_right_unique,
                keep_unmatched_left=keep_unmatched_left,
                keep_unmatched_right=keep_unmatched_right,
            )
        elif probed[0] == 'right_dup':
            raise SerifValueError(
                f"expect_right_unique=True violated: right side has duplicate key {probed[1]} "
                f"(appears {probed[2]} times)."
            )
        elif probed[0] == 'left_dup':
            raise SerifValueError(
                f"expect_left_unique=True violated: left side has duplicate key {probed[1]}."
            )
        else:
            left_take, right_take = probed[1], probed[2]

        # ------------------------------------------------------------------
        # 3. Materialize name-preserving output columns: one gather each
        # ------------------------------------------------------------------
        if not len(left_take):
            return Table(())

        # A side only gains injected None rows when the OTHER side keeps its
        # unmatched rows.
        left_nullable_pad = keep_unmatched_right
        right_nullable_pad = keep_unmatched_left

        result_cols = []
        for orig_col in left_cols:
            result_cols.append(Table._gather_join_column(
                orig_col, left_take, left_nullable_pad))
        for offset, orig_col in enumerate(right_cols):
            if offset not in drop_right_idx:
                result_cols.append(Table._gather_join_column(
                    orig_col, right_take, right_nullable_pad))

        return Table(result_cols)

    def _join_probe_pure(self, left_keys, right_keys, right_nrows, *,
                         validate_hashable,
                         expect_left_unique, expect_right_unique,
                         keep_unmatched_left, keep_unmatched_right):
        """
        Pure-python row matcher: hash-index the right side, probe left rows
        in order, return (left_take, right_take) index lists with -1 as the
        pad sentinel. THE specification for the vectorized probe
        (serif/_accel/join.py) — every semantic here (bucket order, raise
        order, error text) must be reproduced exactly there.
        """
        left_nrows = len(self)

        # Materialize key columns once -- per-row storage access would pay
        # unboxing/decoding costs inside the hot loops below. (The right
        # side is walked only when the bucket accelerator declines.)
        left_key_data = [k._storage.to_tuple() for k in left_keys]

        # ------------------------------------------------------------------
        # 2. Build hash index on the right side (+ cardinality check)
        # ------------------------------------------------------------------
        first_duplicate_key = None
        right_index = (_accel_group(right_keys[0]._storage)
                       if len(right_keys) == 1 else None)
        if right_index is not None:
            if expect_right_unique:
                # First duplicate DETECTED in scan order = the bucket whose
                # SECOND row index is smallest — what the pure loop records.
                dups = [(bucket[1], key) for key, bucket in right_index.items()
                        if len(bucket) > 1]
                if dups:
                    first_duplicate_key = min(dups)[1]
        else:
            right_index = {}
            right_index_build_get = right_index.get
            right_key_data = [k._storage.to_tuple() for k in right_keys]
            for row_idx in range(right_nrows):
                key = tuple(kd[row_idx] for kd in right_key_data)
                if validate_hashable:
                    Table._validate_key_tuple_hashable(key, right_keys, row_idx)
                bucket = right_index_build_get(key)
                if bucket is None:
                    right_index[key] = [row_idx]
                else:
                    bucket.append(row_idx)
                    if expect_right_unique and first_duplicate_key is None:
                        first_duplicate_key = key

        if expect_right_unique and first_duplicate_key is not None:
            raise SerifValueError(
                f"expect_right_unique=True violated: right side has duplicate key {first_duplicate_key} "
                f"(appears {len(right_index[first_duplicate_key])} times)."
            )
        right_index_get = right_index.get

        # ------------------------------------------------------------------
        # 3. Cardinality/match tracking state
        # ------------------------------------------------------------------
        left_keys_seen = set() if expect_left_unique else None
        matched_right_rows = set() if keep_unmatched_right else None

        # ------------------------------------------------------------------
        # 4. Probe in left-row order, building gather index lists — the
        #    rows are only ints here; column emission happens back in
        #    _join_impl, one take per column.
        # ------------------------------------------------------------------
        left_take = []
        right_take = []
        PAD = -1  # gather sentinel: emit None (see _gather_join_column)

        for left_idx in range(left_nrows):
            key = tuple(kd[left_idx] for kd in left_key_data)
            if validate_hashable:
                Table._validate_key_tuple_hashable(key, left_keys, left_idx)

            if left_keys_seen is not None:
                if key in left_keys_seen:
                    raise SerifValueError(
                        f"expect_left_unique=True violated: left side has duplicate key {key}."
                    )
                left_keys_seen.add(key)

            matches = right_index_get(key)
            # `is not None`, not truthiness: accelerated buckets are numpy
            # arrays, which refuse bool() beyond one element. Buckets are
            # never empty on either path.
            if matches is not None:
                for right_idx in matches:
                    if matched_right_rows is not None:
                        matched_right_rows.add(right_idx)
                    left_take.append(left_idx)
                    right_take.append(right_idx)
            elif keep_unmatched_left:
                # Unmatched left row: left values + None-padded right side
                left_take.append(left_idx)
                right_take.append(PAD)

        # ------------------------------------------------------------------
        # 5. Unmatched right rows (full join only)
        # ------------------------------------------------------------------
        if keep_unmatched_right:
            for right_idx in range(right_nrows):
                if right_idx not in matched_right_rows:
                    left_take.append(PAD)
                    right_take.append(right_idx)

        return left_take, right_take

    @staticmethod
    def _gather_join_column(orig_col, indices, nullable_pad):
        """
        Materialize one join output column: gather orig_col's rows at
        `indices`, emitting None where the index is -1 (the pad sentinel
        for rows the other side left unmatched). Typed columns gather on
        the storage buffer through the take accelerator; the fallback
        feeds _wrap_join_column, whose behavior is the specification.
        """
        schema = orig_col.schema()
        if schema is not None and schema.kind is not object:
            fast = _accel_take_pad(orig_col._storage, indices)
            if fast is not None:
                return orig_col._clone(
                    fast,
                    dtype=Schema(schema.kind, schema.nullable or nullable_pad))
        # Per-index access, not to_tuple(): a selective join must not pay a
        # full walk of the source column just to emit a few rows.
        storage = orig_col._storage
        values = [None if i < 0 else storage[i] for i in indices]
        return Table._wrap_join_column(values, orig_col, nullable_pad)

    @staticmethod
    def _wrap_join_column(values, orig_col, nullable_pad):
        """
        Wrap join output values into a Vector, preserving the source column's
        name and (when known) its schema -- one storage walk, no per-element
        re-inference. The result schema is the source schema, widened to
        nullable when the join can inject None rows into this side.
        object/untyped sources fall back to full inference.
        """
        schema = orig_col.schema()
        if schema is None or schema.kind is object:
            return Vector(values, name=orig_col._name)
        return Vector._from_iterable_known_dtype(
            values,
            Schema(schema.kind, schema.nullable or nullable_pad),
            name=orig_col._name,
        )

    def inner_join(self, other, left_on, right_on, expect_left_unique=False, expect_right_unique=True):
        """
        Inner join two Tables on specified key columns.
        Only returns rows where keys match in both tables.

        Args:
            other: Table to join with
            left_on: Column name(s) or Vector(s) from left table
            right_on: Column name(s) or Vector(s) from right table
            expect_left_unique: If True, raises if any left key appears more than once
            expect_right_unique: If True, raises if any right key appears more than once (default True)

        Returns:
            Table with joined results
        """
        return self._join_impl(
            other, left_on, right_on,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
            keep_unmatched_left=False,
            keep_unmatched_right=False,
        )

    def left_join(self, other, left_on, right_on, expect_left_unique=False, expect_right_unique=True):
        """
        Left join two Tables on specified key columns.
        Returns all rows from left table, with matching rows from right (or None for no match).

        Args:
            other: Table to join with
            left_on: Column name(s) or Vector(s) from left table
            right_on: Column name(s) or Vector(s) from right table
            expect_left_unique: If True, raises if any left key appears more than once
            expect_right_unique: If True, raises if any right key appears more than once (default True)

        Returns:
            Table with joined results
        """
        return self._join_impl(
            other, left_on, right_on,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
            keep_unmatched_left=True,
            keep_unmatched_right=False,
        )

    def full_join(self, other, left_on, right_on, expect_left_unique=False, expect_right_unique=False):
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
            expect_left_unique: If True, raises if any left key appears more than once
            expect_right_unique: If True, raises if any right key appears more than once (default False)

        Returns:
            Table with joined results
        """
        return self._join_impl(
            other, left_on, right_on,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
            keep_unmatched_left=True,
            keep_unmatched_right=True,
        )

    @staticmethod
    def _make_uniquifier():
        """Return a uniquify(name) function that suffixes repeats: x, x2, x3..."""
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

        return uniquify

    def _build_partition_index(self, groupby, *, track_row_keys=False,
                               key_label="groupby key"):
        """
        Normalize groupby specs to resolved columns and bucket row indices
        by key tuple. Shared by aggregate() and window().

        Returns (groupby_cols, partition_index, row_keys) where row_keys is
        a per-row key list when track_row_keys=True (window needs it to
        broadcast group values back to rows), else None.
        """
        nrows = len(self)
        if isinstance(groupby, (str, Vector)):
            groupby = [groupby]
        groupby = [self._resolve_column(col) for col in groupby]

        for i, col in enumerate(groupby):
            if len(col) != nrows:
                raise SerifValueError(
                    f"{key_label} at index {i} has length {len(col)}, "
                    f"but table has {nrows} rows."
                )

        # Single-key fast path: bucket in C when the key column supports it
        # (int64, no nulls). track_row_keys declines — window()'s per-row
        # key list would rebuild the Python tuples anyway.
        if len(groupby) == 1 and not track_row_keys:
            fast = _accel_group(groupby[0]._storage)
            if fast is not None:
                return groupby, fast, None

        partition_index = {}
        pk_len = len(groupby)
        over_data = [c._storage.to_tuple() for c in groupby]
        row_keys = [None] * nrows if track_row_keys else None

        for row_idx in range(nrows):
            key = tuple(over_data[i][row_idx] for i in range(pk_len))
            if row_keys is not None:
                row_keys[row_idx] = key
            bucket = partition_index.get(key)
            if bucket is None:
                partition_index[key] = [row_idx]
            else:
                bucket.append(row_idx)

        return groupby, partition_index, row_keys

    @staticmethod
    def _make_group_slicer(source_col):
        """
        Return slicer(row_indices, name) -> the per-group slice of source_col.

        Typed columns gather straight off the storage buffer through the
        take accelerator; on decline (categorical, date/object backends)
        the column is materialized ONCE — lazily, so the accelerated path
        never pays for it — and each group rebuilds with the source's known
        schema, no per-element re-inference. object/untyped sources fall
        back to full inference, exactly as before.
        """
        schema = source_col.schema()
        typed = schema is not None and schema.kind is not object
        state = {}

        def slicer(row_indices, name):
            if typed:
                fast = _accel_take(source_col._storage, row_indices)
                if fast is not None:
                    return source_col._clone(fast, name=name)
            if 'data' not in state:
                state['data'] = source_col._storage.to_tuple()
            values = [state['data'][i] for i in row_indices]
            if not typed:
                return Vector(values, name=name)
            return Vector._from_iterable_known_dtype(
                values, Schema(schema.kind, schema.nullable), name=name)

        return slicer

    @staticmethod
    def _reject_nonscalar(agg_name, value, detail, fn_name):
        # aggregate()/window() are flat-only: every produced cell must be a
        # scalar. A Vector coming back means either a lambda returned a
        # column, or a method like .unique() produced a collection -- both
        # are ambiguous here, so we puke loudly instead of silently nesting.
        if isinstance(value, Vector):
            raise SerifTypeError(
                f"aggregations['{agg_name}']: {detail} returned a non-scalar "
                f"(Vector) value. {fn_name}() is flat-only -- every cell must be "
                f"a scalar. For a per-column block use t[cols].<method>."
            )

    def _apply_aggregations(self, aggregations, group_items, nrows,
                            *, allow_blocks, fn_name):
        """
        Compute per-group scalar values for each aggregation spec.

        Yields (output_name, values) pairs with one value per group, in
        group_items order. Shared dispatch for aggregate() and window():
        bound Vector methods slice the source column per group; bound block
        methods fan out one output column per source column (aggregate only);
        plain callables receive each group as a Table.
        """
        for agg_name, func in aggregations.items():
            if hasattr(func, '__self__') and isinstance(func.__self__, Vector):
                source = func.__self__
                method_name = func.__name__
                if len(source) != nrows:
                    raise SerifValueError(
                        f"aggregations['{agg_name}']: vector length {len(source)} "
                        f"!= table length {nrows}"
                    )

                if source.ndims() == 2:
                    if not allow_blocks:
                        # Block (fan-out) aggregations are supported by
                        # aggregate() but not yet by window(). Refuse rather
                        # than silently index columns by row number.
                        raise SerifTypeError(
                            f"aggregations['{agg_name}']: block aggregations "
                            f"(t[cols].<method>) are not supported in window() yet; "
                            f"use a single-column aggregation or aggregate()."
                        )
                    # Block aggregation: declared width = source column count.
                    # Apply the method to each selected column independently and
                    # fan out to one output column per source column, named by
                    # raw-prepending the dict key (the prefix) to each source
                    # column's own name. Per-column application (rather than
                    # assembling a row) means a mixed-type block never
                    # materialises a heterogeneous Vector.
                    sub_names = source.column_names()
                    sub_cols = source.cols()
                    width = len(sub_names)
                    slicers = [Table._make_group_slicer(c) for c in sub_cols]
                    fanned = [[] for _ in range(width)]
                    for key, row_indices in group_items:
                        for j in range(width):
                            col_slice = slicers[j](row_indices, sub_names[j])
                            try:
                                v = getattr(col_slice, method_name)()
                            except SerifEmptyReductionError as e:
                                col_desc = sub_names[j] if sub_names[j] is not None else f"col{j}"
                                Table._chain_empty_reduction(
                                    e, agg_name,
                                    f"block method '{method_name}', column '{col_desc}'",
                                    key, fn_name)
                            Table._reject_nonscalar(
                                agg_name, v, f"block method '{method_name}'", fn_name)
                            fanned[j].append(v)
                    for j in range(width):
                        base = sub_names[j] if sub_names[j] is not None else f"col{j}_"
                        yield (f"{agg_name}{base}", fanned[j])
                else:
                    slicer = Table._make_group_slicer(source)
                    out = []
                    for key, row_indices in group_items:
                        group_vec = slicer(row_indices, None)
                        try:
                            val = getattr(group_vec, method_name)()
                        except SerifEmptyReductionError as e:
                            Table._chain_empty_reduction(
                                e, agg_name, f"'{method_name}'", key, fn_name)
                        Table._reject_nonscalar(agg_name, val, f"'{method_name}'", fn_name)
                        out.append(val)
                    yield (agg_name, out)
            elif callable(func):
                # Callable receives the group as a Table, one slicer per
                # column (each materializes its column at most once).
                slicers = [(col, Table._make_group_slicer(col))
                           for col in self._storage]
                out = []
                for key, row_indices in group_items:
                    group_cols = [
                        slicer(row_indices, col._name)
                        for col, slicer in slicers
                    ]
                    try:
                        val = func(Table(group_cols))
                    except SerifEmptyReductionError as e:
                        Table._chain_empty_reduction(e, agg_name, "callable", key, fn_name)
                    Table._reject_nonscalar(agg_name, val, "callable", fn_name)
                    out.append(val)
                yield (agg_name, out)
            else:
                hint = (
                    f" (got {type(func).__name__} {func!r}; did you call it by mistake?"
                    f" Use t.col.sum not t.col.sum())"
                    if not callable(func) else ""
                )
                raise SerifTypeError(
                    f"aggregations['{agg_name}'] must be a bound Vector method or callable{hint}"
                )

    def _bound_grouped_sums(self, groupby, aggregations, nrows):
        """Recognize the narrow Arrow hash-grouped sum fast path.

        Return ``(group_column, keys, [(name, values), ...])`` or None to
        decline. Validation errors that the ordinary path would raise before
        doing work are raised here with the same text.
        """
        if not aggregations:
            return None
        specs = [groupby] if isinstance(groupby, (str, Vector)) else groupby
        if specs is None or len(specs) != 1:
            return None
        group_col = self._resolve_column(specs[0])
        if len(group_col) != nrows:
            raise SerifValueError(
                f"groupby key at index 0 has length {len(group_col)}, "
                f"but table has {nrows} rows."
            )

        names = []
        sources = []
        for agg_name, func in aggregations.items():
            if not (hasattr(func, '__self__')
                    and isinstance(func.__self__, Vector)
                    and func.__name__ == 'sum'):
                return None
            source = func.__self__
            if len(source) != nrows:
                raise SerifValueError(
                    f"aggregations['{agg_name}']: vector length {len(source)} "
                    f"!= table length {nrows}"
                )
            if source.ndims() != 1:
                return None
            names.append(agg_name)
            sources.append(source)

        from ._accel.arrow import grouped_sums
        result = grouped_sums(
            group_col._storage, [source._storage for source in sources])
        if result is None:
            return None
        keys, columns = result
        return group_col, keys, list(zip(names, columns))

    @staticmethod
    def _chain_empty_reduction(e, agg_name, desc, key, fn_name):
        """Re-raise a no-verdict error with the group's coordinates attached,
        so the user can tell a data problem ("this group isn't supposed to be
        empty") from a legitimate sparse group (qualify with a lambda)."""
        where = f"group {key!r}" if key != () else "the whole table"
        raise SerifEmptyReductionError(
            f"{fn_name}() aggregation '{agg_name}' ({desc}) over {where}: {e} "
            f"In an aggregation, qualify via a lambda, e.g. "
            f"lambda g: g.<col>.all(on_empty=False)."
        ) from e

    @staticmethod
    def _wrap_group_key_column(values, source_col, name):
        """Wrap groupby key values with the source column's known schema;
        object/untyped sources fall back to inference."""
        schema = source_col.schema()
        if schema is None or schema.kind is object:
            return Vector(values, name=name)
        return Vector._from_iterable_known_dtype(
            values, Schema(schema.kind, schema.nullable), name=name)

    def aggregate(self, groupby=None, aggregations=None):
        """
        Group rows by partition key(s) and compute aggregations.

        Args:
            groupby: Vector, str, or list of these -- column(s) to group by.
                     If None, the entire table is treated as one group.
            aggregations: dict of {output_name: func}
                - Bound method of a Vector (e.g. t.sales.sum): slices the source
                  column per group and calls that method on the slice -> 1 column.
                - Bound method of a block selection (e.g. t['a', 'b'].first):
                  fans out to one column per selected column, each named by
                  raw-prepending output_name to that column's own name
                  (e.g. output_name 'latest_' -> 'latest_a', 'latest_b').
                - Callable: receives the group as a Table, must return a scalar.

            For an ordered/correlated pick (e.g. each deal's most-recent event),
            pre-sort the table and use positional first/last -- a stable global
            sort carries into every group. Bind the block to the SORTED table:
                ts = t.sort_by('date')
                ts.aggregate('deal_id', {'latest_': ts['date', 'valuation'].last})

            aggregate() is flat-only: every produced cell must be a scalar. An
            aggregation that returns a Vector (a lambda returning a column, or a
            collection method like .unique()) raises SerifTypeError.

        Returns:
            Table with one row per unique group, preserving first-appearance order.
            If groupby is None, returns a single-row Table.

        Examples:
            t.aggregate(t.region, {"total": t.sales.sum, "avg": t.price.mean})
            t.aggregate(groupby=t.region, aggregations={"total": t.sales.sum})
            # most-recent event block per deal. Sort first, then bind the block
            # to the SORTED table -- the source columns must be the same table
            # aggregate() groups, or row order won't line up:
            ts = t.sort_by('date')
            ts.aggregate(
                groupby='deal_id',
                aggregations={'latest_': ts['date', 'valuation', 'source'].last},
            )
            t.aggregate(aggregations={"grand_total": t.sales.sum})  # whole table
        """
        nrows = len(self)

        # Allow passing the aggregations dict as the first positional arg
        if isinstance(groupby, dict):
            aggregations = groupby
            groupby = None

        if aggregations is None:
            aggregations = {}

        if groupby is not None:
            fast = self._bound_grouped_sums(groupby, aggregations, len(self))
            if fast is not None:
                group_col, keys, summed = fast
                uniquify = Table._make_uniquifier()
                result_cols = [Table._wrap_group_key_column(
                    keys, group_col, uniquify(group_col._name))]
                for agg_name, values in summed:
                    result_cols.append(Vector(
                        values, name=uniquify(agg_name)))
                return Table(result_cols)

        if groupby is None:
            # Treat the entire table as one group
            partition_index = {(): list(range(nrows))}
            groupby = []
        else:
            groupby, partition_index, _ = self._build_partition_index(groupby)

        group_items = list(partition_index.items())
        uniquify = Table._make_uniquifier()

        # Groupby key columns
        result_cols = []
        for idx, col in enumerate(groupby):
            values = [key[idx] for key, _ in group_items]
            result_cols.append(Table._wrap_group_key_column(
                values, col, name=uniquify(col._name or "key")))

        # Aggregations. Every output name goes through the same uniquifier
        # as the groupby keys — aggregate() never emits duplicate column
        # names, whatever the aggregation dict contains.
        if aggregations:
            for out_name, out_values in self._apply_aggregations(
                    aggregations, group_items, nrows,
                    allow_blocks=True, fn_name='aggregate'):
                result_cols.append(Vector(out_values, name=uniquify(out_name)))

        return Table(result_cols)

    def window(self, groupby, aggregations=None):
        """
        Compute window functions over partitions, returning the same number of rows.

        Like aggregate(), but the aggregated value is broadcast back to every row
        in the group instead of collapsing to one row per group.

        Args:
            groupby: Vector, str, or list -- column(s) to partition by
            aggregations: dict of {output_name: func}
                - Bound method of a Vector (e.g. t.sales.sum): slices the source
                  column per group and calls that method on the slice
                - Callable: receives the group as a Table, must return a scalar

        Returns:
            Table with the same number of rows as input

        Examples:
            t.window(
                groupby=t.region,
                aggregations={
                    "region_total": t.sales.sum,
                    "region_avg":   t.price.mean,
                }
            )
        """
        nrows = len(self)
        groupby, partition_index, row_keys = self._build_partition_index(
            groupby, track_row_keys=True, key_label="Partition key")
        group_items = list(partition_index.items())
        uniquify = Table._make_uniquifier()

        # Groupby key columns are copied straight through -- share storage via
        # _clone (Table() below copies on construction anyway) so the column
        # keeps its backend and subclass (a _Category stays categorical).
        result_cols = []
        for col in groupby:
            result_cols.append(col._clone(col._storage, name=uniquify(col._name or "key")))

        # Aggregations: compute one value per group, then broadcast to rows
        if aggregations:
            keys_in_order = [key for key, _ in group_items]
            for out_name, out_values in self._apply_aggregations(
                    aggregations, group_items, nrows,
                    allow_blocks=False, fn_name='window'):
                group_map = dict(zip(keys_in_order, out_values))
                expanded = [group_map[row_keys[i]] for i in range(nrows)]
                result_cols.append(Vector(expanded, name=uniquify(out_name)))

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
        return _sort.sort_by(self, by, reverse=reverse, na_last=na_last)

    def to_parquet(self, path: str) -> None:
        """Write this Table to a Parquet file. See serif.write_parquet for details."""
        from .io.parquet import write_parquet
        write_parquet(self, path)

    @classmethod
    def from_parquet(cls, path: str) -> 'Table':
        """Read a Parquet file into a Table. See serif.read_parquet for details."""
        from .io.parquet import read_parquet
        return read_parquet(path)

    @classmethod
    def _from_columns_nocopy(cls, columns: list) -> 'Table':
        """
        Assemble a Table from pre-built, freshly-owned Vector columns without
        deep-copying them.  The caller guarantees that no external reference to
        any column exists (i.e. the caller just constructed them).

        Used by read_parquet to skip the O(n*cols) copy that Table.__init__
        normally performs for aliasing safety.
        """
        return _columns.from_columns_nocopy(cls, columns)

    @property
    def _(self):
        """
        Column schema listing: one row per column with the dot-accessor
        name, dtype, and the original name where sanitization changed it.

            .some_string          str     'some string'
            .some_other_strings   str     'some other strings'
            .a_number             int     'a number'
            .a_float              float   'a float'

        Reads column metadata only — never scans data — so it is free at
        any table size. Shows every column (up to 1000).
        """
        from .display import _SchemaView
        return _SchemaView(self)


class MaskedTable(Table):
    """
    Deferred boolean-mask selection — what `t[mask]` returns outside a
    batch() scope. Gathers a column only when someone asks for it.

    Why a snapshot is sound: the storage protocol is rebuild-only and the
    mutation doctrine makes owner-addressed writes swap-on-write — no code
    anywhere mutates a frozen column's storage in place. Capturing each
    column at the storage level at defer time is therefore a permanently
    frozen snapshot: `q = t[t.a > 10]` means "t as it was", forever, with
    no version counters. (The one exception — batch() scopes write into
    private buffers in place — never reaches here: __getitem__ keeps the
    eager path while `t._unlocked`.)

    Row is the existence proof for the shape: a hollow subclass that
    bypasses __init__ and exposes `_storage` as a materialize-on-demand
    property, so every base-class method works unmodified. Hot paths
    (attribute access, string subscripts) gather a single column, cached;
    everything else falls through the `_storage` property, which
    materializes all columns and LATCHES — after the first full access
    the object behaves as a plain Table permanently. Derived results are
    plain Tables/Vectors; the deferred type never escapes as a result.

    Eager at defer time (so error timing and observable state don't
    move): mask type/length validation (in __getitem__) and the survivor
    popcount — len() and .shape are exact from birth.
    """

    # Class-level defaults: an instance that skipped __init__ must read
    # as latched-with-nothing, not recurse through __getattr__ on its
    # own deferral state.
    _mat = None
    _captured = None
    _gathered = None
    _mask_vec = None
    _source_loader = None

    def __new__(cls, source, mask):
        # Bypass Table.__new__/__init__ entirely (see Row).
        return object.__new__(cls)

    def __init__(self, source, mask):
        capture = getattr(type(source), '_mask_capture', None)
        # The source's column map may be stale (a column aliased after
        # construction). The eager path rebuilds names from the gathered
        # columns implicitly; refresh here so the shared map matches —
        # the same lazy rebuild Table.__getattr__ performs.
        if capture is None:
            if any(col._wild for col in source._storage):
                source._column_map = source._build_column_map()

        # Capture at the storage level: private shells sharing each
        # frozen storage O(1) — never the source Table or its column
        # objects, whose names can mutate in place via alias().
        if capture is None:
            captured = tuple(col.copy() for col in source._storage)
            source_loader = None
        else:
            captured, source_loader = capture(source)
        mask_shell = mask.copy()

        # Survivor popcount, eager: len()/shape stay exact and cheap.
        n = _accel_popcount(mask_shell._storage)
        if n is None:
            # None is falsy: null mask entries exclude, same as the filter.
            n = sum(1 for v in mask_shell._storage if v)

        # Deferral state. Table.__setattr__ would route these through
        # column lookup, so bind them raw.
        object.__setattr__(self, '_captured', captured)
        object.__setattr__(self, '_mask_vec', mask_shell)
        object.__setattr__(self, '_gathered', {})
        object.__setattr__(self, '_mat', None)
        object.__setattr__(self, '_source_loader', source_loader)

        # Slot checklist for bypassing Table.__init__ — mirror
        # _from_columns_nocopy. The column map is REUSED from the source
        # (identical names by construction; never mutated in place, only
        # rebound). _warned_collisions carries as a copy so collision
        # warnings the source already fired don't re-fire on a post-latch
        # map rebuild — and a rebuild on our side can't mark the source.
        object.__setattr__(self, '_dtype',      None)
        object.__setattr__(self, '_name',       None)
        object.__setattr__(self, '_wild',       False)
        object.__setattr__(self, '_repr_rows',  None)
        object.__setattr__(self, '_length',     n)
        object.__setattr__(self, '_column_map', source._column_map)
        object.__setattr__(self, '_warned_collisions',
                           set(source._warned_collisions))

    # ------------------------------------------------------------------
    # The deferred core: per-column gather + the materialize-and-latch
    # ------------------------------------------------------------------

    def _gather_column(self, idx):
        """Gather (and cache) one column through the captured snapshot.

        Runs the exact per-column program of the old eager path —
        shell[mask] takes the accel filter or the pure zip-filter — so
        results are identical by construction. Cached: the snapshot is
        frozen, so `q.b + q.b` must not gather twice. The gathered column
        is table-owned, hence tamed and frozen, exactly what
        _build_column_map does to every eager table's columns.
        """
        col = self._gathered.get(idx)
        if col is None:
            if self._source_loader is None:
                col = self._captured[idx][self._mask_vec]
            else:
                col = self._source_loader(idx, self._mask_vec)
            col._wild = False
            col._frozen = True
            self._gathered[idx] = col
        return col

    @property
    def _storage(self):
        """Materialize every column and latch: from here on, every
        base-class method sees a plain Table. Assembled from the same
        cached objects the hot paths handed out, so identity behaves
        like a real Table's. The snapshot is released — a latched
        MaskedTable no longer pins the source buffers."""
        mat = self._mat
        if mat is None:
            cols = tuple(self._gather_column(i)
                         for i in range(len(self._captured)))
            mat = TupleStorage.from_iterable(cols, nullable=False)
            object.__setattr__(self, '_mat', mat)
            self._release_snapshot()
        return mat

    @_storage.setter
    def _storage(self, value):
        # Post-latch rebinds (_write_column, column replacement) land
        # here via Table.__setattr__'s object.__setattr__, which honors
        # data descriptors. A rebind IS a latch: whatever storage the
        # caller installed is now the whole truth.
        object.__setattr__(self, '_mat', value)
        if self._captured is not None:
            self._release_snapshot()

    def _release_snapshot(self):
        object.__setattr__(self, '_captured', None)
        object.__setattr__(self, '_gathered', None)
        object.__setattr__(self, '_mask_vec', None)
        object.__setattr__(self, '_source_loader', None)

    def _snapshot_names_current(self):
        """Gathered columns are handed out live (cached) — a rename
        through one (alias(), the wild mechanic) makes the captured map
        stale, the very condition Table.__getattr__ repairs with a
        rebuild. Detect it and decline the deferred shortcut: the Table
        path latches, rebuilds the map, and fires any collision warning,
        exactly as an eager table would."""
        gathered = self._gathered
        if not gathered:
            return True
        return not any(col._wild for col in gathered.values())

    # ------------------------------------------------------------------
    # Hot paths: single-column access without materializing
    # ------------------------------------------------------------------

    def __getattr__(self, attr):
        # Plain column names (and col{N}_ spellings — the captured map
        # holds those too) gather one column. Everything else — indexed
        # accessors ('name__5'), method fallbacks, a stale map — takes
        # Table's path, which may materialize; correct by default.
        if self._mat is None and self._snapshot_names_current():
            col_idx = self._column_map.get(attr)
            if col_idx is None:
                col_idx = self._column_map.get(attr.lower())
            if col_idx is not None:
                return self._gather_column(col_idx)
        return Table.__getattr__(self, attr)

    def __getitem__(self, key):
        if self._mat is None and self._snapshot_names_current():
            if isinstance(key, str):
                return self._gather_column(
                    _columns.resolve_column_key(self._captured, key))
            if isinstance(key, tuple) and all(isinstance(k, str) for k in key):
                # Multi-column selection: gather only the named columns.
                # Table() copies each (O(1) share), same as the eager path.
                return Table([self[col_name] for col_name in key])
        return Table.__getitem__(self, key)

    def cols(self, key=None):
        # Positional single-column access gathers just that column;
        # cols() / cols(slice) return several, so they materialize.
        if self._mat is None and isinstance(key, int):
            idx = key if key >= 0 else key + len(self._captured)
            if not (0 <= idx < len(self._captured)):
                raise IndexError(
                    f"Column index {key} out of range (table has "
                    f"{len(self._captured)} columns)")
            return self._gather_column(idx)
        return Table.cols(self, key)

    # ------------------------------------------------------------------
    # Cheap introspection: exact from the eager popcount, no gathering
    # ------------------------------------------------------------------

    def __len__(self):
        if self._mat is None:
            return self._length
        return Table.__len__(self)

    @property
    def shape(self):
        if self._mat is None:
            n_cols = len(self._captured)
            return (self._length if n_cols else 0, n_cols)
        return Table.shape.fget(self)

    def column_names(self):
        if self._mat is None and self._snapshot_names_current():
            return [col._name for col in self._captured]
        return Table.column_names(self)

    def _schema_columns(self):
        if self._mat is None and self._snapshot_names_current():
            return self._captured
        return Table._schema_columns(self)
