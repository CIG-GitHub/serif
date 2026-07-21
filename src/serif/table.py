from .vector import Vector
from ._table import columns as _columns
from ._table import grouping as _grouping
from ._table import joins as _joins
from ._table import lifting as _lifting
from ._table import mutation as _mutation
from ._table import rows as _rows
from ._table import selection as _selection
from ._table import sort as _sort
from ._table import transpose as _transpose
from ._table.row import Row
from ._table.row import iter_rows as _iter_rows
from ._vector import Schema


from .errors import SerifValueError
from .errors import SerifTypeError


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
        return _transpose.transpose(self)

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
        return _joins.inner_join(
            self,
            other,
            left_on,
            right_on,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
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
        return _joins.left_join(
            self,
            other,
            left_on,
            right_on,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
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
        return _joins.full_join(
            self,
            other,
            left_on,
            right_on,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
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
                uniquify = _grouping.make_uniquifier()
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
            groupby, partition_index, _ = _grouping.build_partition_index(
                self,
                groupby,
            )

        group_items = list(partition_index.items())
        uniquify = _grouping.make_uniquifier()

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
            for out_name, out_values in _grouping.apply_aggregations(
                self,
                aggregations,
                group_items,
                nrows,
                allow_blocks=True,
                function_name="aggregate",
            ):
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
        groupby, partition_index, row_keys = _grouping.build_partition_index(
            self,
            groupby,
            track_row_keys=True,
            key_label="Partition key",
        )
        group_items = list(partition_index.items())
        uniquify = _grouping.make_uniquifier()

        # Groupby key columns are copied straight through -- share storage via
        # _clone (Table() below copies on construction anyway) so the column
        # keeps its backend and subclass (a _Category stays categorical).
        result_cols = []
        for col in groupby:
            result_cols.append(col._clone(col._storage, name=uniquify(col._name or "key")))

        # Aggregations: compute one value per group, then broadcast to rows
        if aggregations:
            keys_in_order = [key for key, _ in group_items]
            for out_name, out_values in _grouping.apply_aggregations(
                self,
                aggregations,
                group_items,
                nrows,
                allow_blocks=False,
                function_name="window",
            ):
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


# Imported after Table is defined so the deferred subclass can inherit it.
from ._table.deferred import MaskedTable
