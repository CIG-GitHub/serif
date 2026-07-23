"""Table assignment, ownership, and batch mutation coordination."""

from collections.abc import Iterable

from ..errors import SerifIndexError
from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from .._vector.storage import TupleStorage
from . import columns as _columns


def _table_class():
    # Local import avoids a cycle while Table delegates mutation here.
    from ..table import Table
    return Table


def _is_column_name_sequence(key):
    return (
        isinstance(key, (list, tuple))
        and bool(key)
        and all(isinstance(item, str) for item in key)
    )


class _BatchScope:
    """
    Context manager behind Table.batch() — the bulk-edit fast path.

    Everyday mutation doesn't need this: table-addressed writes
    (t[mask, 'v'] = ...) rebuild the column at O(n) per statement. A
    read-modify-write LOOP at O(n) per write is quadratic; batch() makes
    each write O(1).

    Enter: un-share — every column whose storage supports it gets
    privately-copied buffers (the same principle as Table.__init__ copying
    its vectors: un-share whenever write-ownership changes hands), columns
    thaw. `m` IS the table.

    Inside: point writes go raw into the private buffers (fast), and
    thawed columns accept vector-addressed writes (m.v[i] = x); anything
    the buffer can't hold, and any storage without in-place support,
    rebuilds exactly as before — correctness never depends on the fast
    path.

    Exit (including via exception — partial mutation persists, no
    rollback): every column refreezes and in-place licenses revoke (so
    escaped column refs re-freeze too).
    Observable semantics are identical to table-addressed writes; only
    the speed differs.
    """

    __slots__ = ('_table',)

    def __init__(self, table):
        self._table = table

    def __enter__(self):
        table = self._table
        if table._unlocked:
            raise SerifValueError(
                "Table is already inside a batch() scope; "
                "nesting is not supported."
            )
        table._unlocked = True
        for column in table._storage:
            private = getattr(column._storage, 'private_copy', None)
            if private is not None:
                column._storage = private()
                column._inplace_ok = True
            column._frozen = False
        return table

    def __exit__(self, exc_type, exc_val, exc_tb):
        table = self._table
        for column in table._storage:
            column._frozen = True
            column._inplace_ok = False
        table._unlocked = False
        return False


def setattr(table, attr, value):
    """Intercept column replacement and Table metadata assignment."""
    # Let instance attributes initialize normally (before __init__ completes)
    if attr in (
        '_length',
        '_column_map',
        '_dtype',
        '_name',
        '_wild',
        '_repr_rows',
        '_storage',
        '_warned_collisions',
        '_unlocked',
    ):
        object.__setattr__(table, attr, value)
        return

    # Table name lives on an explicit, non-colliding property so that
    # columns own the rest of the attribute namespace (a column named
    # 'name' must resolve to the column, not shadow a property).
    if attr == 'table_name':
        object.__setattr__(table, '_name', value)
        object.__setattr__(table, '_wild', True)
        return
    if attr == 'vector_name':
        raise AttributeError("Table has no 'vector_name' — use '.table_name'.")

    # After initialization, check if setting an existing column
    if table._column_map is not None:
        col_idx_indexed = _columns.resolve_indexed_attribute(
            table._storage,
            attr,
        )

        if col_idx_indexed is not None:
            # Replace the column at validated index
            if not isinstance(value, Vector):
                value = Vector(value)
            else:
                # Copy before storing: keeping the caller's object would
                # alias table state to an external vector (and the rename
                # below would mutate the caller's vector).
                value = value.copy()

            if table._storage and len(value) != table._length:
                raise SerifValueError(
                    f"Cannot assign column '{attr}': length {len(value)} "
                    f"!= table length {table._length}"
                )

            columns = list(table._storage)
            value._name = table._storage[col_idx_indexed]._name
            columns[col_idx_indexed] = value
            table._storage = TupleStorage.from_iterable(
                tuple(columns),
                nullable=False,
            )
            object.__setattr__(
                table,
                '_column_map',
                table._build_column_map(),
            )
            return

        # Regular column lookup by name. Explicit None checks — a column
        # at index 0 is a valid (falsy) lookup result.
        col_idx = _columns.mapped_column_index(table._column_map, attr)
        if col_idx is not None:
            # Replace the column in _storage
            if not isinstance(value, Vector):
                value = Vector(value)
            else:
                # Copy before storing: keeping the caller's object would
                # alias table state to an external vector (and the rename
                # below would mutate the caller's vector).
                value = value.copy()

            # Validate length
            if table._storage and len(value) != table._length:
                raise SerifValueError(
                    f"Cannot assign column '{attr}': length {len(value)} "
                    f"!= table length {table._length}"
                )

            # Replace column (tuples are immutable, so rebuild)
            columns = list(table._storage)
            value._name = table._storage[col_idx]._name
            columns[col_idx] = value
            table._storage = TupleStorage.from_iterable(
                tuple(columns),
                nullable=False,
            )

            # Rebuild column map to reflect any structural changes
            object.__setattr__(
                table,
                '_column_map',
                table._build_column_map(),
            )
            return

    # Reject arbitrary attribute setting - only allow column updates
    raise AttributeError(
        f"Cannot set attribute '{attr}' on Table. "
        f"Column '{attr}' does not exist. Use >>= to add new columns."
    )


def setitem(table, key, value):
    """Plan, validate, and apply an owner-addressed Table assignment."""
    row_spec, col_spec = None, None

    # --- 1. Normalize Key ---
    if _is_column_name_sequence(key):
        # Mirror projection syntax: t[['a', 'b']] = value and
        # t['a', 'b'] = value both mean every row of those columns.
        row_spec = slice(None)
        col_spec = key
    elif isinstance(key, tuple):
        # t[row, col]
        if len(key) != 2:
            raise SerifKeyError(
                "Table assignment requires 1D (row) or 2D (row, col) key."
            )
        row_spec, col_spec = key
        first_is_columns = (
            isinstance(row_spec, str)
            or _is_column_name_sequence(row_spec)
        )
        if first_is_columns:
            row_spec, col_spec = col_spec, row_spec
    else:
        # t[row] or t[slice] -> implies all columns
        row_spec = key
        col_spec = slice(None)

    # --- 2. Validate the complete selector before any write lands ---
    table._validate_assignment_rows(row_spec)
    target_indices = []
    n_cols = len(table._storage)

    if isinstance(col_spec, slice):
        target_indices = list(range(n_cols)[col_spec])
    elif isinstance(col_spec, bool):
        raise SerifTypeError("Boolean values are not column indices")
    elif isinstance(col_spec, int):
        if not (-n_cols <= col_spec < n_cols):
            raise SerifIndexError(
                f"Column index {col_spec} out of range for table width {n_cols}"
            )
        target_indices = [col_spec % n_cols]
    elif isinstance(col_spec, str):
        target_indices = [
            _columns.resolve_column_key(table._storage, col_spec)
        ]
    elif isinstance(col_spec, (tuple, list)):
        # Handle list of names/ints
        for column in col_spec:
            if isinstance(column, str):
                target_indices.append(
                    _columns.resolve_column_key(table._storage, column)
                )
            elif isinstance(column, bool):
                raise SerifTypeError("Boolean values are not column indices")
            elif isinstance(column, int):
                if not (-n_cols <= column < n_cols):
                    raise SerifIndexError(
                        f"Column index {column} out of range for "
                        f"table width {n_cols}"
                    )
                target_indices.append(column % n_cols)
            else:
                raise SerifTypeError(
                    "Column selector lists may contain only names or "
                    f"integer positions, not {type(column).__name__}"
                )
    else:
        raise SerifTypeError(f"Invalid column index type: {type(col_spec)}")

    if not target_indices:
        return  # No columns selected, nothing to do

    # --- 3. Handle Assignment ---

    # CASE A: Scalar Assignment (Broadcast)
    # t[0:5, 'A'] = 10
    if not isinstance(value, Iterable) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        for col_idx in target_indices:
            table._write_column(col_idx, row_spec, value)
        return

    # CASE B: Single Row Assignment
    # t[0, :] = [1, 2, 3]
    if isinstance(row_spec, int):
        # Materialize generator to avoid exhaustion if reused
        val_seq = list(value)
        if len(val_seq) != len(target_indices):
            raise SerifValueError(
                "Row assignment length mismatch: Table target has "
                f"{len(target_indices)} columns, but value has "
                f"{len(val_seq)} items."
            )

        for index, col_idx in enumerate(target_indices):
            table._write_column(col_idx, row_spec, val_seq[index])
        return

    # CASE C: Rectangular/Table Assignment
    # t[1:3, 2:4] = other_table
    Table = _table_class()
    if isinstance(value, Table):
        if len(value.cols()) != len(target_indices):
            raise SerifValueError(
                f"Column count mismatch: Target has {len(target_indices)} "
                f"cols, source table has {len(value.cols())} cols."
            )

        # We delegate row-length validation to the vector.__setitem__ calls below
        for index, col_idx in enumerate(target_indices):
            table._write_column(col_idx, row_spec, value.cols()[index])
        return

    # CASE D: Vector Assignment To One Column
    # A selected Vector is already the canonical 1D value container:
    # t[mask, 'x'] = values[mask]. Multiple target columns remain ambiguous
    # and continue to the unsupported-value error below.
    if isinstance(value, Vector) and len(target_indices) == 1:
        table._write_column(target_indices[0], row_spec, value)
        return

    # CASE E: Raw 2D Iterable Assignment (List of Columns? List of Rows?)
    # Ambiguity Trap: Is [[1,2], [3,4]] two rows of two, or two columns of two?
    # Vector standard: "Iterables usually mean columns".
    # If you pass a list of lists, we treat it as list-of-columns to match
    # Table structure.
    # SPECIAL CASE: If we have a single target column and value is a flat list,
    # treat it as values for that column, not as multiple columns.
    if isinstance(value, (list, tuple)):
        # Single column slice assignment: t[:, 'x'] = [1, 2, 3]
        if len(target_indices) == 1:
            # Check if it's a flat list (not nested)
            if not value or not isinstance(value[0], (list, tuple, Vector)):
                # Flat list -> assign to the single column
                table._write_column(target_indices[0], row_spec, value)
                return

        if len(value) != len(target_indices):
            raise SerifValueError(
                f"Shape mismatch: expected {len(target_indices)} "
                "columns/items."
            )

        # Assume value[i] corresponds to target_indices[i]
        for index, col_idx in enumerate(target_indices):
            table._write_column(col_idx, row_spec, value[index])
        return

    raise SerifTypeError(f"Unsupported assignment value type: {type(value)}")


def validate_assignment_rows(table, row_spec):
    """Validate every row coordinate before a multi-column write starts."""
    nrows = len(table)

    if isinstance(row_spec, bool):
        raise SerifTypeError("Boolean scalar values are not row indices")
    if isinstance(row_spec, int):
        if not (-nrows <= row_spec < nrows):
            raise SerifIndexError(
                f"Row index {row_spec} out of range for table length {nrows}"
            )
        return
    if isinstance(row_spec, slice):
        # Validate a zero step now, before any target column is rebuilt.
        try:
            row_spec.indices(nrows)
        except ValueError as exc:
            raise SerifValueError(str(exc)) from None
        return
    if isinstance(row_spec, Vector):
        schema = row_spec.schema()
        if schema.kind is bool:
            if len(row_spec) != nrows:
                raise SerifValueError(
                    f"Boolean mask length mismatch: {len(row_spec)} != {nrows}"
                )
            return
        if schema.kind is int and not schema.nullable:
            indices = row_spec
        else:
            raise SerifTypeError(
                "Row selector vectors must be bool masks or non-nullable "
                "integer positions"
            )
    elif isinstance(row_spec, (list, tuple)):
        if all(type(item) is bool for item in row_spec):
            if len(row_spec) != nrows:
                raise SerifValueError(
                    f"Boolean mask length mismatch: {len(row_spec)} != {nrows}"
                )
            return
        if not all(type(item) is int for item in row_spec):
            raise SerifTypeError(
                "Row selector lists may contain only booleans or integer "
                "positions"
            )
        indices = row_spec
    else:
        raise SerifTypeError(
            f"Invalid row selector type: {type(row_spec).__name__}"
        )

    for index in indices:
        if not (-nrows <= index < nrows):
            raise SerifIndexError(
                f"Row index {index} out of range for table length {nrows}"
            )


def write_column(table, col_idx, row_spec, value):
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
    column = table._storage[col_idx]
    if table._unlocked:
        column._setitem_impl(row_spec, value)
        return
    new_column = column._clone(column._storage)  # O(1) storage share
    new_column._setitem_impl(row_spec, value)  # rebuild-on-write rebinds
    new_column._wild = False
    new_column._frozen = True
    columns = list(table._storage)
    columns[col_idx] = new_column
    table._storage = TupleStorage.from_iterable(
        tuple(columns),
        nullable=False,
    )


def batch(table):
    return _BatchScope(table)
