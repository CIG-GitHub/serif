"""Table indexing and selection semantics."""

import warnings

from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from . import columns as _columns
from .row import Row


def _table_class():
    # Local import avoids a cycle while Table delegates __getitem__ here.
    from ..table import Table
    return Table


def _masked_table_class():
    # MaskedTable remains in table.py until deferred coordination is extracted.
    from ..table import MaskedTable
    return MaskedTable


def getitem(table, key):
    """Select columns, rows, or cells with Table's existing precedence."""
    key = table._check_duplicate(key)

    # Handle string indexing for column names
    if isinstance(key, str):
        return table._storage[
            _columns.resolve_column_key(table._storage, key)
        ]

    # Handle tuple of strings for multi-column selection
    if isinstance(key, tuple) and all(isinstance(k, str) for k in key):
        # Reuse the single-column lookup above for each name so selection
        # semantics stay identical (exact / sanitized / disambiguated /
        # unnamed) and a missing name raises SerifKeyError instead of being
        # silently dropped. Table() copies its inputs, so no aliasing.
        Table = _table_class()
        return Table([table[col_name] for col_name in key])

    if isinstance(key, tuple):
        if len(key) != len(table.shape):
            raise SerifKeyError(
                "Matrix indexing must provide an index in each dimension: "
                f"{table.shape}"
            )

        # Reject 3+ dimensional indexing explicitly
        if len(key) > 2:
            raise SerifKeyError(
                "Table only supports 2D indexing (row, column); "
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
            # Swap if columns came first:
            # [('a', 'b'), 1:3] -> [1:3, ('a', 'b')]
            row_spec, col_spec = col_spec, row_spec

        # Now row_spec is guaranteed to be rows, col_spec is columns

        # Get the row-sliced table first
        if isinstance(row_spec, slice):
            row_sliced = table[row_spec]  # Returns Table
        elif isinstance(row_spec, int):
            # Single row -> return Row, then index into it
            return table[row_spec][col_spec]
        else:
            raise SerifKeyError(f"Invalid row specifier: {type(row_spec)}")

        # Now select columns from the row-sliced table
        if isinstance(col_spec, int):
            # Single column by index
            return row_sliced.cols(col_spec)
        if isinstance(col_spec, slice):
            # Column slice by index
            selected = row_sliced.cols()[col_spec]
            Table = _table_class()
            return Table(selected)
        if isinstance(col_spec, str):
            # Single column by name
            return row_sliced[col_spec]
        if (
            isinstance(col_spec, tuple)
            and all(isinstance(k, str) for k in col_spec)
        ):
            # Multiple columns by name
            return row_sliced[col_spec]
        raise SerifKeyError(f"Invalid column specifier: {type(col_spec)}")

    if isinstance(key, int):
        # A single integer returns a row value rather than another Table.
        return Row(table, key)

    if isinstance(key, Vector) and key.schema().kind == bool:
        # Nullable masks allowed: null entries exclude the row.
        if len(table) != len(key):
            raise SerifValueError(
                f"Boolean mask length mismatch: {len(table)} != {len(key)}"
            )
        if table._unlocked:
            # batch() scope: column buffers are private and mutate in
            # place, so a storage capture is not a snapshot here —
            # gather eagerly, exactly as before.
            Table = _table_class()
            return Table(tuple(column[key] for column in table._storage))
        MaskedTable = _masked_table_class()
        return MaskedTable(table, key)

    if isinstance(key, list) and {type(element) for element in key} == {bool}:
        if len(table) != len(key):
            raise SerifValueError(
                f"Boolean mask length mismatch: {len(table)} != {len(key)}"
            )
        if table._unlocked:
            Table = _table_class()
            return Table(tuple(column[key] for column in table._storage))
        MaskedTable = _masked_table_class()
        return MaskedTable(table, Vector(key))

    if isinstance(key, slice):
        Table = _table_class()
        return Table(
            tuple(column[key] for column in table._storage),
            name=table._name,
        )

    # NOT RECOMMENDED
    if (
        isinstance(key, Vector)
        and key.schema().kind == int
        and not key.schema().nullable
    ):
        if len(table) > 1000:
            warnings.warn(
                "Subscript indexing is sub-optimal for large vectors; "
                "prefer slices or boolean masks",
                stacklevel=2,
            )
        Table = _table_class()
        return Table(tuple(column[key] for column in table._storage))

    # No silent fall-through: an unrecognized key must puke, not return None.
    raise SerifTypeError(
        "Table indices must be column names (str), ints, slices, boolean "
        "masks, or non-nullable int vectors, not "
        f"{type(key).__name__}"
    )
