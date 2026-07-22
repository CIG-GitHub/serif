"""Stable Table sorting and coordinated row permutation."""

from ..errors import SerifTypeError
from ..errors import SerifValueError
from .._vector.selection import take_storage
from ..vector import Vector
from .._vector.transforms import _null_sort_flag
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates sorting here.
    from ..table import Table
    return Table


def sort_by(table, by, reverse=False, na_last=True):
    """Return a new Table sorted by one or more keys."""
    Table = _table_class()

    # --- 1. Normalize `by` into a list of specs ---
    if isinstance(by, (str, Vector)):
        keys = [by]
    elif isinstance(by, (list, tuple)):
        if not by:
            raise SerifValueError("sort_by() requires at least one sort key")
        keys = list(by)
    else:
        raise SerifTypeError(
            "sort_by() expects a Vector, column name, or sequence of these; "
            f"got {type(by).__name__}"
        )

    # --- 2. Normalize `reverse` to list[bool] ---
    if isinstance(reverse, bool):
        rev_flags = [reverse] * len(keys)
    elif isinstance(reverse, (list, tuple)):
        if len(reverse) != len(keys):
            raise SerifValueError(
                f"reverse has length {len(reverse)}, but sort keys have "
                f"length {len(keys)}"
            )
        rev_flags = [bool(value) for value in reverse]
    else:
        raise SerifTypeError(
            "reverse must be bool or sequence[bool], got "
            f"{type(reverse).__name__}"
        )

    # --- 3. Resolve all keys to Vector columns from this table ---
    resolved = []
    nrows = len(table)

    for spec in keys:
        column = table._resolve_column(spec)
        if len(column) != nrows:
            raise SerifValueError(
                f"Sort key has length {len(column)}, but table has "
                f"{nrows} rows"
            )
        resolved.append(column)

    # --- 4. Edge case: empty table ---
    if nrows == 0:
        # Preserve columns / names but with no rows
        new_columns = [
            Vector([], name=column._name)
            for column in iter_columns(table)
        ]
        return Table(new_columns)

    # --- 5. Build sorted row index using stable multi-key sort ---
    indices = list(range(nrows))

    # Stable sort: apply keys from last to first
    for column, rev in reversed(list(zip(resolved, rev_flags))):
        data = column._storage.to_tuple()

        def key_fn(index, data=data, rev=rev, na_last=na_last):
            value = data[index]
            # Compare on (flag, value): the shared null-flag rule keeps
            # nulls last/first under BOTH sort directions; `value` is only
            # compared among non-None values.
            return (_null_sort_flag(value is None, rev, na_last), value)

        indices.sort(key=key_fn, reverse=rev)

    # --- 6. Rebuild columns in sorted order ---
    # Permute through the storage protocol: preserves each column's
    # backend AND subclass (a _Category stays categorical, an int column
    # keeps ArrayStorage) with zero re-inference. The columns are freshly
    # built, so the nocopy assembly is safe.
    new_columns = [
        column._clone(take_storage(column._storage, indices))
        for column in iter_columns(table)
    ]
    return Table._from_columns_nocopy(new_columns)
