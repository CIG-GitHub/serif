"""Row-aware Table transforms and composition."""

from ..errors import SerifValueError
from .._vector.selection import take_storage
from ..vector import Vector
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates row operations here.
    from ..table import Table
    return Table


def dropna(table):
    """Return rows having no null cells (complete-case filtering)."""
    Table = _table_class()
    columns = tuple(iter_columns(table))
    keep = Vector(
        all(column[row_index] is not None for column in columns)
        for row_index in range(len(table))
    )
    return Table(
        tuple(column[keep] for column in columns),
        name=table._name,
    )


def unique(table):
    """Return the first occurrence of each distinct row, in source order."""
    Table = _table_class()
    columns = tuple(iter_columns(table))
    seen_hashable = set()
    seen_rows = []
    keep = []
    for row_index in range(len(table)):
        row = tuple(column[row_index] for column in columns)
        try:
            duplicate = row in seen_hashable
        except TypeError:
            duplicate = row in seen_rows
        if duplicate:
            continue
        try:
            seen_hashable.add(row)
        except TypeError:
            seen_rows.append(row)
        keep.append(row_index)
    return Table(
        tuple(
            column._clone(take_storage(column._storage, keep))
            for column in columns
        ),
        name=table._name,
    )


def concatenate(table, other):
    """Append rows by pairing each destination column positionally."""
    Table = _table_class()
    left_columns = tuple(iter_columns(table))

    if isinstance(other, Table):
        right_columns = tuple(iter_columns(other))
        if len(left_columns) != len(right_columns):
            raise SerifValueError(
                f"Column count mismatch: "
                f"{len(left_columns)} != {len(right_columns)}"
            )
        return Table(tuple(
            left << right
            for left, right in zip(
                left_columns,
                right_columns,
                strict=True,
            )
        ))

    if len(left_columns) != len(other):
        raise SerifValueError(
            f"Column count mismatch: {len(left_columns)} != {len(other)}"
        )
    return Table(tuple(
        left << right
        for left, right in zip(left_columns, other, strict=True)
    ))
