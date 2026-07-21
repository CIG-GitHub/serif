"""Table axis transposition."""

from ..vector import Vector
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates transpose here.
    from ..table import Table
    return Table


def transpose(table):
    """Return a Table whose columns are the input table's rows."""
    Table = _table_class()
    columns = tuple(iter_columns(table))
    transposed_columns = [
        Vector(tuple(column[row_index] for column in columns))
        for row_index in range(len(table))
    ]
    return Table(transposed_columns)
