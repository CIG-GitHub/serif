"""Positional, left-owned null coalescing for Tables."""

from ..errors import SerifTypeError
from ..errors import SerifValueError
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates coalescing here.
    from ..table import Table
    return Table


def coalesce(table, *others):
    """Coalesce same-shaped Tables positionally."""
    Table = _table_class()
    if not others:
        raise SerifValueError(
            "Table.coalesce() requires at least one fallback Table"
        )

    for position, other in enumerate(others, start=1):
        if not isinstance(other, Table):
            from ..vector import Vector
            argument_type = (
                "Vector"
                if isinstance(other, Vector) and other.ndims() == 1
                else type(other).__name__
            )
            raise SerifTypeError(
                "Table.coalesce() expects only Table arguments; "
                f"argument {position} is {argument_type}"
            )
        if other.shape != table.shape:
            raise SerifValueError(
                "Table.coalesce() shape mismatch at argument "
                f"{position}: {table.shape} != {other.shape}"
            )

    primary_columns = tuple(iter_columns(table))
    fallback_columns = tuple(
        tuple(iter_columns(other))
        for other in others
    )
    result_columns = []
    for index, primary in enumerate(primary_columns):
        result = primary.coalesce(*(
            columns[index]
            for columns in fallback_columns
        ))
        # Coalesce is asymmetric: the primary Table owns all result metadata.
        result._name = primary._name
        result._wild = False
        result_columns.append(result)

    return Table(tuple(result_columns), name=table._name)
