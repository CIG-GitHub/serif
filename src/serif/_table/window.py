"""Table window aggregation orchestration and row-wise broadcast."""

from ..vector import Vector
from . import grouping as _grouping


def _table_class():
    # Local import avoids a cycle while Table delegates windowing here.
    from ..table import Table
    return Table


def window(table, groupby, aggregations=None):
    """Compute partition aggregations and broadcast them to source rows."""
    Table = _table_class()
    nrows = len(table)
    groupby, partition_index, row_keys = _grouping.build_partition_index(
        table,
        groupby,
        track_row_keys=True,
        key_label="Partition key",
    )
    group_items = list(partition_index.items())
    uniquify = _grouping.make_uniquifier()

    # Partition keys pass straight through. Clone their existing storage so
    # each key retains its backend and subclass.
    result_columns = [
        column._clone(
            column._storage,
            name=uniquify(column._name or "key"),
        )
        for column in groupby
    ]

    if aggregations:
        keys_in_order = [key for key, _ in group_items]
        outputs = _grouping.apply_aggregations(
            table,
            aggregations,
            group_items,
            nrows,
            allow_blocks=False,
            function_name="window",
            infer_empty_schema=True,
        )
        for output_name, output_values, output_schema in outputs:
            group_map = dict(zip(keys_in_order, output_values))
            expanded = [group_map[row_keys[index]] for index in range(nrows)]
            output_name = uniquify(output_name)
            if output_schema is None:
                result_columns.append(Vector(expanded, name=output_name))
            else:
                result_columns.append(
                    Vector._from_iterable_known_dtype(
                        expanded,
                        output_schema,
                        name=output_name,
                    )
                )

    return Table(result_columns)
