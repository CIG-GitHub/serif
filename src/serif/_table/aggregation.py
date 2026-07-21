"""Table aggregation orchestration and result construction."""

from .._vector import Schema
from ..errors import SerifValueError
from ..vector import Vector
from . import grouping as _grouping


def _table_class():
    # Local import avoids a cycle while Table delegates aggregation here.
    from ..table import Table
    return Table


def _bound_grouped_sums(table, groupby, aggregations, nrows):
    """Recognize the narrow Arrow hash-grouped sum fast path."""
    if not aggregations:
        return None

    specifications = (
        [groupby]
        if isinstance(groupby, (str, Vector))
        else groupby
    )
    if specifications is None or len(specifications) != 1:
        return None

    group_column = table._resolve_column(specifications[0])
    if len(group_column) != nrows:
        raise SerifValueError(
            f"groupby key at index 0 has length {len(group_column)}, "
            f"but table has {nrows} rows."
        )

    names = []
    sources = []
    for aggregation_name, function in aggregations.items():
        if not (
            hasattr(function, "__self__")
            and isinstance(function.__self__, Vector)
            and function.__name__ == "sum"
        ):
            return None

        source = function.__self__
        if len(source) != nrows:
            raise SerifValueError(
                f"aggregations['{aggregation_name}']: vector length "
                f"{len(source)} != table length {nrows}"
            )
        if source.ndims() != 1:
            return None
        names.append(aggregation_name)
        sources.append(source)

    from .._accel.arrow import grouped_sums

    result = grouped_sums(
        group_column._storage,
        [source._storage for source in sources],
    )
    if result is None:
        return None
    keys, columns = result
    return group_column, keys, list(zip(names, columns))


def _wrap_group_key_column(values, source_column, name):
    """Wrap group keys with their source column's known schema."""
    schema = source_column.schema()
    if schema is None or schema.kind is object:
        return Vector(values, name=name)
    return Vector._from_iterable_known_dtype(
        values,
        Schema(schema.kind, schema.nullable),
        name=name,
    )


def aggregate(table, groupby=None, aggregations=None):
    """Group rows by partition keys and compute scalar aggregations."""
    Table = _table_class()
    nrows = len(table)

    # The aggregations dict may be passed as the first positional argument.
    if isinstance(groupby, dict):
        aggregations = groupby
        groupby = None

    if aggregations is None:
        aggregations = {}

    if groupby is not None:
        fast = _bound_grouped_sums(table, groupby, aggregations, nrows)
        if fast is not None:
            group_column, keys, summed = fast
            uniquify = _grouping.make_uniquifier()
            result_columns = [
                _wrap_group_key_column(
                    keys,
                    group_column,
                    uniquify(group_column._name),
                )
            ]
            for aggregation_name, values in summed:
                result_columns.append(
                    Vector(values, name=uniquify(aggregation_name))
                )
            return Table(result_columns)

    if groupby is None:
        partition_index = {(): list(range(nrows))}
        groupby = []
    else:
        groupby, partition_index, _ = _grouping.build_partition_index(
            table,
            groupby,
        )

    group_items = list(partition_index.items())
    uniquify = _grouping.make_uniquifier()

    result_columns = []
    for index, column in enumerate(groupby):
        values = [key[index] for key, _ in group_items]
        result_columns.append(
            _wrap_group_key_column(
                values,
                column,
                name=uniquify(column._name or "key"),
            )
        )

    if aggregations:
        for output_name, output_values in _grouping.apply_aggregations(
            table,
            aggregations,
            group_items,
            nrows,
            allow_blocks=True,
            function_name="aggregate",
        ):
            result_columns.append(
                Vector(output_values, name=uniquify(output_name))
            )

    return Table(result_columns)
