"""Table aggregation orchestration and result construction."""

from .._execution import DECLINED
from .._vector import Schema
from ..errors import SerifValueError
from ..vector import Vector
from . import columns as _columns
from . import dispatch as _dispatch
from . import grouping as _grouping


def _table_class():
    # Local import avoids a cycle while Table delegates aggregation here.
    from ..table import Table
    return Table


def _bound_grouped_sums(table, groupby, aggregations, nrows):
    """Recognize the narrow Arrow hash-grouped sum fast path."""
    if not aggregations:
        return DECLINED

    specifications = (
        [groupby]
        if isinstance(groupby, (str, Vector))
        else groupby
    )
    if specifications is None or len(specifications) != 1:
        return DECLINED

    group_column = _columns.resolve_column(table, specifications[0])
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
            return DECLINED

        source = function.__self__
        if len(source) != nrows:
            raise SerifValueError(
                f"aggregations['{aggregation_name}']: vector length "
                f"{len(source)} != table length {nrows}"
            )
        if source.ndims() != 1:
            return DECLINED
        names.append(aggregation_name)
        sources.append(source)

    result = _dispatch.grouped_sums(
        (group_column.schema(), group_column._storage),
        [(source.schema(), source._storage) for source in sources],
    )
    if result is DECLINED:
        return DECLINED
    keys, columns = result
    return group_column._name, keys, list(zip(names, columns))


def _wrap_group_key_column(values, source_column, name):
    """Wrap group keys with their source column's known schema."""
    if not values:
        return source_column._clone(
            source_column._storage.slice(slice(0, 0)),
            name=name,
        )
    schema = source_column.schema()
    if schema is None or schema.kind is object:
        return Vector(values, name=name)
    return Vector._from_iterable_known_dtype(
        values,
        Schema(schema.kind, schema.nullable),
        name=name,
    )


def _wrap_group_key_storage(storage, schema, name):
    """Wrap grouped key storage without reconstructing its values."""
    return Vector._from_storage(storage, schema, name=name)


def _wrap_group_sum(storage, source_schema, name):
    """Wrap grouped sum storage with its known non-null result schema."""
    return Vector._from_storage(
        storage,
        Schema(source_schema.kind, False),
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
        if fast is not DECLINED:
            key_name, (key_schema, key_storage), summed = fast
            uniquify = _grouping.make_uniquifier()
            result_columns = [
                _wrap_group_key_storage(
                    key_storage,
                    key_schema,
                    uniquify(key_name),
                )
            ]
            for aggregation_name, (source_schema, storage) in summed:
                result_columns.append(
                    _wrap_group_sum(
                        storage,
                        source_schema,
                        uniquify(aggregation_name),
                    )
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
        outputs = _grouping.apply_aggregations(
            table,
            aggregations,
            group_items,
            nrows,
            allow_blocks=True,
            function_name="aggregate",
            infer_empty_schema=True,
        )
        for output_name, output_values, output_schema in outputs:
            output_name = uniquify(output_name)
            if output_schema is None:
                result_columns.append(Vector(output_values, name=output_name))
            else:
                result_columns.append(
                    Vector._from_iterable_known_dtype(
                        output_values,
                        output_schema,
                        name=output_name,
                    )
                )

    return Table(result_columns)
