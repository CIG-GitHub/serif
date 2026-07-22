"""Shared Table partitioning and grouped aggregation evaluation."""

from .._execution import DECLINED
from .._vector import Schema
from .._vector.selection import take_storage
from ..errors import SerifEmptyReductionError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from ._python import grouping as _python_grouping
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates grouping here.
    from ..table import Table
    return Table


def make_uniquifier():
    """Return a function that suffixes repeated names: x, x2, x3, ..."""
    used_names = set()

    def uniquify(name):
        if name not in used_names:
            used_names.add(name)
            return name
        index = 2
        while f"{name}{index}" in used_names:
            index += 1
        unique_name = f"{name}{index}"
        used_names.add(unique_name)
        return unique_name

    return uniquify


def _numpy_grouping():
    from ._numpy import grouping

    return grouping


def _arrow_grouping():
    from ._arrow import grouping

    return grouping


def _dispatch_single_key(storage):
    """Try useful optional single-key bucket implementations in order."""
    result = _numpy_grouping().group_indices(storage)
    if result is not DECLINED:
        return result
    return _arrow_grouping().group_strings(storage)


def _bucket_storages(storages, nrows, *, track_row_keys=False):
    """Bucket validated key storage through optional, then Python paths."""
    if len(storages) == 1 and not track_row_keys:
        result = _dispatch_single_key(storages[0])
        if result is not DECLINED:
            return result, None
    return _python_grouping.bucket_rows(
        storages,
        nrows,
        track_row_keys=track_row_keys,
    )


def build_partition_index(
    table,
    groupby,
    *,
    track_row_keys=False,
    key_label="groupby key",
):
    """Resolve group keys and bucket row indices in first-appearance order."""
    nrows = len(table)
    if isinstance(groupby, (str, Vector)):
        groupby = [groupby]
    groupby = [table._resolve_column(column) for column in groupby]

    for index, column in enumerate(groupby):
        if len(column) != nrows:
            raise SerifValueError(
                f"{key_label} at index {index} has length {len(column)}, "
                f"but table has {nrows} rows."
            )

    partition_index, row_keys = _bucket_storages(
        [column._storage for column in groupby],
        nrows,
        track_row_keys=track_row_keys,
    )
    return groupby, partition_index, row_keys


def _make_group_slicer(source_column):
    """Return a schema-aware slicer for one aggregation source column."""
    schema = source_column.schema()
    typed = schema is not None and schema.kind is not object
    state = {}

    def slicer(row_indices, name):
        if typed:
            return source_column._clone(
                take_storage(source_column._storage, row_indices),
                name=name,
            )
        if "data" not in state:
            state["data"] = source_column._storage.to_tuple()
        values = [state["data"][index] for index in row_indices]
        if not typed:
            return Vector(values, name=name)
        return Vector._from_iterable_known_dtype(
            values,
            Schema(schema.kind, schema.nullable),
            name=name,
        )

    return slicer


def _reject_nonscalar(aggregation_name, value, detail, function_name):
    """Enforce aggregate()/window()'s flat-result contract."""
    if isinstance(value, Vector):
        raise SerifTypeError(
            f"aggregations['{aggregation_name}']: {detail} returned a "
            f"non-scalar (Vector) value. {function_name}() is flat-only -- "
            "every cell must be a scalar. For a per-column block use "
            "t[cols].<method>."
        )


def _chain_empty_reduction(
    error,
    aggregation_name,
    description,
    key,
    function_name,
):
    """Re-raise a no-verdict reduction with its group coordinates."""
    where = f"group {key!r}" if key != () else "the whole table"
    raise SerifEmptyReductionError(
        f"{function_name}() aggregation '{aggregation_name}' "
        f"({description}) over {where}: {error} In an aggregation, qualify "
        "via a lambda, e.g. lambda g: g.<col>.all(on_empty=False)."
    ) from error


def apply_aggregations(
    table,
    aggregations,
    group_items,
    nrows,
    *,
    allow_blocks,
    function_name,
):
    """Yield one named scalar-result sequence per aggregation output."""
    Table = _table_class()

    for aggregation_name, function in aggregations.items():
        if hasattr(function, "__self__") and isinstance(
            function.__self__, Vector
        ):
            source = function.__self__
            method_name = function.__name__
            if len(source) != nrows:
                raise SerifValueError(
                    f"aggregations['{aggregation_name}']: vector length "
                    f"{len(source)} != table length {nrows}"
                )

            if source.ndims() == 2:
                if not allow_blocks:
                    raise SerifTypeError(
                        f"aggregations['{aggregation_name}']: block "
                        "aggregations (t[cols].<method>) are not supported "
                        "in window() yet; use a single-column aggregation "
                        "or aggregate()."
                    )

                source_columns = tuple(iter_columns(source))
                source_names = [column._name for column in source_columns]
                width = len(source_columns)
                slicers = [
                    _make_group_slicer(column) for column in source_columns
                ]
                fanned = [[] for _ in range(width)]

                for key, row_indices in group_items:
                    for index in range(width):
                        column_slice = slicers[index](
                            row_indices,
                            source_names[index],
                        )
                        try:
                            value = getattr(column_slice, method_name)()
                        except SerifEmptyReductionError as error:
                            column_description = (
                                source_names[index]
                                if source_names[index] is not None
                                else f"col{index}"
                            )
                            _chain_empty_reduction(
                                error,
                                aggregation_name,
                                f"block method '{method_name}', column "
                                f"'{column_description}'",
                                key,
                                function_name,
                            )
                        _reject_nonscalar(
                            aggregation_name,
                            value,
                            f"block method '{method_name}'",
                            function_name,
                        )
                        fanned[index].append(value)

                for index in range(width):
                    base = (
                        source_names[index]
                        if source_names[index] is not None
                        else f"col{index}_"
                    )
                    yield f"{aggregation_name}{base}", fanned[index]
            else:
                slicer = _make_group_slicer(source)
                output = []
                for key, row_indices in group_items:
                    group_vector = slicer(row_indices, None)
                    try:
                        value = getattr(group_vector, method_name)()
                    except SerifEmptyReductionError as error:
                        _chain_empty_reduction(
                            error,
                            aggregation_name,
                            f"'{method_name}'",
                            key,
                            function_name,
                        )
                    _reject_nonscalar(
                        aggregation_name,
                        value,
                        f"'{method_name}'",
                        function_name,
                    )
                    output.append(value)
                yield aggregation_name, output
        elif callable(function):
            slicers = [
                (column, _make_group_slicer(column))
                for column in iter_columns(table)
            ]
            output = []
            for key, row_indices in group_items:
                group_columns = [
                    slicer(row_indices, column._name)
                    for column, slicer in slicers
                ]
                try:
                    value = function(Table(group_columns))
                except SerifEmptyReductionError as error:
                    _chain_empty_reduction(
                        error,
                        aggregation_name,
                        "callable",
                        key,
                        function_name,
                    )
                _reject_nonscalar(
                    aggregation_name,
                    value,
                    "callable",
                    function_name,
                )
                output.append(value)
            yield aggregation_name, output
        else:
            hint = (
                f" (got {type(function).__name__} {function!r}; did you call "
                "it by mistake? Use t.col.sum not t.col.sum())"
            )
            raise SerifTypeError(
                f"aggregations['{aggregation_name}'] must be a bound Vector "
                f"method or callable{hint}"
            )
