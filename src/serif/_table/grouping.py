"""Shared Table partitioning and grouped aggregation evaluation."""

import warnings

from .._execution import DECLINED
from .._vector.selection import take_storage
from ..errors import SerifEmptyReductionWarning
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from . import dispatch as _dispatch
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


def _bucket_storages(storages, nrows, *, track_row_keys=False):
    """Bucket validated key storage through optional, then Python paths."""
    if len(storages) == 1 and not track_row_keys:
        result = _dispatch.group_single_key(storages[0])
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
    storage = source_column._storage

    def slicer(row_indices, name):
        if typed:
            return source_column._clone(
                take_storage(storage, row_indices),
                name=name,
            )
        values = [storage[index] for index in row_indices]
        return Vector(values, name=name)

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


def _bound_reduction(function):
    """Return (source, method, accessor) for a Serif-bound operation."""
    owner = getattr(function, '__self__', None)
    if isinstance(owner, Vector):
        return owner, function.__name__, None

    source = getattr(owner, '_serif_bound_vector', None)
    accessor_name = getattr(owner, '_serif_accessor_name', None)
    if isinstance(source, Vector) and accessor_name is not None:
        return source, function.__name__, accessor_name
    return None


def _invoke_bound_reduction(vector, method_name, accessor_name):
    target = (
        vector
        if accessor_name is None
        else getattr(vector, accessor_name)
    )
    return getattr(target, method_name)()


# Verdict reductions whose empty case has a Python identity to return.
_VERDICT_IDENTITY = {'all': True, 'any': False}


def _warn_empty_verdict_groups(
    empty_keys,
    total_groups,
    aggregation_name,
    description,
    method_name,
    function_name,
):
    """One warning per aggregation output naming the empty-verdict groups."""
    identity = _VERDICT_IDENTITY[method_name]
    if empty_keys == [()]:
        where = "the whole table has zero valid values"
    else:
        shown = ", ".join(repr(key) for key in empty_keys[:8])
        if len(empty_keys) > 8:
            shown += ", ..."
        where = (
            f"{len(empty_keys)} of {total_groups} groups have zero valid "
            f"values (keys: {shown})"
        )
    warnings.warn(
        f"{function_name}() aggregation '{aggregation_name}' "
        f"({description}): {where}; {method_name}() returned its identity "
        f"({identity}) there. Qualify via a lambda to state the empty-case "
        f"verdict yourself and silence this warning, e.g. "
        f"lambda g: g.<col>.{method_name}(on_empty={identity}).",
        SerifEmptyReductionWarning,
        stacklevel=2,
    )


def apply_aggregations(
    table,
    aggregations,
    group_items,
    nrows,
    *,
    allow_blocks,
    function_name,
):
    """Yield each output's name, scalar-result sequence, and known schema."""
    Table = _table_class()

    for aggregation_name, function in aggregations.items():
        result_schema = getattr(function, '_serif_result_schema', None)
        group_results = getattr(function, '_serif_group_results', None)
        if group_results is not None:
            yield aggregation_name, group_results(group_items), result_schema
            continue

        bound_reduction = _bound_reduction(function)
        if bound_reduction is not None:
            source, method_name, accessor_name = bound_reduction
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
                identity = _VERDICT_IDENTITY.get(method_name)
                empty_by_column = [[] for _ in range(width)]
                total_groups = 0

                for key, row_indices in group_items:
                    total_groups += 1
                    for index in range(width):
                        column_slice = slicers[index](
                            row_indices,
                            source_names[index],
                        )
                        if (
                            identity is not None
                            and column_slice.count() == 0
                        ):
                            empty_by_column[index].append(key)
                            value = identity
                        else:
                            value = _invoke_bound_reduction(
                                column_slice,
                                method_name,
                                accessor_name,
                            )
                        _reject_nonscalar(
                            aggregation_name,
                            value,
                            f"block method '{method_name}'",
                            function_name,
                        )
                        fanned[index].append(value)

                for index in range(width):
                    if empty_by_column[index]:
                        column_description = (
                            source_names[index]
                            if source_names[index] is not None
                            else f"col{index}"
                        )
                        _warn_empty_verdict_groups(
                            empty_by_column[index],
                            total_groups,
                            aggregation_name,
                            f"block method '{method_name}', column "
                            f"'{column_description}'",
                            method_name,
                            function_name,
                        )

                for index in range(width):
                    base = (
                        source_names[index]
                        if source_names[index] is not None
                        else f"col{index}_"
                    )
                    yield f"{aggregation_name}{base}", fanned[index], None
            else:
                slicer = _make_group_slicer(source)
                identity = _VERDICT_IDENTITY.get(method_name)
                empty_keys = []
                total_groups = 0
                output = []
                for key, row_indices in group_items:
                    total_groups += 1
                    group_vector = slicer(row_indices, None)
                    if identity is not None and group_vector.count() == 0:
                        empty_keys.append(key)
                        value = identity
                    else:
                        value = _invoke_bound_reduction(
                            group_vector,
                            method_name,
                            accessor_name,
                        )
                    _reject_nonscalar(
                        aggregation_name,
                        value,
                        f"'{method_name}'",
                        function_name,
                    )
                    output.append(value)
                if empty_keys:
                    _warn_empty_verdict_groups(
                        empty_keys,
                        total_groups,
                        aggregation_name,
                        f"'{method_name}'",
                        method_name,
                        function_name,
                    )
                yield aggregation_name, output, None
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
                value = function(Table(group_columns))
                _reject_nonscalar(
                    aggregation_name,
                    value,
                    "callable",
                    function_name,
                )
                output.append(value)
            yield aggregation_name, output, result_schema
        else:
            hint = (
                f" (got {type(function).__name__} {function!r}; did you call "
                "it by mistake? Use t.col.sum not t.col.sum())"
            )
            raise SerifTypeError(
                f"aggregations['{aggregation_name}'] must be a bound Vector "
                f"method or callable{hint}"
            )
