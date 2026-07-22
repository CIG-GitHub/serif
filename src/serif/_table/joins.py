"""Table join validation, matching, and result construction."""

from .._execution import DECLINED
from .._vector import Schema
from .._vector.selection import take_pad_storage
from .._vector.selection import take_pad_values
from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from . import columns as _columns
from ._python import joins as _python_joins
from .columns import iter_columns


def _table_class():
    # Local import avoids a cycle while Table delegates joins here.
    from ..table import Table
    return Table


def _validate_key_tuple_hashable(key_tuple, key_cols, row_idx):
    """Validate object/untyped join-key values before hashing."""
    try:
        hash(key_tuple)
    except TypeError as exc:
        for index, (component, column) in enumerate(zip(key_tuple, key_cols)):
            try:
                hash(component)
            except TypeError:
                column_name = column._name or f"key_{index}"
                raise SerifTypeError(
                    f"Join key value in '{column_name}' at row {row_idx} is "
                    f"not hashable: {type(component).__name__}. Join keys "
                    "must be hashable."
                ) from exc
        raise SerifTypeError(
            f"Join key at row {row_idx} is not hashable."
        ) from exc


def _validate_join_keys(table, other, left_on, right_on):
    """Validate and normalize join key specifications."""
    from datetime import date, datetime

    def get_column(source, column_spec, side_name):
        try:
            return _columns.resolve_column(source, column_spec)
        except (SerifKeyError, ValueError):
            raise _columns.missing_column_error(
                column_spec if isinstance(column_spec, str) else "column",
                context=f"{side_name} table",
            )

    def validate_key_dtype(column, side_name, index):
        schema = column.schema()
        if schema is None:
            return

        kind = schema.kind
        if kind is float:
            raise SerifTypeError(
                f"Invalid join key dtype 'float' at position {index} on "
                f"{side_name} side. Floating-point columns cannot be used "
                "as join keys due to precision issues."
            )

        allowed_types = (int, str, bool, date, datetime, object)
        if kind not in allowed_types:
            raise SerifTypeError(
                f"Invalid join key dtype '{kind.__name__}' at position "
                f"{index} on {side_name} side. Join keys must support stable "
                "equality and hashing."
            )

    if isinstance(left_on, (str, Vector)):
        left_on = [left_on]
    if isinstance(right_on, (str, Vector)):
        right_on = [right_on]

    if not (isinstance(left_on, list) and isinstance(right_on, list)):
        raise SerifValueError(
            "left_on and right_on must be strings, Vectors, or lists"
        )
    if not left_on or not right_on:
        raise SerifValueError("Must specify at least 1 join key")
    if len(left_on) != len(right_on):
        raise SerifValueError(
            f"left_on and right_on must have same length: "
            f"got {len(left_on)} and {len(right_on)}"
        )

    normalized = []
    for index, (left_spec, right_spec) in enumerate(zip(left_on, right_on)):
        left_column = get_column(table, left_spec, "left")
        right_column = get_column(other, right_spec, "right")

        if len(left_column) != len(table):
            raise SerifValueError(
                f"Left join key at index {index} has length "
                f"{len(left_column)}, but left table has {len(table)} rows"
            )
        if len(right_column) != len(other):
            raise SerifValueError(
                f"Right join key at index {index} has length "
                f"{len(right_column)}, but right table has {len(other)} rows"
            )

        validate_key_dtype(left_column, "left", index)
        validate_key_dtype(right_column, "right", index)

        left_schema = left_column.schema()
        right_schema = right_column.schema()
        if left_schema is not None and right_schema is not None:
            if left_schema.kind is not right_schema.kind:
                raise SerifTypeError(
                    f"Join key at index {index} has mismatched dtypes: "
                    f"{left_schema.kind.__name__} (left) vs "
                    f"{right_schema.kind.__name__} (right)"
                )

        normalized.append((left_column, right_column))

    return normalized


def _numpy_joins():
    from ._numpy import joins

    return joins


def _arrow_joins():
    from ._arrow import joins

    return joins


def _dispatch_single_key_join(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Try useful single-key join implementations in deterministic order."""
    arguments = (
        left_storage,
        right_storage,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )
    result = _numpy_joins().probe_int64_dense(*arguments)
    if result is not DECLINED:
        return result
    result = _arrow_joins().probe_strings_hash(*arguments)
    if result is not DECLINED:
        return result
    result = _numpy_joins().probe_int64(*arguments)
    if result is not DECLINED:
        return result
    return _arrow_joins().probe_strings(*arguments)


def _probe_python(
    table,
    left_keys,
    right_keys,
    right_nrows,
    *,
    validate_hashable,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Run the canonical probe with semantic hashability validation."""
    validate_left = None
    validate_right = None
    if validate_hashable:
        validate_left = lambda key, row: _validate_key_tuple_hashable(
            key,
            left_keys,
            row,
        )
        validate_right = lambda key, row: _validate_key_tuple_hashable(
            key,
            right_keys,
            row,
        )

    return _python_joins.probe(
        [key._storage for key in left_keys],
        [key._storage for key in right_keys],
        len(table),
        right_nrows,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
        validate_left=validate_left,
        validate_right=validate_right,
    )


def _raise_probe_diagnostic(probed):
    """Raise the public cardinality error represented by a probe outcome."""
    if probed[0] == "right_dup":
        raise SerifValueError(
            f"expect_right_unique=True violated: right side has duplicate "
            f"key {probed[1]} (appears {probed[2]} times)."
        )
    if probed[0] == "left_dup":
        raise SerifValueError(
            f"expect_left_unique=True violated: left side has duplicate "
            f"key {probed[1]}."
        )


def _join(
    table,
    other,
    left_on,
    right_on,
    *,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Shared orchestration for inner, left, and full joins."""
    Table = _table_class()
    pairs = _validate_join_keys(table, other, left_on, right_on)
    left_keys = [left_key for left_key, _ in pairs]
    right_keys = [right_key for _, right_key in pairs]

    validate_hashable = any(
        column.schema() is None or column.schema().kind is object
        for column in left_keys + right_keys
    )

    right_nrows = len(other)
    left_columns = tuple(iter_columns(table))
    right_columns = tuple(iter_columns(other))

    # Drop a right column only when it is the resolved right key object and
    # the corresponding left and right key names match. Duplicate names are
    # legal, so name equality alone is insufficient.
    drop_right_indices = {
        index
        for index, column in enumerate(right_columns)
        if any(
            column is right_key and left_key._name == right_key._name
            for left_key, right_key in pairs
        )
    }

    probed = (
        _dispatch_single_key_join(
            left_keys[0]._storage,
            right_keys[0]._storage,
            expect_left_unique,
            expect_right_unique,
            keep_unmatched_left,
            keep_unmatched_right,
        )
        if len(pairs) == 1
        else DECLINED
    )
    if probed is DECLINED:
        probed = _probe_python(
            table,
            left_keys,
            right_keys,
            right_nrows,
            validate_hashable=validate_hashable,
            expect_left_unique=expect_left_unique,
            expect_right_unique=expect_right_unique,
            keep_unmatched_left=keep_unmatched_left,
            keep_unmatched_right=keep_unmatched_right,
        )

    if probed[0] != "ok":
        _raise_probe_diagnostic(probed)
    left_take, right_take = probed[1], probed[2]

    if not len(left_take):
        return Table(())

    left_nullable_pad = keep_unmatched_right
    right_nullable_pad = keep_unmatched_left
    result_columns = [
        _gather_join_column(column, left_take, left_nullable_pad)
        for column in left_columns
    ]
    result_columns.extend(
        _gather_join_column(column, right_take, right_nullable_pad)
        for index, column in enumerate(right_columns)
        if index not in drop_right_indices
    )
    return Table(result_columns)


def _gather_join_column(original_column, indices, nullable_pad):
    """Gather one output column, padding unmatched lanes with null."""
    schema = original_column.schema()
    if schema is not None and schema.kind is not object:
        fast = take_pad_storage(original_column._storage, indices)
        if fast is not DECLINED:
            return original_column._clone(
                fast,
                dtype=Schema(schema.kind, schema.nullable or nullable_pad),
            )

    values = take_pad_values(original_column._storage, indices)
    return _wrap_join_column(values, original_column, nullable_pad)


def _wrap_join_column(values, original_column, nullable_pad):
    """Wrap gathered values while preserving known source-column schema."""
    schema = original_column.schema()
    if schema is None or schema.kind is object:
        return Vector(values, name=original_column._name)
    return Vector._from_iterable_known_dtype(
        values,
        Schema(schema.kind, schema.nullable or nullable_pad),
        name=original_column._name,
    )


def inner_join(
    table,
    other,
    left_on,
    right_on,
    expect_left_unique=False,
    expect_right_unique=True,
):
    return _join(
        table,
        other,
        left_on,
        right_on,
        expect_left_unique=expect_left_unique,
        expect_right_unique=expect_right_unique,
        keep_unmatched_left=False,
        keep_unmatched_right=False,
    )


def left_join(
    table,
    other,
    left_on,
    right_on,
    expect_left_unique=False,
    expect_right_unique=True,
):
    return _join(
        table,
        other,
        left_on,
        right_on,
        expect_left_unique=expect_left_unique,
        expect_right_unique=expect_right_unique,
        keep_unmatched_left=True,
        keep_unmatched_right=False,
    )


def full_join(
    table,
    other,
    left_on,
    right_on,
    expect_left_unique=False,
    expect_right_unique=False,
):
    return _join(
        table,
        other,
        left_on,
        right_on,
        expect_left_unique=expect_left_unique,
        expect_right_unique=expect_right_unique,
        keep_unmatched_left=True,
        keep_unmatched_right=True,
    )
