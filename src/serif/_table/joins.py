"""Table join validation, matching, and result construction."""

from .._accel.api import _accel_group
from .._accel.api import _accel_join_probe
from .._execution import DECLINED
from .._vector import Schema
from .._vector.selection import take_pad_storage
from .._vector.selection import take_pad_values
from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..vector import Vector
from . import columns as _columns
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
        _accel_join_probe(
            left_keys[0]._storage,
            right_keys[0]._storage,
            expect_left_unique,
            expect_right_unique,
            keep_unmatched_left,
            keep_unmatched_right,
        )
        if len(pairs) == 1
        else None
    )
    if probed is None:
        left_take, right_take = _join_probe_pure(
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
    elif probed[0] == "right_dup":
        raise SerifValueError(
            f"expect_right_unique=True violated: right side has duplicate "
            f"key {probed[1]} (appears {probed[2]} times)."
        )
    elif probed[0] == "left_dup":
        raise SerifValueError(
            f"expect_left_unique=True violated: left side has duplicate "
            f"key {probed[1]}."
        )
    else:
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


def _join_probe_pure(
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
    """Hash-index the right side and probe in left-row order."""
    left_nrows = len(table)
    left_key_data = [key._storage.to_tuple() for key in left_keys]

    first_duplicate_key = None
    right_index = (
        _accel_group(right_keys[0]._storage)
        if len(right_keys) == 1
        else None
    )
    if right_index is not None:
        if expect_right_unique:
            duplicates = [
                (bucket[1], key)
                for key, bucket in right_index.items()
                if len(bucket) > 1
            ]
            if duplicates:
                first_duplicate_key = min(duplicates)[1]
    else:
        right_index = {}
        right_index_get = right_index.get
        right_key_data = [key._storage.to_tuple() for key in right_keys]
        for row_index in range(right_nrows):
            key = tuple(data[row_index] for data in right_key_data)
            if validate_hashable:
                _validate_key_tuple_hashable(key, right_keys, row_index)
            bucket = right_index_get(key)
            if bucket is None:
                right_index[key] = [row_index]
            else:
                bucket.append(row_index)
                if expect_right_unique and first_duplicate_key is None:
                    first_duplicate_key = key

    if expect_right_unique and first_duplicate_key is not None:
        raise SerifValueError(
            f"expect_right_unique=True violated: right side has duplicate "
            f"key {first_duplicate_key} "
            f"(appears {len(right_index[first_duplicate_key])} times)."
        )
    right_index_get = right_index.get

    left_keys_seen = set() if expect_left_unique else None
    matched_right_rows = set() if keep_unmatched_right else None
    left_take = []
    right_take = []
    pad = -1

    for left_index in range(left_nrows):
        key = tuple(data[left_index] for data in left_key_data)
        if validate_hashable:
            _validate_key_tuple_hashable(key, left_keys, left_index)

        if left_keys_seen is not None:
            if key in left_keys_seen:
                raise SerifValueError(
                    f"expect_left_unique=True violated: left side has "
                    f"duplicate key {key}."
                )
            left_keys_seen.add(key)

        matches = right_index_get(key)
        if matches is not None:
            for right_index_value in matches:
                if matched_right_rows is not None:
                    matched_right_rows.add(right_index_value)
                left_take.append(left_index)
                right_take.append(right_index_value)
        elif keep_unmatched_left:
            left_take.append(left_index)
            right_take.append(pad)

    if keep_unmatched_right:
        for right_index_value in range(right_nrows):
            if right_index_value not in matched_right_rows:
                left_take.append(pad)
                right_take.append(right_index_value)

    return left_take, right_take


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
