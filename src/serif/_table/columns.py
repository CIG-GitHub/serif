"""Table column traversal, lookup, naming, metadata, and composition."""

import warnings
from collections.abc import Iterable

from ..errors import SerifKeyError
from ..errors import SerifTypeError
from ..errors import SerifValueError
from ..naming import _disambiguate
from ..naming import _reserved_collision
from ..naming import _sanitize_user_name
from ..vector import Vector
from .._vector.storage import TupleStorage


def _table_class():
    # Local import avoids a cycle while Table delegates column operations here.
    from ..table import Table
    return Table


def iter_columns(table):
    """Iterate a table's structural columns, never its public row iterator."""
    return iter(table._storage)


def missing_column_error(name, context="Table"):
    return SerifKeyError(f"Column '{name}' not found in {context}")


def resolve_column_key(columns, key):
    """Resolve a string key to a column index in a column sequence."""
    for index, column in enumerate(columns):
        if column._name == key:
            return index

    key_lower = key.lower()
    for index, column in enumerate(columns):
        if column._name is not None:
            base = _sanitize_user_name(column._name)
            if base is None:
                if f"col{index}_" == key_lower:
                    return index
            elif base == key_lower:
                return index
            elif _disambiguate(base, index) == key_lower:
                return index
        elif f"col{index}_" == key_lower:
            return index

    raise missing_column_error(key)


def parse_indexed_attribute(attr):
    """Parse ``name__N`` into its sanitized base name and column index."""
    base, separator, suffix = attr.rpartition('__')
    if separator and suffix.isdigit():
        if not base:
            raise AttributeError(
                f"Invalid indexed accessor '{attr}': missing base name"
            )
        return _sanitize_user_name(base), int(suffix)
    return attr, None


def resolve_indexed_attribute(columns, attr):
    """Resolve and validate an indexed attribute, or return ``None``."""
    base_name, column_index = parse_indexed_attribute(attr)
    if column_index is None:
        return None
    if column_index < 0 or column_index >= len(columns):
        raise AttributeError(
            f"Column index {column_index} out of range "
            f"(table has {len(columns)} columns)"
        )

    column = columns[column_index]
    sanitized = _sanitize_user_name(column._name)
    if sanitized != base_name:
        raise AttributeError(
            f"Column {column_index} is '{column._name}' "
            f"(sanitizes to '{sanitized}'), not '{base_name}'"
        )
    return column_index


def mapped_column_index(column_map, attr):
    """Look up an attribute in a column map with case-insensitive fallback."""
    column_index = column_map.get(attr)
    if column_index is None:
        column_index = column_map.get(attr.lower())
    return column_index


def build_column_map(table):
    """Build the sanitized attribute-name map and freeze owned columns."""
    column_map = {}
    seen = {}
    for index, column in enumerate(iter_columns(table)):
        if column._name is not None:
            collision = _reserved_collision(column._name)
            if (
                collision is not None
                and collision not in table._warned_collisions
            ):
                table._warned_collisions.add(collision)
                warnings.warn(
                    f"Column '{column._name}' collides with the reserved "
                    f"method/attribute '{collision}': dot access "
                    f"'t.{collision}' returns the method, not this column. "
                    f"Use 't.{collision}_' or 't[{column._name!r}]' to get "
                    f"the column, or rename it.",
                    UserWarning,
                    stacklevel=3,
                )

            base = _sanitize_user_name(column._name)
            if base is None:
                sanitized = f"col{index}_"
            elif base in seen:
                other = seen[base]
                if column._wild or other._wild:
                    warnings.warn(
                        f"Duplicate column name '{base}' "
                        f"(from '{other._name}' and '{column._name}') "
                        "detected. Dot access will be disambiguated with "
                        "indexed suffixes.",
                        UserWarning,
                        stacklevel=3,
                    )
                sanitized = _disambiguate(base, index)
            else:
                sanitized = base
                seen[base] = column
        else:
            sanitized = f"col{index}_"

        column_map[sanitized] = index
        column._mark_tame()
        if not table._unlocked:
            column._frozen = True
    return column_map


def attribute_names(table):
    """Return ordinary attributes plus the current sanitized column names."""
    return set(table._build_column_map().keys()) | set(object.__dir__(table))


def get_attribute(table, attr, fallback):
    """Resolve Table column dot access, including indexed accessors."""
    columns = table._storage
    if any(column._wild for column in columns or []):
        table._column_map = table._build_column_map()

    column_index = resolve_indexed_attribute(columns, attr)
    if column_index is not None:
        return columns[column_index]

    if attr.startswith('col') and attr.endswith('_'):
        middle = attr[3:-1]
        if middle.isdigit():
            index = int(middle)
            if 0 <= index < len(columns):
                return columns[index]
            raise AttributeError(f"Column index {index} out of range")

    column_index = mapped_column_index(table._column_map, attr)
    if column_index is not None:
        return columns[column_index]

    try:
        return fallback(attr)
    except AttributeError:
        raise AttributeError(
            f"{table.__class__.__name__!s} object has no attribute '{attr}'"
        )


def columns(table, key=None):
    """Return one or more structural columns by position."""
    if isinstance(key, int):
        return table._storage[key]
    if isinstance(key, slice):
        return table._storage.to_tuple()[key]
    return table._storage.to_tuple()


def column_names(table):
    return [column._name for column in iter_columns(table)]


def schema_columns(table):
    """Return the metadata-only column sequence used by the schema view."""
    return columns(table)


def to_dict(table):
    """Serialize a table to a column-oriented dict of Python lists."""
    result = {}
    positions = {}
    for index, column in enumerate(iter_columns(table)):
        key = column._name if column._name is not None else f"col{index}_"
        try:
            previous = positions.get(key)
        except TypeError:
            raise SerifTypeError(
                f"Cannot export column {index} to dict: column name {key!r} "
                "is not hashable. Rename the column first."
            ) from None
        if previous is not None:
            raise SerifValueError(
                "to_dict() requires unique export keys; columns "
                f"{previous} and {index} both map to {key!r}. Rename one of "
                "the columns or export the columns as ordered pairs."
            )
        positions[key] = index
        result[key] = list(column._storage)
    return result


def resolve_column(table, spec):
    """Resolve a string or existing Vector column specification."""
    if isinstance(spec, str):
        return table[spec]
    if isinstance(spec, Vector):
        return spec
    raise SerifTypeError(
        "Column specification must be string or Vector, "
        f"got {type(spec).__name__}"
    )


def rename(table, mapping):
    """Return a new Table with simultaneous owner-addressed renames."""
    copied_columns = [column.copy() for column in iter_columns(table)]
    original_names = [column._name for column in copied_columns]

    for key, new_name in mapping.items():
        if isinstance(key, bool):
            raise SerifTypeError(
                "rename key must be a column name (str) or index (int), "
                f"not bool: {key!r}"
            )
        if isinstance(key, int):
            if not 0 <= key < len(copied_columns):
                raise SerifKeyError(
                    f"Column index {key} out of range "
                    f"(table has {len(copied_columns)} columns)"
                )
            copied_columns[key]._name = new_name
            continue

        matches = [
            index
            for index, name in enumerate(original_names)
            if name == key
        ]
        if not matches:
            raise missing_column_error(key)
        if len(matches) > 1:
            raise SerifKeyError(
                f"Column name '{key}' is ambiguous "
                f"({len(matches)} columns share it); rename by position "
                "instead, e.g. "
                f"rename({{{matches[0]}: {new_name!r}}})."
            )
        copied_columns[matches[0]]._name = new_name

    return from_columns_nocopy(_table_class(), copied_columns)


def drop(table, *names):
    """Return a new Table without the selected named columns."""
    if len(names) == 1 and isinstance(names[0], (list, tuple)):
        names = tuple(names[0])

    source_columns = tuple(iter_columns(table))
    existing = [column._name for column in source_columns]
    for name in names:
        if name not in existing:
            raise missing_column_error(name)

    drop_set = set(names)
    kept = [
        column for column in source_columns
        if column._name not in drop_set
    ]
    return _table_class()(kept)


def compose_vector(vector, other):
    """Compose a Vector with one or more columns through ``>>``."""
    if (
        vector._dtype is not None
        and vector._dtype.kind in (bool, int)
        and isinstance(other, int)
    ):
        warnings.warn(
            "The behavior of >> and << have been overridden for "
            "concatenation. Use .bit_lshift()/.bit_rshift() to shift bits.",
            stacklevel=2,
        )

    if type(other).__name__ == 'Table':
        return Vector((vector,) + other.cols())
    if isinstance(other, Vector):
        return Vector((vector,) + (other,))
    if isinstance(other, dict):
        columns = [vector]
        for name, values in other.items():
            if not isinstance(values, Vector):
                values = Vector(values)
            columns.append(
                values.alias(name) if values._name != name else values
            )
        return Vector(columns)
    if isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        return Vector([vector, Vector(tuple(value for value in other))])
    if len(vector) == 0:
        return Vector((other,), dtype=vector._dtype)
    raise SerifTypeError(
        "Cannot add a column of constant values. "
        "Try using Vector.filled(value, length)."
    )


def compose(table, other):
    """Append one or more columns through Table's ``>>`` operator."""
    Table = _table_class()

    if (
        table._dtype is not None
        and table._dtype.kind in (bool, int)
        and isinstance(other, int)
    ):
        warnings.warn(
            "The behavior of >> and << have been overridden. "
            "Use .bit_lshift()/.bit_rshift() to shift bits.",
            stacklevel=2,
        )

    if isinstance(other, dict):
        named_columns = []
        for column_name, values in other.items():
            if isinstance(values, Vector):
                column = values.copy()
            elif isinstance(values, Iterable) and not isinstance(
                values,
                (str, bytes, bytearray),
            ):
                column = Vector(values)
            else:
                raise SerifValueError(
                    f"Column '{column_name}' value must be iterable "
                    "(list, Vector, etc.), not scalar. "
                    f"Use Vector.filled({values!r}, {len(table)}) for "
                    "scalar broadcast."
                )

            if table._storage and len(column) != table._length:
                raise SerifValueError(
                    f"Column '{column_name}' has length {len(column)}, "
                    f"expected {table._length}"
                )

            column._name = column_name
            if _sanitize_user_name(column_name) in table._column_map:
                warnings.warn(
                    f"Adding column with name '{column_name}' which already "
                    "exists in the table. Consider renaming to avoid "
                    "confusion.",
                    UserWarning,
                    stacklevel=3,
                )
            named_columns.append(column)

        return Table(tuple(iter_columns(table)) + tuple(named_columns))

    if isinstance(other, Table):
        if (
            table._dtype is not None
            and not table._dtype.nullable
            and other.schema() is not None
            and not other.schema().nullable
            and table._dtype.kind != other.schema().kind
        ):
            raise SerifTypeError(
                "Cannot concatenate two typesafe Vectors of different types"
            )
        return Vector(
            tuple(iter_columns(table)) + tuple(iter_columns(other)),
            dtype=table._dtype,
        )
    if isinstance(other, Vector):
        return Vector(
            tuple(iter_columns(table)) + (other,),
            dtype=table._dtype,
        )
    if isinstance(other, Iterable) and not isinstance(
        other,
        (str, bytes, bytearray),
    ):
        return Vector(
            tuple(iter_columns(table)) + (Vector(other),),
            dtype=table._dtype,
        )
    if len(table) == 0:
        return Vector((other,), dtype=table._dtype)
    raise SerifTypeError(
        "Cannot add a column of constant values. "
        "Try using Vector.filled(value, length)."
    )


def from_columns_nocopy(cls, columns):
    """Assemble a Table from freshly owned, pre-built Vector columns."""
    table = object.__new__(cls)
    object.__setattr__(table, '_dtype', None)
    object.__setattr__(table, '_name', None)
    object.__setattr__(table, '_wild', False)
    object.__setattr__(table, '_repr_rows', None)
    object.__setattr__(
        table,
        '_length',
        len(columns[0]) if columns else 0,
    )
    object.__setattr__(table, '_column_map', None)
    object.__setattr__(table, '_warned_collisions', set())
    object.__setattr__(
        table,
        '_storage',
        TupleStorage.from_iterable(tuple(columns), nullable=False),
    )
    object.__setattr__(table, '_column_map', table._build_column_map())
    return table
