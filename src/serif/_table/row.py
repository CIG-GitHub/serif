"""Read-only Table row views and row-iteration coordination."""

from ..errors import SerifTypeError
from ..vector import Vector
from .._vector.storage import ArrayStorage, TupleStorage
from . import columns as _columns


class Row(Vector):
    """
    Row behaves like a Vector (math, logic, isinstance), but it is a
    zero-copy view into the Table's columns.

    We deliberately bypass Vector.__init__ to avoid O(N) scans and alias
    tracking during iteration.
    """

    __slots__ = ('_raw_cols', '_columns', '_column_map', '_index', '_dtype')

    def __new__(cls, table, index=0):
        # Bypass Vector.__new__ entirely.
        # This prevents the infinite loop of checking "is the input iterable?"
        return object.__new__(cls)

    def __init__(self, table, index=0):
        # Grab the raw backing store for each column.
        # For non-nullable ArrayStorage we use the array.array directly — this
        # avoids materialising O(N) Python int/float objects upfront (which would
        # happen inside to_tuple()).  Per-element access is slightly slower for
        # array.array than for a plain tuple, but the savings from not having
        # ~N*ncols live objects during iteration easily wins at any realistic N.
        # For nullable ArrayStorage or TupleStorage, fall back to the existing
        # tuple path so null-handling stays correct.
        def _backing(storage):
            if isinstance(storage, ArrayStorage) and storage._mask is None:
                return storage._data  # array.array — O(1) index, lazy boxing
            return storage.to_tuple()

        self._columns = tuple(table._storage)
        self._raw_cols = [_backing(col._storage) for col in self._columns]
        self._column_map = table._column_map
        self._index = index

        # Smart Dtype Inference (Runs once per table iteration/access)
        # If all columns are the same type, the row is that type.
        # Otherwise, it's an object vector.
        from .._vector.dtype import Schema

        if not table._storage:
            self._dtype = Schema(object, True)
        else:
            # Check uniformity of column types
            col_dtypes = [col._dtype for col in table._storage]
            unique_kinds = {dt.kind for dt in col_dtypes}

            if len(unique_kinds) == 1:
                # Homogeneous (Matrix-like)
                kind = unique_kinds.pop()
                # If ANY column is nullable, the row vector must be nullable
                is_nullable = any(dt.nullable for dt in col_dtypes)
                self._dtype = Schema(kind, is_nullable)
            else:
                # Heterogeneous (DataFrame-like)
                self._dtype = Schema(object, True)

        # CRITICAL: We DO NOT call super().__init__()
        # calling Vector.__init__ would materialize the data and kill performance.
        # We are a "Hollow" Vector.

    def set_index(self, index):
        """ Mutable iterator pattern for speed """
        self._index = index
        return self

    @property
    def _storage(self):
        """
        Materialized on demand — the "lazy" part of the view.

        Base Vector methods (math, comparisons, aggregation, sorting) all
        read self._storage; building a TupleStorage of the current row's
        values at access time makes every one of them work on a Row without
        copying anything until the moment it's actually needed.
        """
        return TupleStorage(tuple(col[self._index] for col in self._raw_cols))

    def _clone(self, new_storage, dtype=..., name=...):
        # An operation result derived from a Row is a value, not a view —
        # return a plain Vector of the row's dtype.
        use_dtype = self._dtype if dtype is ... else dtype
        use_name = None if name is ... else name
        return Vector._from_storage(new_storage, use_dtype, name=use_name)

    def __setitem__(self, key, value):
        raise SerifTypeError(
            "Row is a read-only view. Assign through the table instead: "
            "t[row_index, col] = value"
        )

    @property
    def shape(self):
        """
        Recursive shape check.
        1. Standard Vector: (len,)
        2. Vector of Vectors/Tables: (len, inner_dims...)
        """
        my_len = len(self._raw_cols)
        if my_len == 0:
            return (0,)

        # Peek at the first element (using raw access to avoid object creation)
        # to see if it has dimensions (is a Vector/Table)
        first_val = self._raw_cols[0][self._index]

        if hasattr(first_val, 'shape'):
            return (my_len,) + first_val.shape

        return (my_len,)

    def __repr__(self):
        # Custom repr to look like a Row, not a Vector
        idx = self._index
        values = [repr(col[idx]) for col in self._raw_cols]
        return f"Row({idx}: {', '.join(values)})"

    def __getattr__(self, attr):
        # 1. Try column names first (Row behavior)
        col_idx = self._column_map.get(attr.lower())
        if col_idx is not None:
            return self._raw_cols[col_idx][self._index]

        # 2. Fall back to Vector methods (sum, mean, cast, etc.)
        return super().__getattr__(attr)

    def __getitem__(self, key):
        # Optimized hot path for loops
        if type(key) is int:
            return self._raw_cols[key][self._index]

        if type(key) is str:
            col_idx = _columns.resolve_column_key(self._columns, key)
            return self._raw_cols[col_idx][self._index]

        # Fallback to standard vector slicing/masking
        return super().__getitem__(key)

    def __iter__(self):
        # Return a list iterator so unpacking (a, b = row) uses C-level list_next
        # rather than suspending a Python generator frame per element.
        idx = self._index
        return iter([col[idx] for col in self._raw_cols])

    def __len__(self):
        return len(self._raw_cols)


def iter_rows(table):
    """Iterate with one mutable Row view parked on each successive index."""
    row_view = Row(table, 0)
    n = len(table)
    for index in range(n):
        yield row_view.set_index(index)
