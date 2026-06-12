"""
Categorical vector: ordered string categories backed by integer codes.

A _Category stores values as int codes into a fixed, ordered category list.
The user always sees strings; the int encoding is internal.

Properties:
- categories: tuple[str, ...] — ordered, immutable, no duplicates
- None is allowed (nullable), never appears in categories
- Comparisons use category order, not lexicographic order
- sort_by() respects category order
- Joining with plain string vectors works transparently (value equality)
"""

from __future__ import annotations
from .base import Vector
from .storage import ArrayStorage
from .dtype import Schema
from ..errors import SerifValueError, SerifTypeError


# Ordering operators — used to decide raise vs. all-False for unknown scalars
import operator as _op
_ORDERING_OPS = frozenset({_op.lt, _op.le, _op.gt, _op.ge})


class _Category(Vector):
    """String vector with a fixed, ordered category list."""

    _categories: tuple  # ordered category strings
    _code_storage: ArrayStorage  # int codes; -1 sentinel is masked by ByteMask

    # Override _ndims from parent
    _ndims = 1


    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def __init__(self, codes: ArrayStorage, categories: tuple, name=None, nullable=False):
        """Internal constructor — use _Category.from_values() externally."""
        self._code_storage = codes
        self._categories = categories
        self._dtype = Schema(str, nullable)
        self._name = name
        self._display_as_row = False
        self._wild = True
        self._fp = None
        self._fp_powers = None
        # _storage must satisfy the base class protocol (iterable of decoded values)
        self._storage = _CategoryStorage(codes, categories)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_values(cls, values, categories, *, name=None):
        """
        Build a _Category from an iterable of string values and an ordered
        category list.

        Parameters
        ----------
        values : iterable of str | None
        categories : list | tuple | Vector of str
            Defines both membership and order. No duplicates allowed.
        name : str, optional

        Raises
        ------
        SerifValueError
            If any non-None value is not in categories.
        SerifTypeError
            If categories contains non-string elements.
        """
        from array import array

        # Reject unordered collections — iteration order would be arbitrary
        if isinstance(categories, (set, frozenset)):
            raise SerifTypeError(
                "categories must be ordered — use a list or tuple, not a set"
            )

        # Normalise categories
        if isinstance(categories, Vector):
            cat_list = [v for v in categories if v is not None]
        else:
            cat_list = list(categories)

        if not all(isinstance(c, str) for c in cat_list):
            raise SerifTypeError("categories must be strings")
        if len(cat_list) != len(set(cat_list)):
            raise SerifValueError("categories must not contain duplicates")

        cat_tuple = tuple(cat_list)
        cat_index = {c: i for i, c in enumerate(cat_tuple)}

        codes_list = []
        null_flags = []
        has_nulls = False

        for v in values:
            if v is None:
                has_nulls = True
                null_flags.append(True)
                codes_list.append(0)  # sentinel
            else:
                if not isinstance(v, str):
                    raise SerifTypeError(
                        f"Categorical values must be strings or None, got {type(v).__name__!r}"
                    )
                if v not in cat_index:
                    raise SerifValueError(
                        f"Value {v!r} is not in the category list {cat_tuple}"
                    )
                null_flags.append(False)
                codes_list.append(cat_index[v])

        from .nullable import ByteMask
        raw = array('q', codes_list)
        mask = ByteMask.from_iterable(null_flags) if has_nulls else None
        code_storage = ArrayStorage(raw, mask)

        return cls(code_storage, cat_tuple, name=name, nullable=has_nulls)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def categories(self) -> tuple:
        """Ordered category tuple."""
        return self._categories

    def _code_of(self, value: str) -> int:
        """Return the integer code for a string value. Raises if not in categories."""
        try:
            return self._categories.index(value)
        except ValueError:
            raise SerifValueError(f"Value {value!r} is not in the category list {self._categories}")

    # ------------------------------------------------------------------
    # Standard Vector protocol
    # ------------------------------------------------------------------

    def __len__(self):
        return len(self._code_storage)

    def __iter__(self):
        yield from self._storage

    def __getitem__(self, key):
        # Delegate to base for slice/mask/int, then re-wrap
        if isinstance(key, int):
            return self._storage[key]

        if isinstance(key, slice):
            new_codes = self._code_storage.slice(key)
            return _Category(new_codes, self._categories, name=self._name,
                                nullable=self._dtype.nullable)

        if isinstance(key, Vector) and key.schema().kind == bool and not key.schema().nullable:
            # boolean mask
            codes_list = []
            null_flags = []
            has_nulls = False
            for i, keep in enumerate(key):
                if keep:
                    is_null = self._code_storage.is_null(i)
                    if is_null:
                        has_nulls = True
                        null_flags.append(True)
                        codes_list.append(0)
                    else:
                        null_flags.append(False)
                        codes_list.append(self._code_storage[i])

            from array import array
            from .nullable import ByteMask
            raw = array('q', codes_list)
            mask = ByteMask.from_iterable(null_flags) if has_nulls else None
            new_codes = ArrayStorage(raw, mask)
            return _Category(new_codes, self._categories, name=self._name,
                                nullable=has_nulls)

        # Fallback: decode → base Vector handles it
        return Vector(list(self._storage)).__getitem__(key)

    def copy(self, new_storage=None, **kwargs):
        if new_storage is None:
            import copy
            return _Category(
                copy.copy(self._code_storage),
                self._categories,
                name=self._name,
                nullable=self._dtype.nullable,
            )
        # When called with a storage override (e.g. from _clone), decode back
        return Vector(list(new_storage))

    # ------------------------------------------------------------------
    # Comparisons — category-order aware
    # ------------------------------------------------------------------

    def _to_code(self, other):
        """Convert a scalar rhs to a code, or return None if incompatible."""
        if isinstance(other, str):
            return self._code_of(other)
        return None

    def _elementwise_compare(self, other, op):
        is_ordering = op in _ORDERING_OPS

        # vs scalar string
        if isinstance(other, str):
            if other not in self._categories:
                if is_ordering:
                    raise SerifValueError(
                        f"Cannot order-compare: {other!r} is not in the category list {self._categories}"
                    )
                # == unknown value → all False; != unknown value → all True (for non-null)
                import operator as _op2
                is_ne = op is _op2.ne
                return Vector._from_iterable_known_dtype(
                    [is_ne and not self._code_storage.is_null(i) for i in range(len(self))],
                    Schema(bool, False)
                )
            rhs_code = self._categories.index(other)
            result = []
            for i in range(len(self)):
                if self._code_storage.is_null(i):
                    result.append(False)
                else:
                    result.append(bool(op(self._code_storage[i], rhs_code)))
            return Vector._from_iterable_known_dtype(result, Schema(bool, False))

        # vs another categorical
        if isinstance(other, _Category):
            if self._categories != other._categories:
                if is_ordering:
                    raise SerifValueError(
                        "Cannot order-compare categoricals with different category lists"
                    )
                # equality/inequality across different lists → compare by label value
                result = []
                for lv, rv in zip(self._storage, other._storage):
                    if lv is None or rv is None:
                        result.append(False)
                    else:
                        result.append(bool(op(lv, rv)))
                return Vector._from_iterable_known_dtype(result, Schema(bool, False))
            # Same categories — use codes directly
            result = []
            for i in range(len(self)):
                l_null = self._code_storage.is_null(i)
                r_null = other._code_storage.is_null(i)
                if l_null or r_null:
                    result.append(False)
                else:
                    result.append(bool(op(self._code_storage[i], other._code_storage[i])))
            return Vector._from_iterable_known_dtype(result, Schema(bool, False))

        # vs plain string vector
        if isinstance(other, Vector) and other.schema() is not None and other.schema().kind == str:
            if is_ordering:
                # Build lookup once; raise if any rhs value isn't in categories
                cat_index = {c: i for i, c in enumerate(self._categories)}
                result = []
                for i, rv in enumerate(other):
                    if self._code_storage.is_null(i) or rv is None:
                        result.append(False)
                        continue
                    if rv not in cat_index:
                        raise SerifValueError(
                            f"Cannot order-compare: {rv!r} is not in the category list {self._categories}"
                        )
                    result.append(bool(op(self._code_storage[i], cat_index[rv])))
            else:
                # equality/inequality: compare by label value
                result = []
                for lv, rv in zip(self._storage, other):
                    if lv is None or rv is None:
                        result.append(False)
                    else:
                        result.append(bool(op(lv, rv)))
            return Vector._from_iterable_known_dtype(result, Schema(bool, False))

        raise SerifTypeError(
            f"Cannot compare categorical with {type(other).__name__!r}"
        )

    def __eq__(self, other):
        import operator
        return self._elementwise_compare(other, operator.eq)

    def __ne__(self, other):
        import operator
        return self._elementwise_compare(other, operator.ne)

    def __lt__(self, other):
        import operator
        return self._elementwise_compare(other, operator.lt)

    def __le__(self, other):
        import operator
        return self._elementwise_compare(other, operator.le)

    def __gt__(self, other):
        import operator
        return self._elementwise_compare(other, operator.gt)

    def __ge__(self, other):
        import operator
        return self._elementwise_compare(other, operator.ge)

    # ------------------------------------------------------------------
    # set_categories
    # ------------------------------------------------------------------

    def set_categories(self, categories):
        """
        Return a new categorical with a different category list.

        The data values are unchanged. The new list may reorder, add, or
        remove categories, subject to one constraint: any category currently
        in use (has at least one non-null value) must appear in the new list.

        Parameters
        ----------
        categories : list | tuple | Vector of str
            New ordered category list. No duplicates. No sets.

        Returns
        -------
        _Category

        Raises
        ------
        SerifValueError
            If any in-use category is absent from the new list.
        SerifTypeError
            If categories is a set/frozenset or contains non-strings.
        """
        return _Category.from_values(self._storage, categories, name=self._name)

    # ------------------------------------------------------------------
    # Sorting — respects category order
    # ------------------------------------------------------------------

    def sort_by(self, reverse=False, na_last=True):
        n = len(self)
        if na_last:
            key_fn = lambda i: (self._code_storage.is_null(i),
                                self._code_storage[i] if not self._code_storage.is_null(i) else 0)
        else:
            key_fn = lambda i: (0 if self._code_storage.is_null(i) else 1,
                                self._code_storage[i] if not self._code_storage.is_null(i) else 0)

        order = sorted(range(n), key=key_fn, reverse=reverse)

        from array import array
        from .nullable import ByteMask
        new_codes_list = []
        new_null_flags = []
        has_nulls = False
        for i in order:
            is_null = self._code_storage.is_null(i)
            if is_null:
                has_nulls = True
                new_null_flags.append(True)
                new_codes_list.append(0)
            else:
                new_null_flags.append(False)
                new_codes_list.append(self._code_storage[i])

        raw = array('q', new_codes_list)
        mask = ByteMask.from_iterable(new_null_flags) if has_nulls else None
        new_code_storage = ArrayStorage(raw, mask)
        return _Category(new_code_storage, self._categories, name=self._name,
                            nullable=self._dtype.nullable)

    # ------------------------------------------------------------------
    # repr
    # ------------------------------------------------------------------

    def __repr__(self):
        from ..display import _printr
        return _printr(self)

    # ------------------------------------------------------------------
    # isin — works by value
    # ------------------------------------------------------------------

    def isin(self, values):
        value_set = set(values) if not isinstance(values, set) else values
        result = [False if v is None else v in value_set for v in self._storage]
        return Vector._from_iterable_known_dtype(result, Schema(bool, False))

    # ------------------------------------------------------------------
    # schema / ndims compatibility
    # ------------------------------------------------------------------

    def ndims(self):
        return 1

    def schema(self):
        return self._dtype


class _CategoryStorage:
    """
    Thin read-only adapter that decodes int codes back to strings.
    Satisfies the storage protocol expected by Vector base methods.
    """
    __slots__ = ('_codes', '_categories')

    def __init__(self, codes: ArrayStorage, categories: tuple):
        self._codes = codes
        self._categories = categories

    def __len__(self):
        return len(self._codes)

    def __getitem__(self, idx: int):
        if self._codes.is_null(idx):
            return None
        return self._categories[self._codes[idx]]

    def __iter__(self):
        cats = self._categories
        for i in range(len(self._codes)):
            if self._codes.is_null(i):
                yield None
            else:
                yield cats[self._codes[i]]

    def is_null(self, idx: int) -> bool:
        return self._codes.is_null(idx)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def slice(self, slc: slice):
        return _CategoryStorage(self._codes.slice(slc), self._categories)
