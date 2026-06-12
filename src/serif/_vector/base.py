import operator
import warnings
import re
import math
from builtins import isinstance as b_isinstance
from collections.abc import Iterator
from collections.abc import Iterable

from ..errors import SerifTypeError
from ..errors import SerifIndexError
from ..errors import SerifValueError
from ..errors import SerifKeyError
from ..display import _printr
from ..naming import _sanitize_user_name
from .dtype import Schema
from .dtype import infer_dtype
from .dtype import infer_kind
from .dtype import promote_dtype
from .dtype import validate_scalar
from .storage import ArrayStorage
from .storage import TupleStorage

from datetime import date
from datetime import datetime
from datetime import timedelta
from itertools import chain

from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple

# ============================================================
# Reverse arithmetic operation helpers
## This section looks ok
# ============================================================
def _reverse_sub(y, x):
    return x - y

def _reverse_truediv(y, x):
    return x / y

def _reverse_floordiv(y, x):
    return x // y

def _reverse_mod(y, x):
    return x % y

def _reverse_pow(y, x):
    return x ** y

def _slice_length(s: slice, sequence_length: int) -> int:
    start, stop, step = s.indices(sequence_length)
    return max(0, (stop - start + (step - (1 if step > 0 else -1))) // step)


def _pre_compute_op_schema(lhs_schema, rhs, op_func=None):
    """
    Resolve the output Schema for a binary math op purely from types.

    Returns None when the result type cannot be determined without touching data
    (e.g. object dtype, temporal ops, or an unknown rhs kind).
    """
    from .numeric import _KIND_PROMOTION

    if lhs_schema is None or lhs_schema.kind is object:
        return None
    lhs_kind = lhs_schema.kind

    _is_truediv = op_func is operator.truediv or op_func is _reverse_truediv

    if isinstance(rhs, Vector):
        rhs_schema = rhs._dtype
        if rhs_schema is None or rhs_schema.kind is object:
            return None
        result_kind = _KIND_PROMOTION.get((lhs_kind, rhs_schema.kind))
        if result_kind is None:
            return None
        if _is_truediv and result_kind in (bool, int):
            result_kind = float
        return Schema(result_kind, lhs_schema.nullable or rhs_schema.nullable)

    # Scalar: resolve rhs kind
    rhs_kind = type(rhs)
    result_kind = _KIND_PROMOTION.get((lhs_kind, rhs_kind))
    if result_kind is None:
        return None
    if _is_truediv and result_kind in (bool, int):
        result_kind = float
    return Schema(result_kind, lhs_schema.nullable)


# ============================================================
# Small helpers
# ============================================================

def _is_hashable(x: Any) -> bool:
    try:
        hash(x)
        return True
    except Exception:
        return False


def _safe_sortable_list(xs: Iterable[Any]) -> List[Any]:
    """
    Deterministic representation for sets in fingerprinting.
    """
    try:
        return sorted(xs)
    except Exception:
        return sorted((repr(x) for x in xs))


class MethodProxy:
    """Proxy that defers method calls to each element in a Vector."""
    def __init__(self, vector, method_name):
        self._vector = vector
        self._method_name = method_name
    
    def __call__(self, *args, **kwargs):
        method = self._method_name
        results = []
        for elem in self._vector._storage:
            if elem is None:
                results.append(None)
            else:
                results.append(getattr(elem, method)(*args, **kwargs))
        return Vector(results)


# ============================================================
# Vector construction helpers
# ============================================================

def _collect_and_infer(iterable, dtype_hint):
    """
    Walk 'iterable' exactly once.

    Simultaneously: collects values into a list, checks whether all items are
    Vector instances (Table candidate), and infers the DataType.

    Returns
    -------
    data        : list
    all_vectors : bool  — True if every item is a Vector (Table candidate)
    dtype       : DataType | None
    """
    data = []
    all_vectors = True
    dtype = dtype_hint

    for val in iterable:
        data.append(val)
        if not isinstance(val, Vector):
            all_vectors = False
        if dtype is None:
            k = infer_kind(val)
            dtype = Schema(object, True) if k is None else Schema(k, False)
        else:
            dtype = promote_dtype(dtype, val)

    return data, all_vectors, dtype


def _pick_target_class(dtype):
    """Return the Vector subclass appropriate for the given DataType."""
    if dtype is None:
        return Vector
    if dtype.kind is str:
        from .string import _String
        return _String
    if dtype.kind is int:
        from .numeric import _Int
        return _Int
    if dtype.kind is float:
        from .numeric import _Float
        return _Float
    if dtype.kind is date:
        from .dates import _Date
        return _Date
    return Vector


# ============================================================
# Main backend
# ============================================================


class Vector():
    """ Iterable vector with optional type safety """
    _dtype = None  # Schema instance (private)
    _storage = None
    _name = None
    _display_as_row = False
    _wild = False  # Flag for name changes (used by Table column tracking)
    _ndims = 1     # Class-level constant; Table overrides with 2
    
    # Fingerprint constants for O(1) change detection
    _FP_P = (1 << 61) - 1  # Mersenne prime (2^61 - 1)
    _FP_B = 1315423911     # Base for rolling hash

    def schema(self):
        """Get the Schema (kind, nullable) of this vector."""
        return self._dtype


    def __new__(cls, initial=(), dtype=None, name=None, as_row=False, **kwargs):
        # Subclass construction (Table, _Int, _Float, etc.): just allocate,
        # __init__ will handle initialization.
        if cls is not Vector:
            return object.__new__(cls)

        # Normalize dtype: plain Python types are a hint, not a full Schema.
        # Only a Schema instance carries both kind and nullable — use the fast path.
        # Plain types need inference to determine nullable from actual data.
        if dtype is not None and not isinstance(dtype, Schema):
            dtype_hint = Schema(dtype, False)
        else:
            dtype_hint = dtype

        # Fast path: data already materialized and Schema known (internal calls).
        if isinstance(initial, (list, tuple)) and isinstance(dtype_hint, Schema):
            data = initial
            is_table = False
            dtype = dtype_hint
        else:
            # Single Python loop: collect + table-check + infer simultaneously
            data, is_table, dtype = _collect_and_infer(initial, dtype_hint)

        # Table path
        if is_table and data:
            if len({len(x) for x in data}) == 1:
                from ..table import Table
                return Table(initial=data, dtype=dtype, name=name, as_row=as_row)
            warnings.warn('Passing vectors of different length will not produce a Table.')

        # Dispatch to the right subclass
        target_class = _pick_target_class(dtype)

        # Allocate and fully initialize — __init__ will see _storage and return early
        instance = object.__new__(target_class)
        instance._dtype = dtype
        instance._name = name
        instance._display_as_row = as_row
        instance._wild = True
        instance._fp = None
        instance._fp_powers = None
        nullable = dtype.nullable if dtype is not None else True
        instance._storage = instance._build_storage(data, nullable)
        return instance


    def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
        # Factory path: __new__ already built the instance fully.
        if '_storage' in self.__dict__:
            return

        # Subclass path: Table.__init__ → super().__init__(), or other subclass
        # direct construction. self._dtype may already be set by the subclass.
        self._name = name
        self._display_as_row = as_row
        self._wild = True
        if dtype is not None:
            if not isinstance(dtype, Schema):
                dtype = Schema(dtype, False)
            self._dtype = dtype
        nullable = self._dtype.nullable if self._dtype is not None else True
        self._storage = self._build_storage(initial, nullable)
        self._fp = None
        self._fp_powers = None

    def _build_storage(self, data, nullable):
        tc = getattr(self, 'typecode', None)
        if tc is not None:
            return ArrayStorage.from_iterable(data, typecode=tc, nullable=nullable)
        return TupleStorage.from_iterable(data, nullable=nullable)

    def _clone(self, new_storage, dtype=..., name=...):
        """
        Fastest possible copy: bypass __new__, __init__, and all inference.

        Use when the output dtype and subclass are already known — e.g. after
        a sort, permutation, or any op where the type cannot change.

        dtype and name default to self's values (sentinel ... means "keep").
        Pass explicit values to override.
        """
        instance = object.__new__(type(self))
        instance._dtype = self._dtype if dtype is ... else dtype
        instance._name = self._name if name is ... else name
        instance._display_as_row = self._display_as_row
        instance._wild = True
        instance._fp = None
        instance._fp_powers = None
        instance._storage = new_storage
        return instance

    @classmethod
    def _from_iterable_known_dtype(cls, iterable, dtype, *, name=None, as_row=False):
        """Build a Vector directly from an iterable when the dtype is already known.
        Bypasses __new__/__init__ inference; one walk into storage."""
        target_cls = _pick_target_class(dtype)
        instance = object.__new__(target_cls)
        instance._dtype = dtype
        instance._name = name
        instance._display_as_row = as_row
        instance._wild = True
        instance._fp = None
        instance._fp_powers = None
        nullable = dtype.nullable if dtype is not None else True
        instance._storage = instance._build_storage(iterable, nullable)
        return instance


    @property
    def shape(self):
        if not self._storage:
            return tuple()
        return (len(self),)

    @property
    def name(self):
        """Get the name of this vector."""
        return self._name
    
    @name.setter
    def name(self, new_name):
        """Set the name of this vector."""
        self._name = new_name
        self._wild = True  # Mark as wild when renamed

    #-----------------------------------------------------
    # Fingerprinting
    #-----------------------------------------------------

    @staticmethod
    def _hash_element(x: Any) -> int:
        P = Vector._FP_P
        B = Vector._FP_B

        if x is None:
            return 0x9E3779B97F4A7C15
        
        if hasattr(x, "fingerprint") and callable(getattr(x, "fingerprint")):
            return int(x.fingerprint())

        if isinstance(x, float):
            if math.isnan(x):
                return 0xDEADBEEFCAFEBABE
            return hash(x)

        if isinstance(x, set):
            rep = _safe_sortable_list(list(x))
            return Vector._hash_element(tuple(rep))

        if isinstance(x, (list, tuple)):
            h = 0
            for elem in x:
                h = (h * B + Vector._hash_element(elem)) % P
            return h

        if _is_hashable(x):
            return hash(x)

        return hash(repr(x))

    def _ensure_fp_powers(self) -> None:
        n = len(self._storage)
        if n == 0:
            self._fp_powers = []
            return
        P = self._FP_P
        B = self._FP_B
        pw = [1] * n
        for i in range(n - 2, -1, -1):
            pw[i] = (pw[i + 1] * B) % P
        self._fp_powers = pw

    def _compute_fingerprint_full(self) -> int:
        P = self._FP_P
        B = self._FP_B
        total = 0
        for x in self._storage:
            h = self._hash_element(x)
            total = (total * B + h) % P
        return total

    def fingerprint(self) -> int:
        if self._fp is None:
            if self._fp_powers is None or len(self._fp_powers) != len(self._storage):
                self._ensure_fp_powers()
            self._fp = self._compute_fingerprint_full()
        return self._fp

    def _invalidate_fp(self) -> None:
        self._fp = None

    @classmethod
    def new(cls, default_element, length, typesafe=False):
        """ create a new, initialized vector of length * default_element"""
        if length:
            assert isinstance(length, int)
            dtype = infer_dtype([default_element])
            if typesafe:
                dtype = Schema(dtype.kind, False)
            return cls([default_element for _ in range(length)], dtype=dtype)
        dtype = infer_dtype([default_element]) if default_element is not None else Schema(object, False)
        if typesafe:
            dtype = Schema(dtype.kind, False)
        return cls(dtype=dtype)


    def copy(self, new_values=None, name=...):
        use_name = self._name if name is ... else name
        if self._dtype is not None:
            # Typed 1D vector: build storage directly, no inference needed.
            source = new_values if new_values is not None else self._storage
            return self._clone(self._build_storage(source, self._dtype.nullable), name=use_name)
        # dtype unknown (Table or untyped vector): full constructor for class dispatch.
        return Vector(list(new_values or self._storage), dtype=None, name=use_name, as_row=self._display_as_row)
    
    def to_object(self):
        """
        Convert this vector to object dtype, allowing mixed types.
        
        Returns a new Vector with dtype=object containing the same values.
        Useful when you need to assign values of different types to a vector.
        
        Example:
            a = Vector([1, 2, 3, 4])   # int vector
            a = a.to_object()            # now object vector
            a[2] = "ryan"                # allowed - can mix types
        """
        return self._clone(
            TupleStorage.from_iterable(self._storage),
            dtype=Schema(object, self._dtype.nullable if self._dtype is not None else True),
            name=self._name,
        )

    def alias(self, new_name):
        """
        Assign a name to an unnamed vector (returns self for chaining).
        
        This method only works on unnamed vectors. If the vector already has a name,
        use the .name property directly or .copy(name=...) to create a named copy.
        """
        if self._name is not None:
            raise SerifValueError(
                "alias() is reserved for unnamed vectors only. "
                "To rename: use .name = 'new'. "
                "To copy with new name: use .copy(name='new')"
            )
        self._name = new_name
        self._wild = True  # Mark as wild when named
        return self

    def rename(self, new_name):
        """Deprecated: Use .name property or .alias() for unnamed vectors."""
        warnings.warn(
            "rename() is deprecated. Use .name = 'new' to rename, or .alias() for unnamed vectors.",
            DeprecationWarning,
            stacklevel=2
        )
        self.name = new_name
        self._wild = True  # Mark as wild when renamed
        return self
    
    def _mark_tame(self):
        """Mark this vector as tame (not wild)"""
        self._wild = False

    def __repr__(self):
        return(_printr(self))

    def cast(self, target_type):
        """
        Convert each element to target_type, recursively if the element is a Vector.
        Preserves None values and infers nullable dtype.
        """
        py_target_type = target_type  # logical type for dtype / error messages

        # Python Date interceptors
        if target_type is date:
            def caster(x):
                if isinstance(x, date):
                    return x
                return date.fromisoformat(x)
        elif target_type is datetime:
            def caster(x):
                if isinstance(x, datetime):
                    return x
                return datetime.fromisoformat(x)
        else:
            caster = target_type  # either a type like str/int, or a callable

        out = []
        has_none = False
        for i, elem in enumerate(self._storage):
            if elem is None:
                out.append(None)
                has_none = True
                continue

            try:
                if isinstance(elem, Vector):
                    out.append(elem.cast(target_type))
                else:
                    out.append(caster(elem))
            except Exception as exc:
                type_name = getattr(py_target_type, "__name__", repr(py_target_type))
                raise ValueError(
                    f"Cast failed at index {i}: {elem!r} cannot be converted to {type_name}"
                ) from exc

        # Now decide dtype using the *logical* type, not the callable
        if isinstance(py_target_type, type):
            new_dtype = Schema(py_target_type, has_none)
        else:
            new_dtype = infer_dtype(out)

        return Vector(out, dtype=new_dtype, name=self._name, as_row=self._display_as_row)

    def fillna(self, value):
        dtype = self.schema()

        # Type check and promotion (same pattern as __setitem__)
        if dtype is not None and value is not None:
            try:
                validate_scalar(value, dtype)
            except TypeError:
                # Value is incompatible - need promotion
                required_dtype = infer_dtype([value])
                try:
                    # Clone (zero-cost storage share), then promote in one walk.
                    result = self._clone(self._storage)
                    result._promote(required_dtype.kind)
                    # Fill in one walk; Vector(tuple, Schema) fast-paths to free wrap.
                    out = tuple(value if x is None else x for x in result._storage)
                    return Vector(
                        out,
                        dtype=Schema(required_dtype.kind, False),
                        name=self._name,
                        as_row=self._display_as_row
                    )
                except SerifTypeError:
                    raise ValueError(
                        f"fillna: value {value!r} (type {type(value).__name__}) "
                        f"cannot be used with {dtype.kind.__name__} vector. "
                        f"Promotion not supported."
                    )

        # Standard path: fill value is compatible with dtype
        out = tuple(value if x is None else x for x in self._storage)

        # Replacing None with a non-None value eliminates all nulls;
        # replacing None with None leaves nullability unchanged.
        new_nullable = value is None and (self._dtype.nullable if self._dtype is not None else True)

        # Construct new dtype
        if dtype is None:
            # Mixed type → leave as None (dtype inference will happen)
            new_dtype = None
        else:
            new_dtype = Schema(dtype.kind, new_nullable)

        return Vector(
            out,
            dtype=new_dtype,
            name=self._name,
            as_row=self._display_as_row
        )

    def dropna(self):
        """
        Remove None values from the vector.
        
        Returns
        -------
        Vector
            New vector with Nones removed
        
        Examples
        --------
        >>> v = Vector([1, None, 3, None, 5])
        >>> v.dropna()
        Vector([1, 3, 5])
        """
        storage = self._storage
        new_dtype = Schema(self._dtype.kind, False)
        if isinstance(storage, ArrayStorage):
            from array import array as _array
            tc = storage._data.typecode
            kept = [i for i in range(len(storage)) if not storage.is_null(i)]
            new_data = _array(tc, (storage._data[i] for i in kept))
            new_storage = ArrayStorage(new_data, None)
        else:
            new_storage = TupleStorage(tuple(x for x in storage._data if x is not None))
        return self._clone(new_storage, dtype=new_dtype)

    def isna(self):
        """
        Return boolean mask of None values.
        
        Returns
        -------
        Vector
            Boolean vector, True where value is None
        
        Examples
        --------
        >>> v = Vector([1, None, 3])
        >>> v.isna()
        Vector([False, True, False])
        """
        storage = self._storage
        if isinstance(storage, ArrayStorage):
            # Fast path: the null mask is already a packed byte array.
            # If there is no mask, no elements are null.
            if storage._mask is None:
                return self._clone(
                    TupleStorage((False,) * len(storage)),
                    dtype=Schema(bool, False),
                )
            return self._clone(
                TupleStorage(tuple(b == 1 for b in storage._mask._data)),
                dtype=Schema(bool, False),
            )
        return Vector._from_iterable_known_dtype(
            (elem is None for elem in storage),
            Schema(bool, False),
            as_row=self._display_as_row,
        )

    def isinstance(self, types):
        """
        Check if each element is an instance of the given type(s).
        
        Parameters
        ----------
        types : type or tuple of types
            Type or tuple of types to check against (use type(None) for NoneType)
        
        Returns
        -------
        Vector
            Boolean vector, True where element matches type(s)
        
        Examples
        --------
        >>> v = Vector([1, "hello", 3.14, None])
        >>> v.isinstance(int)
        Vector([True, False, False, False])
        >>> v.isinstance((int, float))
        Vector([True, False, True, False])
        >>> v.isinstance(type(None))
        Vector([False, False, False, True])
        """
        return Vector._from_iterable_known_dtype(
            (b_isinstance(elem, types) for elem in self._storage),
            Schema(bool, False),
            as_row=self._display_as_row,
        )

    @property
    def _(self):
        """ streamlined display """
        return ''


    def __iter__(self):
        """ iterate over the underlying tuple """
        return iter(self._storage)

    def __len__(self):
        """ length of the underlying tuple """
        return len(self._storage)

    @property
    def T(self):
        if self._dtype is not None:
            # 1D typed vector: share immutable storage, just flip display flag.
            instance = self._clone(self._storage)
            instance._display_as_row = not self._display_as_row
            return instance
        # Table or untyped: full copy for correct class dispatch.
        inverted = self.copy(name=self._name)
        inverted._display_as_row = not self._display_as_row
        return inverted


    def __getitem__(self, key):
        """ Get item(s) from self. Behavior varies by input type:
        The following return a Vector:
            # Vector of bool: Logical indexing (masking). Get all items where the boolean is True
            # List where every element is a bool. See Vector of bool
            # Slice: return the array elements of the slice.

        Special: Indexing a single index returns a value
            # Int: 
        """
        if isinstance(key, int):
            # Effectively a different input type (single not a list). Returning a value, not a vector.
            return self._storage[key]

        if isinstance(key, tuple):
            if len(key) != len(self.shape):
                raise SerifKeyError(f"Matrix indexing must provide an index in each dimension: {self.shape}")
            if len(key) == 1:
                return self[key[0]]
            return self._storage[key[-1]][key[:-1]]

        key = self._check_duplicate(key)
        if isinstance(key, Vector) and key.schema().kind == bool and not key.schema().nullable:
            if len(self) != len(key):
                raise ValueError(f"Boolean mask length mismatch: {len(self)} != {len(key)}")
            return self.copy((x for x, y in zip(self, key, strict=True) if y), name=self._name)
        if isinstance(key, list) and {type(e) for e in key} == {bool}:
            if len(self) != len(key):
                raise ValueError(f"Boolean mask length mismatch: {len(self)} != {len(key)}")
            return self.copy((x for x, y in zip(self, key, strict=True) if y), name=self._name)
        if isinstance(key, slice):
            return self._clone(self._storage.slice(key))

        # NOT RECOMMENDED
        if isinstance(key, Vector) and key.schema().kind == int and not key.schema().nullable:
            if len(self) > 1000:
                warnings.warn('Subscript indexing is sub-optimal for large vectors; prefer slices or boolean masks')
            return self.copy((self[x] for x in key), name=self._name)

        # NOT RECOMMENDED
        if isinstance(key, list) and {type(e) for e in key} == {int}:
            if len(self) > 1000:
                warnings.warn('Subscript indexing is sub-optimal for large vectors')
            return self.copy((self[x] for x in key), name=self._name)
        raise SerifTypeError(f'Vector indices must be boolean vectors, integer vectors or integers, not {str(type(key))}')


    def __setitem__(self, key, value):
        """
        Optimized in-place assignment for Vector with:
        - boolean masks
        - slices
        - integer indices
        - index vectors
        - list/tuple index sets

        Includes:
        - dtype validation & promotion
        - copy-on-write
        - fingerprint incremental update
        """

        # === Fast precomputed checks ===
        key = self._check_duplicate(key)
        value = self._check_duplicate(value)

        # Is the incoming value iterable?
        is_seq_val = (
            isinstance(value, Iterable)
            and not isinstance(value, (str, bytes, bytearray))
        )

        n = len(self)
        underlying = self._storage  # local bind
        append_update = lambda idx, v: updates.append((idx, v))

        updates = []  # list of (idx, new_value)

        # =====================================================================
        # CASE 1 — Boolean mask (fast-path)
        # =====================================================================
        if (
            isinstance(key, Vector)
            and key.schema().kind == bool
            and not key.schema().nullable
        ) or (
            isinstance(key, list) and all(isinstance(e, bool) for e in key)
        ):
            if len(key) != n:
                raise SerifValueError("Boolean mask length must match vector length.")

            # Precompute true indices (much faster than branch-per-element)
            true_indices = [i for i, flag in enumerate(key) if flag]
            tcount = len(true_indices)

            if is_seq_val:
                if tcount != len(value):
                    raise SerifValueError(
                        "Iterable length must match number of True mask elements."
                    )
                for idx, v in zip(true_indices, value):
                    append_update(idx, v)
            else:
                for idx in true_indices:
                    append_update(idx, value)

        # =====================================================================
        # CASE 2 — Slice assignment
        # =====================================================================
        elif isinstance(key, slice):
            slice_len = _slice_length(key, n)
            start, stop, step = key.indices(n)

            if is_seq_val:
                if slice_len != len(value):
                    raise SerifValueError("Slice length and value length must match.")
                values_to_assign = value
            else:
                # repeat the scalar
                values_to_assign = [value] * slice_len

            # faster than enumerate(zip()) for slices
            rng = range(start, stop, step)
            for idx, new_val in zip(rng, values_to_assign):
                append_update(idx, new_val)

        # =====================================================================
        # CASE 3 — Single integer index
        # =====================================================================
        elif isinstance(key, int):
            # normalize negative index
            if key < 0:
                key += n
            if not (0 <= key < n):
                raise SerifIndexError(
                    f"Index {key} out of range for vector length {n}"
                )
            append_update(key, value)

        # =====================================================================
        # CASE 4 — Vector of integer indices
        # =====================================================================
        elif (
            isinstance(key, Vector)
            and key.schema().kind == int
            and not key.schema().nullable
        ):
            if is_seq_val:
                if len(key) != len(value):
                    raise SerifValueError(
                        "Index-vector length must match value length."
                    )
                for idx, val in zip(key, value):
                    if idx < 0:
                        idx += n
                    if not (0 <= idx < n):
                        raise SerifIndexError(f"Index {idx} out of range.")
                    append_update(idx, val)
            else:
                for idx in key:
                    if idx < 0:
                        idx += n
                    if not (0 <= idx < n):
                        raise SerifIndexError(f"Index {idx} out of range.")
                    append_update(idx, value)

        # =====================================================================
        # CASE 5 — List or tuple of integer indices
        # =====================================================================
        elif (
            isinstance(key, (list, tuple))
            and all(isinstance(e, int) for e in key)
        ):
            if is_seq_val:
                if len(key) != len(value):
                    raise SerifValueError("Index list must match value length.")
                for idx, val in zip(key, value):
                    if idx < 0:
                        idx += n
                    if not (0 <= idx < n):
                        raise SerifIndexError(f"Index {idx} out of range.")
                    append_update(idx, val)
            else:
                for idx in key:
                    if idx < 0:
                        idx += n
                    if not (0 <= idx < n):
                        raise SerifIndexError(f"Index {idx} out of range.")
                    append_update(idx, value)

        else:
            raise SerifTypeError(
                f"Invalid key type: {type(key)}. Must be boolean mask, slice, int, "
                "integer vector, or list/tuple of ints."
            )

        # =====================================================================
        # FAST-PATH TYPE CHECK / PROMOTION
        # =====================================================================
        if updates:
            new_values = [v for _, v in updates]

            # Object dtype accepts any type - skip validation
            if self._dtype is not None and self._dtype.kind is not object:
                incompatible = None
                for val in new_values:
                    try:
                        validate_scalar(val, self._dtype)
                    except TypeError:
                        incompatible = val
                        break

                if incompatible is not None:
                    required_dtype = infer_dtype([incompatible])
                    try:
                        self._promote(required_dtype.kind)
                        underlying = self._storage
                    except SerifTypeError:
                        raise SerifTypeError(
                            f"Cannot set {required_dtype.kind.__name__} in "
                            f"{self._dtype.kind.__name__} vector. "
                            f"Promotion not supported."
                        )
        # =====================================================================
        # MUTATE — copy-on-write + fingerprint updates
        # =====================================================================
        data_list = list(underlying)           # COW materialization

        for idx, new_val in updates:
            old_val = data_list[idx]
            data_list[idx] = new_val

        nullable = self._dtype.nullable if self._dtype is not None else True
        self._storage = TupleStorage.from_iterable(data_list, nullable=nullable)
        self._invalidate_fp()



    """ Comparison Operators - equality and hashing
        # __eq__ ==
        # __ge__ >=
        # __gt__ >
        # __lt__ <
        # __le__ <=
        # __ne__ !=
    """
    def _elementwise_compare(self, other, op):
        other = self._check_duplicate(other)

        # CASE A: Self is 2D (Table on Left)
        # T == v -> [C1==v, C2==v, ...]
        if self.ndims() == 2:
            return self.copy(tuple(
                # recursive call: Column == other
                col._elementwise_compare(other, op) 
                for col in self.cols()
            ))
        
        # CASE B: Other is 2D (Table on Right)
        # v == T -> [v==C1, v==C2, ...]
        if isinstance(other, Vector) and other.ndims() == 2:
            return other.copy(tuple(
                # recursive call: self == Column
                self._elementwise_compare(col, op) 
                for col in other.cols()
            ))
        
        if isinstance(other, Vector):
            # Raise mismatched lengths
            if len(self) != len(other):
                raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
            return Vector._from_iterable_known_dtype(
                (False if (x is None or y is None) else bool(op(x, y)) for x, y in zip(self, other, strict=True)),
                Schema(bool, False),
            )
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            # Raise mismatched lengths
            if len(self) != len(other):
                raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
            return Vector._from_iterable_known_dtype(
                (False if (x is None or y is None) else bool(op(x, y)) for x, y in zip(self, other, strict=True)),
                Schema(bool, False),
            )
        # Scalar comparison
        if other is None and op in (operator.eq, operator.ne):
            warnings.warn(
                "Null comparison: `v == None` always returns False for null values. "
                "Use `v.isna()` to test for nulls.",
                stacklevel=2
            )
        return Vector._from_iterable_known_dtype(
            (False if x is None else bool(op(x, other)) for x in self),
            Schema(bool, False),
        )    # Now, we can redefine the comparison methods using the helper function
    
    def __eq__(self, other):
        return self._elementwise_compare(other, operator.eq)

    def __ge__(self, other):
        return self._elementwise_compare(other, operator.ge)

    def __gt__(self, other):
        return self._elementwise_compare(other, operator.gt)

    def __le__(self, other):
        return self._elementwise_compare(other, operator.le)

    def __lt__(self, other):
        return self._elementwise_compare(other, operator.lt)

    def __ne__(self, other):
        return self._elementwise_compare(other, operator.ne)

    def __and__(self, other):
        return self._elementwise_compare(other, operator.and_)

    def __or__(self, other):
        return self._elementwise_compare(other, operator.or_)

    def __xor__(self, other):
        return self._elementwise_compare(other, operator.xor)

    def __rand__(self, other):
        return self._elementwise_compare(other, operator.and_)

    def __ror__(self, other):
        return self._elementwise_compare(other, operator.or_)

    def __rxor__(self, other):
        return self._elementwise_compare(other, operator.xor)


    """ Math operations """
    def _elementwise_operation(self, other, op_func, op_name: str, op_symbol: str):
        """Helper function to handle element-wise operations with broadcasting."""
        other = self._check_duplicate(other)

        # CASE A: Self is 2D (Table on Left)
        # T + v -> [C1+v, C2+v, ...]
        if self.ndims() == 2:
            return self.copy(tuple(
                col._elementwise_operation(other, op_func, op_name, op_symbol)
                for col in self.cols()
            ))

        # CASE B: Other is 2D (Table on Right)
        # v + T -> [v+C1, v+C2, ...]
        if isinstance(other, Vector) and other.ndims() == 2:
            return other.copy(tuple(
                self._elementwise_operation(col, op_func, op_name, op_symbol)
                for col in other.cols()
            ))

        if isinstance(other, Vector):
            if len(self) != len(other):
                raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
            result_dtype = _pre_compute_op_schema(self._dtype, other, op_func)
            try:
                result_values = tuple(
                    None if (x is None or y is None) else op_func(x, y)
                    for x, y in zip(self, other, strict=True)
                )
            except TypeError:
                result_values = tuple((x, y) for x, y in zip(self, other, strict=True))
                return Vector(result_values, dtype=Schema(object, False), name=None, as_row=self._display_as_row)
            if result_dtype is None:
                result_dtype = infer_dtype(result_values)
            return Vector(result_values, dtype=result_dtype, name=None, as_row=self._display_as_row)

        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            if len(self) != len(other):
                raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
            try:
                result_values = tuple(
                    None if (x is None or y is None) else op_func(x, y)
                    for x, y in zip(self, other, strict=True)
                )
            except TypeError:
                result_values = tuple((x, y) for x, y in zip(self, other, strict=True))
                return Vector(result_values, dtype=Schema(object, False), name=None, as_row=self._display_as_row)
            result_dtype = infer_dtype(result_values)
            return Vector(result_values, dtype=result_dtype, name=None, as_row=self._display_as_row)

        # Scalar path
        result_dtype = _pre_compute_op_schema(self._dtype, other, op_func)
        try:
            result_values = tuple(
                None if x is None else op_func(x, other)
                for x in self._storage
            )
            if result_dtype is None:
                result_dtype = infer_dtype(result_values)
            return Vector(result_values, dtype=result_dtype, name=None, as_row=self._display_as_row)
        except TypeError:
            raise SerifTypeError(
                f"Unsupported operand type(s) for '{op_symbol}': "
                f"'{self._dtype.kind.__name__}' and '{type(other).__name__}'."
            )

    def _unary_operation(self, op_func, op_name: str):
        """Helper function to handle unary operations on each element."""
        storage = self._storage
        if isinstance(storage, ArrayStorage):
            from array import array as _array
            tc = storage._data.typecode
            new_data = _array(tc, (op_func(storage._data[i]) for i in range(len(storage._data))))
            new_storage = ArrayStorage(new_data, storage._mask)
        else:
            new_storage = TupleStorage(tuple(
                None if x is None else op_func(x) for x in storage._data
            ))
        return self._clone(new_storage)
    
    def __add__(self, other):
        return self._elementwise_operation(other, operator.add, '__add__', '+')

    def __mul__(self, other):
        return self._elementwise_operation(other, operator.mul, '__mul__', '*')

    def __sub__(self, other):
        return self._elementwise_operation(other, operator.sub, '__sub__', '-')

    def __neg__(self):
        return self._unary_operation(operator.neg, '__neg__')

    def __pos__(self):
        return self._unary_operation(operator.pos, '__pos__')

    def __abs__(self):
        return self._unary_operation(operator.abs, '__abs__')

    def __invert__(self):
        # For boolean vectors, use logical NOT instead of bitwise NOT
        if self._dtype and self._dtype.kind is bool:
            return self._clone(
                self._build_storage((not x for x in self), self._dtype.nullable)
            )
        return self._unary_operation(operator.invert, '__invert__')

    def __truediv__(self, other):
        return self._elementwise_operation(other, operator.truediv, '__truediv__', '/')

    def __floordiv__(self, other):
        return self._elementwise_operation(other, operator.floordiv, '__floordiv__', '//')

    def __mod__(self, other):
        return self._elementwise_operation(other, operator.mod, '__mod__', '%')

    def __pow__(self, other):
        return self._elementwise_operation(other, operator.pow, '__pow__', '**')

    def __radd__(self, other):
        """Reverse addition: other + self (handles strings specially)"""
        other = self._check_duplicate(other)
        
        # Vector + Vector
        if isinstance(other, Vector):
            if len(self) != len(other):
                raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
            return Vector._from_iterable_known_dtype(
                (None if (x is None or y is None) else x + y for x, y in zip(other, self, strict=True)),
                self._dtype,
                as_row=self._display_as_row,
            )
        
        # Scalar + Vector
        if not isinstance(other, Iterable) or isinstance(other, (str, bytes, bytearray)):
            return Vector._from_iterable_known_dtype(
                (None if x is None else other + x for x in self),
                self._dtype,
                as_row=self._display_as_row,
            )
        
        # Iterable + Vector
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            if len(self) != len(other):
                raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
            return Vector._from_iterable_known_dtype(
                (None if (x is None or y is None) else x + y for x, y in zip(other, self, strict=True)),
                self._dtype,
                as_row=self._display_as_row,
            )
        
        raise SerifTypeError(f"Unsupported operand type: {type(other).__name__}")

    def __rmul__(self, other):
        return self.__mul__(other)

    def __rsub__(self, other):
        return self._elementwise_operation(other, _reverse_sub, '__rsub__', '-')

    def __rtruediv__(self, other):
        return self._elementwise_operation(other, _reverse_truediv, '__rtruediv__', '/')

    def __rfloordiv__(self, other):
        return self._elementwise_operation(other, _reverse_floordiv, '__rfloordiv__', '//')

    def __rmod__(self, other):
        return self._elementwise_operation(other, _reverse_mod, '__rmod__', '%')

    def __rpow__(self, other):
        return self._elementwise_operation(other, _reverse_pow, '__rpow__', '**')


    def _promote(self, new_dtype):
        """ Check if a vector can change data type (int -> float, float -> complex) """
        # Handle both Python types and DataType instances
        if isinstance(new_dtype, Schema):
            target_kind = new_dtype.kind
        elif isinstance(new_dtype, type):
            # Python type like int, float
            target_kind = new_dtype
        else:
            raise SerifTypeError(f"new_dtype must be a Schema instance or Python type, not {type(new_dtype).__name__}")
            
        # Already the target type
        if self._dtype.kind is target_kind:
            return
        
        # Allow numeric promotions: int -> float, float -> complex
        if target_kind is float and self._dtype.kind is int:
            new_tuple = tuple(float(x) if x is not None else None for x in self._storage)
            self._storage = TupleStorage.from_iterable(new_tuple, nullable=self._dtype.nullable)
            self._dtype = Schema(float, self._dtype.nullable)
        elif target_kind is complex and self._dtype.kind in (int, float):
            new_tuple = tuple(complex(x) if x is not None else None for x in self._storage)
            self._storage = TupleStorage.from_iterable(new_tuple, nullable=self._dtype.nullable)
            self._dtype = Schema(complex, self._dtype.nullable)
        elif target_kind is datetime and self._dtype.kind is date:
            new_tuple = tuple(datetime.combine(x, datetime.min.time()) if x is not None else None for x in self._storage)
            self._storage = TupleStorage.from_iterable(new_tuple, nullable=self._dtype.nullable)
            self._dtype = Schema(datetime, self._dtype.nullable)
        else:
            # For backwards compat, raise error if trying invalid promotion
            raise SerifTypeError(f'Cannot convert Vector from {self._dtype.kind.__name__} to {target_kind.__name__}.')
        return

    def ndims(self):
        return self._ndims

    def cols(self, key=None):
        if isinstance(key, int):
            return self._storage[key]
        if isinstance(key, slice):
            return self._storage.to_tuple()[key]
        return self._storage.to_tuple()

    """
    Recursive Vector Operations
    """
    def max(self):
        if self.ndims() == 2:
            return self.copy((c.max() for c in self.cols()), name=None).T
        return max(self)

    def min(self):
        if self.ndims() == 2:
            return self.copy((c.min() for c in self.cols()), name=None).T
        return min(self)

    def sum(self):
        if self.ndims() == 2:
            return self.copy((c.sum() for c in self.cols()), name=None).T
        # Exclude None values from sum
        return sum(v for v in self._storage if v is not None)

    def all(self):
        """Return True if all elements are truthy (excluding None)."""
        if self.ndims() == 2:
            return self.copy((c.all() for c in self.cols()), name=None).T
        return all(v for v in self._storage if v is not None)

    def any(self):
        """Return True if any element is truthy (excluding None)."""
        if self.ndims() == 2:
            return self.copy((c.any() for c in self.cols()), name=None).T
        return any(v for v in self._storage if v is not None)

    def mean(self):
        if self.ndims() == 2:
            return self.copy((c.mean() for c in self.cols()), name=None).T
        # Exclude None values from mean
        non_none = [v for v in self._storage if v is not None]
        return sum(non_none) / len(non_none) if non_none else None

    def stdev(self, population=False):
        if self.ndims() == 2:
            return self.copy((c.stdev(population) for c in self.cols()), name=None).T
        # Exclude None values from stdev
        non_none = [v for v in self._storage if v is not None]
        if len(non_none) < 2:
            return None
        m = sum(non_none) / len(non_none)
        # use in-place sum over generator for fastness. I AM SPEED!
        # This is still 10x slower than numpy.
        num = sum((x-m)*(x-m) for x in non_none)
        return (num/(len(non_none) - 1 + population))**0.5

    def count(self):
        if self.ndims() == 2:
            return self.copy((c.count() for c in self.cols()), name=None).T
        return sum(1 for v in self._storage if v is not None)

    def unique(self):
        seen = set()
        out = []
        has_none = False

        # Fast path: hashable
        try:
            for x in self._storage:
                if x not in seen:
                    seen.add(x)
                    out.append(x)
                    if x is None:
                        has_none = True
            if self._dtype is not None:
                return Vector(out, dtype=Schema(self._dtype.kind, has_none))
            return Vector(out)
        except TypeError:
            pass   # fall through → slow path

        # Slow path: unhashables
        out = []
        has_none = False
        for x in self._storage:
            if not any(x == y for y in out):
                out.append(x)
                if x is None:
                    has_none = True
        if self._dtype is not None:
            return Vector(out, dtype=Schema(self._dtype.kind, has_none))
        return Vector(out)


    def argsort(self):
        return [i for i, _ in sorted(enumerate(self._storage), key=lambda x: x[1])]

    def pluck(self, key, default=None):
        """Extract a key/index from each element, returning default if not found.
        
        Works with dicts, lists, tuples, strings, or any subscriptable object.
        """
        results = []
        for item in self._storage:
            # If item is None, can't subscript it
            if item is None:
                results.append(default)
                continue
            
            try:
                results.append(item[key])
            except (KeyError, IndexError, TypeError):
                # KeyError: dict key missing
                # IndexError: list/tuple index out of range
                # TypeError: item not subscriptable
                results.append(default)
        
        return Vector(results)

    def sort_by(self, reverse=False, na_last=True):
        """
        Stable sort. Returns a new Vector.

        Parameters
        ----------
        reverse : bool
            Sort in descending order if True
        na_last : bool
            If True, None sorts after all valid values.
            If False, None sorts before all valid values.

        Returns
        -------
        Vector
            Sorted vector with same dtype
        """
        storage = self._storage
        n = len(storage)

        # Build sort order from indices — avoids materializing Python objects
        # for the ArrayStorage case. is_null() is a direct mask check (no unboxing).
        if na_last:
            key_fn = lambda i: (storage.is_null(i), storage[i] if not storage.is_null(i) else 0)
        else:
            key_fn = lambda i: (0 if storage.is_null(i) else 1, storage[i] if not storage.is_null(i) else 0)

        order = sorted(range(n), key=key_fn, reverse=reverse)

        # Permute storage directly — no Vector() constructor, no type inference
        if isinstance(storage, ArrayStorage):
            from array import array as _array
            from .nullable import ByteMask
            tc = storage._data.typecode
            new_data = _array(tc, (storage._data[i] for i in order))
            if storage._mask is not None:
                new_mask = ByteMask.from_iterable(storage._mask.is_null(i) for i in order)
            else:
                new_mask = None
            new_storage = ArrayStorage(new_data, new_mask)
        else:
            new_storage = TupleStorage(tuple(storage._data[i] for i in order))

        # Bypass Vector.__new__ — dtype is invariant under sort
        return self._clone(new_storage)


    def _check_duplicate(self, other):
        if id(self) == id(other):
            return self.copy()
        return other


    def _check_native_typesafe(self, other):
        """ Ensure native type conversions (python) will not affect underlying type """
        dtype_kind = self._dtype.kind
        if not dtype_kind:
            return True
        if dtype_kind == type(other):
            return True
        if dtype_kind == Vector:
            return True
        if not (not self._dtype.nullable or isinstance(other, Iterable)):
            return True
        if dtype_kind == float and type(other) == int: # includes bool since isinstance(True, int) returns True
            return True
        if dtype_kind == complex and type(other) in (int, float): # ditto
            return True
        return False


    def __matmul__(self, other):
        """
        Universal Matrix Multiplication / Dot Product.
        Logic is centralized here to keep Table lightweight.
        
        Implements:
        1. Vector @ Vector -> Scalar (Dot Product)
        2. Matrix @ Vector -> Vector (Linear Combination)
        3. Matrix @ Matrix -> Matrix (Recursive columns)
        """
        other = self._check_duplicate(other)
        
        # === CASE 1: SELF IS MATRIX (2D) ===
        if self.ndims() == 2:
            
            # 1a. Matrix @ Matrix
            # Recursive: This Matrix @ Each Column of Other
            if hasattr(other, 'cols') and other.ndims() == 2:
                # Returns a tuple of vectors, wrapped in a new Vector (which becomes Table)
                result = self.copy(tuple(self @ col for col in other.cols()))
                result.name = None
                return result

            # 1b. Matrix @ Vector
            # The "Trick": Linear Combination of Columns
            # Result = sum(Column_i * Scalar_i)
            if isinstance(other, Vector):
                cols = self.cols() 
                
                if len(cols) != len(other):
                        raise SerifValueError(f"Dim mismatch: Matrix cols {len(cols)} != Vector len {len(other)}")
                
                # OPTIMIZATION: Access other._storage directly to avoid index overhead in loop
                scalars = other._storage
                
                if not cols:
                    return Vector([])

                # Start accumulator with first term (Col_0 * Scalar_0)
                acc = cols[0] * scalars[0]
                
                # Add remaining terms
                for i in range(1, len(cols)):
                    acc = acc + (cols[i] * scalars[i])
                
                return acc

        # === CASE 2: SELF IS VECTOR (1D) ===
        
        # 2a. Vector @ Matrix
        # Broadcast self against columns of matrix
        if hasattr(other, 'cols') and other.ndims() == 2:
                return Vector(tuple(self @ col for col in other.cols()))

        # 2b. Vector @ Vector (Dot Product)
        # Standard sum of products
        if len(self) != len(other):
            raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
        return sum(x*y for x, y in zip(self._storage, other._storage, strict=True))

    def __rmatmul__(self, other):
        other = self._check_duplicate(other)
        if len(self.shape) > 1:
            return Vector(tuple(x @ other for x in self.cols()))
        if len(self) != len(other):
            raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
        return sum(x*y for x, y in zip(self._storage, other._storage, strict=True))
        raise SerifTypeError(f"Unsupported operand type(s) for '*': '{self._dtype.__name__}' and '{type(other).__name__}'.")


    def __bool__(self):
        """
        Raises an error because using a vector in a boolean context is ambiguous.
        
        Users often mistakenly use 'if vec' when they mean 'if vec.any()' or 'if vec.all()'.
        Use len(vec) > 0 to check for emptiness.
        """
        raise TypeError(
            "Vector cannot be used in a boolean context (e.g., 'if vector:'). "
            "Use .any() or .all() for element-wise checks, or len(vector) > 0 to check for emptiness."
        )


    def __lshift__(self, other):
        """ The << operator behavior has been overridden to attempt to concatenate (append) the new array to the end of the first
        """
        if self._dtype.kind in (bool, int) and isinstance(other, int):
            warnings.warn(f"The behavior of >> and << have been overridden for concatenation. Use .bitshift() to shift bits.")

        nullable = self._dtype.nullable if self._dtype is not None else True
        if isinstance(other, Vector):
            if not self._dtype.nullable and not other.schema().nullable and self._dtype.kind != other.schema().kind:
                raise SerifTypeError("Cannot concatenate two typesafe Vectors of different types")
            return self._clone(self._build_storage(chain(self._storage, other._storage), nullable))
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            return self._clone(self._build_storage(chain(self._storage, other), nullable))
        return self._clone(self._build_storage(chain(self._storage, (other,)), nullable))


    def __rshift__(self, other):
        """ The >> operator behavior has been overridden to add the column(s) of other to self
        """
        if self._dtype.kind in (bool, int) and isinstance(other, int):
            warnings.warn(f"The behavior of >> and << have been overridden for concatenation. Use .bitshift() to shift bits.")

        if type(other).__name__ == 'Table':
            return Vector((self,) + other.cols())
        if isinstance(other, Vector):
            return Vector((self,) + (other,))
        if isinstance(other, dict):
            cols = [self]
            for k, v in other.items():
                if not isinstance(v, Vector):
                    v = Vector(v)
                cols.append(v.alias(k) if v._name != k else v)
            return Vector(cols)
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            return Vector([self, Vector(tuple(x for x in other))])
        elif not self:
            return Vector((other,),
                dtype=self._dtype)
        raise SerifTypeError("Cannot add a column of constant values. Try using Vector.new(element, length).")

    def __rlshift__(self, other):
        """ The << operator behavior has been overridden to attempt to concatenate (append)
        Handles: other << self (where other is not a Vector)
        """
        # Convert other to Vector and concatenate with self
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            return Vector(chain(other, self._storage))
        # Scalar case: [other] + self
        return Vector(chain((other,), self._storage))

    def __rrshift__(self, other):
        """ The >> operator behavior has been overridden to add columns
        Handles: other >> self (where other is not a Vector)
        Creates a table with other as first column(s) and self as additional column(s)
        """
        # Convert other to Vector and combine column-wise
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            return Vector((Vector(tuple(other)), self),
                None,
                None,
                False)
        # Scalar case: create a single-element vector for other
        return Vector((Vector((other,)), self),
            None,
            None,
            False)

    
    def bit_lshift(self, other):
        """
        Bitwise left shift (<<).
        Explicit method since '<<' operator is used for concatenation.
        """
        return self._elementwise_operation(other, operator.lshift, 'bit_lshift', '<<')

    def bit_rshift(self, other):
        """
        Bitwise right shift (>>).
        Explicit method since '>>' operator is used for column addition.
        """
        return self._elementwise_operation(other, operator.rshift, 'bit_rshift', '>>')


    def __getattr__(self, name):
        """Proxy attribute access to underlying dtype.
        
        Distinguishes between properties (like date.year) and methods (like str.replace):
        - Properties are evaluated immediately and return a Vector
        - Methods return a MethodProxy that waits for () to be called
        """
        # 1. If we are untyped (object), don't guess. Explicit > Implicit.
        # Use __dict__ to avoid recursive __getattr__ calls
        schema = object.__getattribute__(self, 'schema')()
        if schema is None:
            raise AttributeError(f"Empty Vector has no attribute '{name}'")
        dtype_kind = schema.kind
        if dtype_kind is object:
            raise AttributeError(f"Vector[object] has no attribute '{name}'")
        
        # 2. Inspect the class definition of the type we are holding
        # getattr(cls, name) returns the actual class member (method, property, slot)
        cls_attr = getattr(dtype_kind, name, None)
        
        if cls_attr is None:
            # If the class doesn't have it, we definitely don't have it
            raise AttributeError(f"'{dtype_kind.__name__}' object has no attribute '{name}'")
        
        # 3. Check if it's callable at the class level
        # If it's callable, it's a method. If not, it's a property/descriptor.
        if callable(cls_attr):
            # It's a method -> Return the proxy to wait for ()
            return MethodProxy(self, name)
        else:
            # property (non-callable attribute)
            return Vector(tuple(
                getattr(x, name) if x is not None else None
                for x in self._storage
            ))

