import operator
import warnings
import math
import hashlib
import struct
from builtins import isinstance as b_isinstance
from collections.abc import Iterable
from decimal import Decimal

from ..errors import SerifTypeError
from ..errors import SerifIndexError
from ..errors import SerifValueError
from ..errors import SerifKeyError
from ..errors import SerifEmptyReductionError
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
from typing import List

# ============================================================
# Reverse arithmetic operation helpers
# ============================================================
def _reverse_add(y, x):
    return x + y

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


# ============================================================
# Kleene three-valued logic (see docs/null-semantics.md)
# The known operand may settle the result; otherwise unknown propagates.
# ============================================================

def _kleene_and(x, y):
    if x is not None and not x:
        return False
    if y is not None and not y:
        return False
    if x is None or y is None:
        return None
    return True


def _kleene_or(x, y):
    if x is not None and x:
        return True
    if y is not None and y:
        return True
    if x is None or y is None:
        return None
    return False


def _kleene_xor(x, y):
    # No settling operand for xor: unknown with anything is unknown.
    if x is None or y is None:
        return None
    return bool(x) != bool(y)


# Accelerator dispatch: only THESE functions have a vectorized twin —
# anything else passed to _logical_elementwise declines to the pure zip.
_KLEENE_OP_NAMES = {_kleene_and: 'and', _kleene_or: 'or', _kleene_xor: 'xor'}


def _check_on_empty(method_name, on_empty):
    # Identity checks, not truthiness: on_empty=1 is a bug, not a True.
    if on_empty is None or on_empty is True or on_empty is False:
        return
    raise SerifTypeError(
        f"{method_name}(): on_empty must be True or False (or None, the "
        f"default, which raises on zero valid values); got {on_empty!r}"
    )

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

def _null_sort_flag(is_null: bool, reverse: bool, na_last: bool) -> bool:
    """
    Sort-key flag that places nulls last (na_last=True) or first for BOTH
    sort directions: the flag flips with `reverse` so the nulls' final
    position is direction-independent. Shared by Vector.sort_by,
    _Category.sort_by, and Table.sort_by — one rule, three callers.
    """
    return (is_null != reverse) if na_last else (is_null == reverse)


def _fingerprint_frame(tag: bytes, payload: bytes = b'') -> bytes:
    """Return an unambiguous binary frame for fingerprint encoding."""
    return tag + len(payload).to_bytes(8, 'big') + payload


def _fingerprint_encode(value: Any) -> bytes:
    """Canonical, cross-process encoding for supported Python values.

    Deliberately raises for unknown objects instead of falling back to repr(),
    which commonly embeds memory addresses and would make a persistent DAG key
    look deterministic while changing between processes.
    """
    if value is None:
        return _fingerprint_frame(b'n')
    if type(value) is bool:
        return _fingerprint_frame(b'b', b'1' if value else b'0')
    if type(value) is int:
        return _fingerprint_frame(b'i', str(value).encode('ascii'))
    if type(value) is float:
        if math.isnan(value):
            payload = b'nan'
        elif math.isinf(value):
            payload = b'+inf' if value > 0 else b'-inf'
        else:
            # Python considers -0.0 and 0.0 equal; canonicalize accordingly.
            payload = struct.pack('>d', 0.0 if value == 0.0 else value)
        return _fingerprint_frame(b'f', payload)
    if type(value) is complex:
        return _fingerprint_frame(
            b'c', _fingerprint_encode(value.real) + _fingerprint_encode(value.imag))
    if type(value) is str:
        return _fingerprint_frame(b's', value.encode('utf-8'))
    if type(value) is bytes:
        return _fingerprint_frame(b'y', value)
    if type(value) is bytearray:
        return _fingerprint_frame(b'a', bytes(value))
    if isinstance(value, datetime):
        payload = value.isoformat(timespec='microseconds').encode('utf-8')
        payload += bytes((value.fold,))
        return _fingerprint_frame(b'z', payload)
    if isinstance(value, date):
        return _fingerprint_frame(b'd', value.isoformat().encode('ascii'))
    if isinstance(value, timedelta):
        payload = (
            _fingerprint_encode(value.days)
            + _fingerprint_encode(value.seconds)
            + _fingerprint_encode(value.microseconds)
        )
        return _fingerprint_frame(b't', payload)
    if isinstance(value, Decimal):
        decimal_tuple = value.as_tuple()
        payload = (
            _fingerprint_encode(decimal_tuple.sign)
            + _fingerprint_encode(decimal_tuple.digits)
            + _fingerprint_encode(decimal_tuple.exponent)
        )
        return _fingerprint_frame(b'm', payload)
    if isinstance(value, Vector):
        return _fingerprint_frame(b'v', bytes.fromhex(value.fingerprint()))
    if type(value) in (list, tuple):
        tag = b'l' if type(value) is list else b'q'
        return _fingerprint_frame(
            tag, b''.join(_fingerprint_encode(v) for v in value))
    if type(value) in (set, frozenset):
        tag = b'e' if type(value) is set else b'r'
        encoded = sorted(_fingerprint_encode(v) for v in value)
        return _fingerprint_frame(tag, b''.join(encoded))
    if isinstance(value, dict):
        encoded = [(_fingerprint_encode(k), _fingerprint_encode(v))
                   for k, v in value.items()]
        encoded.sort(key=lambda pair: pair[0])
        payload = b''.join(k + v for k, v in encoded)
        return _fingerprint_frame(b'g', payload)
    raise SerifTypeError(
        "fingerprint() does not know how to encode "
        f"{type(value).__module__}.{type(value).__qualname__}; cast the "
        "value to a supported Python type first."
    )


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


def _elementwise_proxy(method_name):
    """
    Build a per-element proxy method: None passes through, every other
    element has `method_name` called on it, results collected into a Vector.

    Used by the typed subclasses (_String, _Date) to stamp element methods
    onto the class at definition time — same semantics as the MethodProxy
    that Vector.__getattr__ falls back to, but visible to dir() and
    tab-completion.
    """
    def proxy(self, *args, **kwargs):
        return Vector(tuple(
            (getattr(s, method_name)(*args, **kwargs) if s is not None else None)
            for s in self._storage
        ))
    proxy.__name__ = method_name
    proxy.__doc__ = f"Element-wise {method_name}() on each value (None passes through)."
    return proxy


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
    saw_none = False
    saw_vector = False

    for val in iterable:
        data.append(val)
        if isinstance(val, Vector):
            # Columns don't inform a SCALAR dtype: running promote_dtype on
            # them fires the object-degrade warning for every mixed-kind
            # table composed via >> (the "column" it degrades is a phantom
            # — the 1-D reading abandoned once is_table comes back True).
            saw_vector = True
            continue
        all_vectors = False
        if dtype is None:
            # Order-independent inference: a leading None only sets nullable;
            # the kind comes from the first non-None value.
            if val is None:
                saw_none = True
                continue
            dtype = Schema(infer_kind(val), saw_none)
        else:
            dtype = promote_dtype(dtype, val)

    if saw_vector and not all_vectors:
        # Degenerate mix of columns and scalars in one collection — not a
        # table, so the Vectors ARE elements after all. Re-infer over
        # everything so promotion (and its degrade warning) fires here,
        # where it's real. Rare path; the second walk costs nothing.
        dtype = dtype_hint
        saw_none = False
        for val in data:
            if dtype is None:
                if val is None:
                    saw_none = True
                    continue
                dtype = Schema(infer_kind(val), saw_none)
            else:
                dtype = promote_dtype(dtype, val)

    if dtype is None and saw_none:
        # All values were None: no kind to infer.
        dtype = Schema(object, True)

    return data, all_vectors, dtype


def _storage_for_dtype(dtype, data, nullable):
    """
    Build the storage backend appropriate for a Schema. Mirrors the subclass
    _build_storage dispatch, but keyed on dtype rather than instance class —
    used by __setitem__ rebuilds, where an in-place kind promotion can leave
    the instance class behind the new dtype.
    """
    kind = dtype.kind if dtype is not None else None
    if kind is int:
        try:
            return ArrayStorage.from_iterable(data, typecode='q', nullable=nullable)
        except OverflowError:
            # Arbitrary-precision ints don't fit i64 — values stay exact in
            # a tuple backend (never truncate; Python semantics first).
            return TupleStorage.from_iterable(data, nullable=nullable)
    if kind is float:
        return ArrayStorage.from_iterable(data, typecode='d', nullable=nullable)
    if kind is str:
        from .storage import StringStorage
        return StringStorage.from_iterable(data)
    return TupleStorage.from_iterable(data, nullable=nullable)


def _accel_filter(storage, mask):
    """Numpy-accelerated boolean-mask filter; None = decline to the pure
    path, whose behavior is the specification. OPTIONAL numpy — transport,
    never semantics; see serif/_accel/__init__.py for the doctrine."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .._accel.mask import filter_storage
    return filter_storage(storage, mask)


def _accel_popcount(mask_storage):
    """Numpy-accelerated True count of a boolean mask (nulls count False);
    None = decline to the pure count, whose behavior is the specification.
    OPTIONAL numpy — transport, never semantics; see serif/_accel/__init__.py."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .._accel.mask import popcount_storage
    return popcount_storage(mask_storage)


def _accel_take(storage, indices):
    """Numpy-accelerated positional gather; None = decline to the pure
    storage.take(), whose behavior is the specification. OPTIONAL numpy —
    transport, never semantics; see serif/_accel/__init__.py."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .._accel.mask import take_storage
    return take_storage(storage, indices)


def _take(storage, indices):
    """storage.take() behind the accelerator: numpy gather when the backend
    is supported, the protocol's pure take() otherwise."""
    fast = _accel_take(storage, indices)
    return fast if fast is not None else storage.take(indices)


def _accel_take_pad(storage, indices):
    """Numpy-accelerated gather where index -1 emits null (join pad rows);
    None = decline to the caller's pure emission, whose behavior is the
    specification."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .._accel.mask import take_pad_storage
    return take_pad_storage(storage, indices)


def _accel_group(storage):
    """Accelerated single-key bucketing ({(key,): [rows]} in first-
    appearance order); None = decline to the pure dict loop, whose
    behavior is the specification. numpy buckets int64 keys from buffer
    math; string keys ride arrow's hash kernel into the same math
    (serif/_accel/arrow.py, which gates on both switches itself)."""
    from .. import _accel
    fast = None
    if _accel._USE_NUMPY:
        from .._accel.group import group_indices
        fast = group_indices(storage)
    if fast is None:
        from .._accel.arrow import group_strings
        fast = group_strings(storage)
    return fast


def _accel_join_probe(left_storage, right_storage,
                      expect_left_unique, expect_right_unique,
                      keep_unmatched_left, keep_unmatched_right):
    """Accelerated single-key join probe. Returns a tagged tuple (see
    serif/_accel/join.py) or None = decline to the pure matcher, whose
    behavior is the specification. numpy probes int64 keys; string keys
    encode through arrow into the same probe core (serif/_accel/arrow.py,
    which gates on both switches itself)."""
    from .. import _accel
    fast = None
    if _accel._USE_NUMPY:
        from .._accel.join import probe_int64_dense
        fast = probe_int64_dense(
            left_storage, right_storage,
            expect_left_unique, expect_right_unique,
            keep_unmatched_left, keep_unmatched_right)
    if fast is None:
        from .._accel.arrow import join_probe_strings_hash
        fast = join_probe_strings_hash(
            left_storage, right_storage,
            expect_left_unique, expect_right_unique,
            keep_unmatched_left, keep_unmatched_right)
    if fast is None and _accel._USE_NUMPY:
        from .._accel.join import probe_int64
        fast = probe_int64(left_storage, right_storage,
                           expect_left_unique, expect_right_unique,
                           keep_unmatched_left, keep_unmatched_right)
    if fast is None:
        from .._accel.arrow import join_probe_strings
        fast = join_probe_strings(left_storage, right_storage,
                                  expect_left_unique, expect_right_unique,
                                  keep_unmatched_left, keep_unmatched_right)
    return fast


def _accel_reduce(storage, op, **kwargs):
    """Try a numpy-accelerated reduction. Returns (True, value) when the
    fast path produced the answer, (False, None) on decline — the caller
    runs the pure path, whose behavior is the specification. None is a
    legitimate value (max of all-null), hence the flag."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return False, None
    from .._accel import reduce as _reduce
    result = getattr(_reduce, op)(storage, **kwargs)
    if result is _accel.DECLINED:
        return False, None
    return True, result


def _accel_binop(storage, rhs, op_func, result_dtype):
    """Accelerated elementwise arithmetic; None = decline. The schema is
    already resolved by _pre_compute_op_schema — the accelerator computes
    values, never semantics. Returns a Vector or None.

    Ordering: TRUE DIVISION tries arrow first — its checked kernel skips
    null lanes natively, so the numpy tier's neutralize-divisors copy
    and zero-scan never run (identical results, fewer passes). Everything
    else runs numpy first; int lanes its overflow bounds pass declined
    (it must over-predict) get arrow's CHECKED kernels, which decline
    only on actual overflow (serif/_accel/arrow.py)."""
    from .. import _accel
    fast = None
    if op_func is operator.truediv:
        from .._accel.arrow import div_floats
        fast = div_floats(storage, rhs, op_func, result_dtype.kind)
    if fast is None and _accel._USE_NUMPY:
        from .._accel.ops import binop_storage
        fast = binop_storage(storage, rhs, op_func, result_dtype.kind)
    if fast is None:
        from .._accel.arrow import binop_ints
        fast = binop_ints(storage, rhs, op_func, result_dtype.kind)
    if fast is None:
        return None
    result = Vector._from_storage(fast, result_dtype)
    result._wild = True   # match the pure constructors' name-tracking flag
    return result


def _accel_compare(storage, rhs, op_func, nullable):
    """Accelerated elementwise comparison; None = decline to the pure
    path, whose behavior is the specification. Both backends get a try
    (see serif/_accel/__init__.py): numpy for fixed-width lanes, then
    arrow for the string content numpy cannot see."""
    from .. import _accel
    fast = None
    if _accel._USE_NUMPY:
        from .._accel.ops import compare_storage
        fast = compare_storage(storage, rhs, op_func)
    if fast is None:
        from .._accel.arrow import compare_strings
        fast = compare_strings(storage, rhs, op_func)
    if fast is None:
        return None
    result = Vector._from_storage(fast, Schema(bool, nullable))
    result._wild = True
    return result


def _accel_logical(storage, rhs, op_name):
    """Numpy-accelerated Kleene &/|/^; None = decline to the pure zip,
    whose behavior is the specification. Nullability is post-hoc like the
    pure path's `any(v is None)` — the mask-None convention makes the two
    agree exactly."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .._accel.ops import logical_storage
    fast = logical_storage(storage, rhs, op_name)
    if fast is None:
        return None
    result = Vector._from_storage(fast, Schema(bool, fast._mask is not None))
    result._wild = True
    return result


def _accel_invert(storage, nullable):
    """Numpy-accelerated Kleene NOT; None = decline. nullable is the
    schema-carried flag — __invert__ preserves the input schema rather
    than recomputing it post-hoc."""
    from .. import _accel
    if not _accel._USE_NUMPY:
        return None
    from .._accel.ops import invert_storage
    fast = invert_storage(storage)
    if fast is None:
        return None
    result = Vector._from_storage(fast, Schema(bool, nullable))
    result._wild = True
    return result


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
    _wild = False  # Flag for name changes (used by Table column tracking)
    _ndims = 1     # Class-level constant; Table overrides with 2
    # Mutation doctrine: read through the column, write through the table.
    # A table-owned column is FROZEN (__setitem__ raises; write via
    # t[key, 'col'] = value, which swaps in a fresh column). _inplace_ok is
    # set only inside a batch() scope, after copy-on-enter privatized the
    # buffers — it licenses raw in-place writes that would corrupt shared
    # storage anywhere else. Standalone vectors: unfrozen, rebuild-on-write.
    _frozen = False
    _inplace_ok = False
    
    def schema(self):
        """Get the Schema (kind, nullable) of this vector."""
        return self._dtype


    def __new__(cls, initial=(), dtype=None, name=None, **kwargs):
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
                return Table(initial=data, dtype=dtype, name=name)
            from ..errors import SerifValueError
            raise SerifValueError('Passing vectors of different length will not produce a Table.')

        # Dispatch to the right subclass
        target_class = _pick_target_class(dtype)

        # Allocate and fully initialize — __init__ will see _storage and return early
        instance = object.__new__(target_class)
        instance._dtype = dtype
        instance._name = name
        instance._wild = True
        nullable = dtype.nullable if dtype is not None else True
        instance._storage = instance._build_storage(data, nullable)
        return instance


    def __init__(self, initial=(), dtype=None, name=None, **kwargs):
        # Factory path: __new__ already built the instance fully.
        if '_storage' in self.__dict__:
            return

        # Subclass path: Table.__init__ → super().__init__(), or other subclass
        # direct construction. self._dtype may already be set by the subclass.
        self._name = name
        self._wild = True
        if dtype is not None:
            if not isinstance(dtype, Schema):
                dtype = Schema(dtype, False)
            self._dtype = dtype
        nullable = self._dtype.nullable if self._dtype is not None else True
        self._storage = self._build_storage(initial, nullable)

    def _build_storage(self, data, nullable):
        tc = getattr(self, 'typecode', None)
        if tc is not None:
            return ArrayStorage.from_iterable(data, typecode=tc, nullable=nullable)
        # Route str and bool columns to their compact buffer backends
        if getattr(self, '_dtype', None) is not None and self._dtype.kind is str:
            from .storage import StringStorage
            return StringStorage.from_iterable(data)
        if getattr(self, '_dtype', None) is not None and self._dtype.kind is bool:
            from .storage import BoolStorage
            return BoolStorage.from_iterable(data, nullable=nullable)
        return TupleStorage.from_iterable(data, nullable=nullable)

    def _clone(self, new_storage, dtype=..., name=...):
        """
        Fastest possible copy: bypass __new__, __init__, and all inference.

        Use when the output dtype and subclass are already known — e.g. after
        a sort, permutation, or any op where the type cannot change.

        dtype and name default to self's values (sentinel ... means "keep").
        Pass explicit values to override.

        Construction-path contract (which builder to use when)
        -------------------------------------------------------
        Vector(...)                    — public: full inference + class
                                         dispatch; the only path that may
                                         return a Table.
        copy()                         — same subclass & schema, name kept
                                         by default; storage SHARED (rebuild-
                                         on-write keeps copies independent),
                                         rebuilt only when new_values given.
        _clone(storage)                — SAME subclass, dtype/name kept:
                                         permutations of existing data only.
                                         Never changes the element kind.
        _from_storage(storage, dtype)  — class picked FROM dtype, unnamed
                                         wildness off: I/O fast paths with a
                                         fully-known prebuilt backend.
        _from_iterable_known_dtype(it, dtype)
                                       — class picked from dtype, one walk
                                         into storage, zero inference:
                                         derived results whose schema is
                                         known before materialization.
        """
        instance = object.__new__(type(self))
        instance._dtype = self._dtype if dtype is ... else dtype
        instance._name = self._name if name is ... else name
        instance._wild = True
        instance._storage = new_storage
        return instance

    @classmethod
    def _from_storage(cls, storage, dtype, name=None):
        """Create a Vector directly from a pre-built storage object.
        Zero iterations, zero inference — for I/O fast paths where storage
        and dtype are fully known (e.g. Parquet DOUBLE/INT64 non-nullable)."""
        target_cls = _pick_target_class(dtype) if dtype is not None else cls
        instance = object.__new__(target_cls)
        instance._dtype = dtype
        instance._name = name
        instance._wild = False
        instance._storage = storage
        return instance

    @classmethod
    def _from_iterable_known_dtype(cls, iterable, dtype, *, name=None):
        """Build a Vector directly from an iterable when the dtype is already known.
        Bypasses __new__/__init__ inference; one walk into storage."""
        target_cls = _pick_target_class(dtype)
        instance = object.__new__(target_cls)
        instance._dtype = dtype
        instance._name = name
        instance._wild = True
        nullable = dtype.nullable if dtype is not None else True
        instance._storage = instance._build_storage(iterable, nullable)
        return instance


    @property
    def shape(self):
        if not self._storage:
            return tuple()
        return (len(self),)

    @property
    def vector_name(self):
        """Get the name of this vector.

        Named `vector_name`, not `name`, so that a column literally called
        'name' — the single most common column name — is NOT shadowed by this
        property on a Table. See Table.table_name for the table-level counterpart.
        """
        return self._name

    @vector_name.setter
    def vector_name(self, new_name):
        """Set the name of this vector."""
        self._require_mutable_metadata()
        self._name = new_name
        self._wild = True  # Mark as wild when renamed

    #-----------------------------------------------------
    # Fingerprinting
    #-----------------------------------------------------

    def fingerprint(self) -> str:
        """Return a deterministic identity for values and analytical schema.

        The digest is stable across Python processes and includes dimensions,
        names, dtypes, nullability, categorical order, decimal metadata, and
        values. The result is a 64-character BLAKE2b hex string.

        Fingerprints are intentionally recomputed on each call. This keeps
        metadata changes visible without coupling identity correctness to
        mutation-path cache invalidation.
        """
        digest = hashlib.blake2b(digest_size=32, person=b'serif-fp-v1')

        if self.ndims() == 2:
            columns = self.cols()
            digest.update(_fingerprint_frame(b'T'))
            digest.update(_fingerprint_encode(getattr(self, '_name', None)))
            digest.update(_fingerprint_encode(len(self)))
            digest.update(_fingerprint_encode(len(columns)))
            for column in columns:
                digest.update(bytes.fromhex(column.fingerprint()))
            return digest.hexdigest()

        schema = self.schema()
        kind = schema.kind
        kind_name = f"{kind.__module__}.{kind.__qualname__}"
        digest.update(_fingerprint_frame(b'V'))
        digest.update(_fingerprint_encode(getattr(self, '_name', None)))
        digest.update(_fingerprint_encode(len(self)))
        digest.update(_fingerprint_encode(kind_name))
        digest.update(_fingerprint_encode(schema.nullable))

        categories = getattr(self, '_categories', None)
        digest.update(_fingerprint_encode(categories))

        storage = self._storage
        decimal_meta = None
        if hasattr(storage, '_scale') and hasattr(storage, '_precision'):
            decimal_meta = (storage._scale, storage._precision)
        digest.update(_fingerprint_encode(decimal_meta))

        for value in storage:
            digest.update(_fingerprint_encode(value))
        return digest.hexdigest()

    @classmethod
    def filled(cls, value, length, typesafe=False):
        """Create a vector of `length` copies of `value` (a fill-constructor).

        Example: ``Vector.filled(0.5, length=15)`` → fifteen 0.5s.
        A length of 0 yields an empty vector that still carries `value`'s dtype.
        """
        if length:
            assert isinstance(length, int)
            dtype = infer_dtype([value])
            if typesafe:
                dtype = Schema(dtype.kind, False)
            return cls([value for _ in range(length)], dtype=dtype)
        dtype = infer_dtype([value]) if value is not None else Schema(object, False)
        if typesafe:
            dtype = Schema(dtype.kind, False)
        return cls(dtype=dtype)


    def copy(self, new_values=None, name=...):
        """
        Snapshot of this vector. With no new_values this is O(1): the frozen
        storage object is SHARED, not duplicated — the storage protocol is
        rebuild-only (storage.py), so mutating either vector rebinds it a NEW
        storage and the other never sees the write. Pass new_values to build
        a same-schema vector from different data (storage rebuilt).
        """
        use_name = self._name if name is ... else name
        if self._dtype is not None:
            if new_values is None:
                return self._clone(self._storage, name=use_name)
            # Typed 1D vector with replacement values: build storage
            # directly, no inference needed.
            return self._clone(self._build_storage(new_values, self._dtype.nullable), name=use_name)
        # dtype unknown (Table or untyped vector): full constructor for class
        # dispatch. Explicit None check — an empty new_values means "empty
        # copy", not "copy the original".
        source = list(new_values) if new_values is not None else list(self._storage)
        return Vector(source, dtype=None, name=use_name)
    
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
        Set this vector's name and return self, for chaining.

        The chainable counterpart to the `.vector_name` setter — use it inside
        expressions, e.g. `(a * 2).alias('twice_a')` or
        `Table([v.alias('x'), ...])`. Works whether or not the vector is
        already named (it just sets the name).
        """
        self._require_mutable_metadata()
        self._name = new_name
        self._wild = True  # Mark as wild when (re)named
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
                raise SerifValueError(
                    f"Cast failed at index {i}: {elem!r} cannot be converted to {type_name}"
                ) from exc

        # Now decide dtype using the *logical* type, not the callable
        if isinstance(py_target_type, type):
            new_dtype = Schema(py_target_type, has_none)
        else:
            new_dtype = infer_dtype(out)

        return Vector(out, dtype=new_dtype, name=self._name)

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
                    )
                except SerifTypeError:
                    raise SerifValueError(
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
        new_dtype = Schema(self._dtype.kind, False) if self._dtype is not None else None
        kept = [i for i in range(len(storage)) if not storage.is_null(i)]
        return self._clone(_take(storage, kept), dtype=new_dtype)

    def is_na(self):
        """
        Return boolean mask of None values.
        
        Returns
        -------
        Vector
            Boolean vector, True where value is None
        
        Examples
        --------
        >>> v = Vector([1, None, 3])
        >>> v.is_na()
        Vector([False, True, False])
        """
        storage = self._storage
        from .storage import StringStorage, BoolStorage
        if isinstance(storage, (ArrayStorage, StringStorage, BoolStorage)):
            # Fast path: iterate the null mask directly — it already knows
            # which slots are null (yields True per null), so we avoid
            # unboxing numerics / decoding UTF-8 just to test None. If there
            # is no mask, no elements are null. Built via _from_storage (not
            # _clone) so the result is a plain unnamed bool Vector, per the
            # naming invariant for derived vectors.
            if storage._mask is None:
                result = BoolStorage(bytearray(len(storage)))
            else:
                result = BoolStorage(bytearray(
                    1 if is_null else 0 for is_null in storage._mask))
            return Vector._from_storage(result, Schema(bool, False))
        return Vector._from_iterable_known_dtype(
            (elem is None for elem in storage),
            Schema(bool, False),
        )

    def is_type(self, types):
        """
        Boolean mask: True where each element is an instance of the given
        type(s). Uses isinstance semantics — subclasses count (e.g. a bool
        element matches int).
        
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
        >>> v.is_type(int)
        Vector([True, False, False, False])
        >>> v.is_type((int, float))
        Vector([True, False, True, False])
        >>> v.is_type(type(None))
        Vector([False, False, False, True])
        """
        return Vector._from_iterable_known_dtype(
            (b_isinstance(elem, types) for elem in self._storage),
            Schema(bool, False),
        )

    def __iter__(self):
        """ iterate over the underlying tuple """
        return iter(self._storage)

    def __len__(self):
        """ length of the underlying tuple """
        return len(self._storage)

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
            try:
                return self._storage[key]
            except IndexError:
                raise SerifIndexError(
                    f"Index {key} out of range for vector length {len(self)}"
                ) from None

        if isinstance(key, tuple):
            # 1-D vectors accept only single-element tuples; deeper indexing
            # belongs to Table (nested vectors are not a supported shape).
            if len(key) != len(self.shape):
                raise SerifKeyError(f"Matrix indexing must provide an index in each dimension: {self.shape}")
            return self[key[0]]

        key = self._check_duplicate(key)
        if isinstance(key, Vector) and key.schema().kind == bool:
            # Nullable masks are allowed: a null entry EXCLUDES the row
            # (SQL WHERE semantics — None is falsy in the filter below).
            if len(self) != len(key):
                raise SerifValueError(f"Boolean mask length mismatch: {len(self)} != {len(key)}")
            fast = _accel_filter(self._storage, key._storage)
            if fast is not None:
                return self._clone(fast)
            return self.copy((x for x, y in zip(self, key, strict=True) if y), name=self._name)
        if isinstance(key, list) and {type(e) for e in key} == {bool}:
            if len(self) != len(key):
                raise SerifValueError(f"Boolean mask length mismatch: {len(self)} != {len(key)}")
            fast = _accel_filter(self._storage, key)
            if fast is not None:
                return self._clone(fast)
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


    def _require_mutable(self):
        """Raise if this vector is a frozen table-owned column.

        The doctrine: read through the column, write through the table.
        A vector read out of a table is a value — it cannot mutate the
        table (and the table cannot mutate it back: owner writes swap in
        a fresh column object). Mutation is owner-addressed.
        """
        if self._frozen:
            col = self._name if self._name is not None else 'col'
            raise SerifTypeError(
                "Read-out columns are values: this vector is owned by a "
                "Table and is frozen. Write through the owning table "
                "instead:\n"
                f"    t[key, {col!r}] = value\n"
                "For an independent mutable vector use .copy(); for bulk "
                "point-write loops use `with t.batch() as m:`."
            )

    def _require_mutable_metadata(self):
        """Reject schema mutation through a table-owned column."""
        if self._frozen:
            col = self._name if self._name is not None else 'col'
            raise SerifTypeError(
                "Read-out columns are values: this vector is owned by a "
                "Table and its metadata is frozen. Rename through the table "
                "instead:\n"
                f"    t = t.rename({{{col!r}: 'new_name'}})\n"
                "For an independent renameable vector use .copy()."
            )

    def __setitem__(self, key, value):
        """Public assignment: frozen table-owned columns raise (write
        through the table — see _require_mutable); everything else
        delegates to _setitem_impl."""
        self._require_mutable()
        self._setitem_impl(key, value)

    def _setitem_impl(self, key, value):
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

        updates = []  # list of (idx, new_value)

        # =====================================================================
        # CASE 1 — Boolean mask (fast-path)
        # =====================================================================
        if (
            isinstance(key, Vector) and key.schema().kind == bool
        ) or (
            isinstance(key, list) and all(isinstance(e, bool) for e in key)
        ):
            if len(key) != n:
                raise SerifValueError("Boolean mask length must match vector length.")

            # Precompute true indices (much faster than branch-per-element).
            # A null mask entry assigns nothing (None is falsy here) — SQL
            # WHERE semantics, see docs/null-semantics.md.
            true_indices = [i for i, flag in enumerate(key) if flag]
            tcount = len(true_indices)

            if is_seq_val:
                if tcount != len(value):
                    raise SerifValueError(
                        "Iterable length must match number of True mask elements."
                    )
                for idx, v in zip(true_indices, value):
                    updates.append((idx, v))
            else:
                for idx in true_indices:
                    updates.append((idx, value))

        # =====================================================================
        # CASE 2 — Slice assignment
        # =====================================================================
        elif isinstance(key, slice):
            start, stop, step = key.indices(n)
            slice_len = len(range(start, stop, step))

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
                updates.append((idx, new_val))

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
            updates.append((key, value))

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
                    updates.append((idx, val))
            else:
                for idx in key:
                    if idx < 0:
                        idx += n
                    if not (0 <= idx < n):
                        raise SerifIndexError(f"Index {idx} out of range.")
                    updates.append((idx, value))

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
                    updates.append((idx, val))
            else:
                for idx in key:
                    if idx < 0:
                        idx += n
                    if not (0 <= idx < n):
                        raise SerifIndexError(f"Index {idx} out of range.")
                    updates.append((idx, value))

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
                saw_none = False
                for val in new_values:
                    if val is None:
                        # Assigning None widens a non-nullable schema to
                        # nullable — the column is typed "X", not "X and
                        # never absent". (A strict never-null column mode
                        # would raise here; that is a future concept, not
                        # today's default.)
                        saw_none = True
                        continue
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

                if saw_none and not self._dtype.nullable:
                    self._dtype = Schema(self._dtype.kind, True)
        # =====================================================================
        # MUTATE — in-place fast path (batch() scope only), else rebuild
        # =====================================================================
        # Raw writes are legal only inside a batch() scope: copy-on-enter
        # privatized the buffers, so nothing else can observe them. The
        # storage declines (False) anything it can't hold — including after
        # an in-place kind promotion, which swapped in a TupleStorage with
        # no write_inplace at all — and the rebuild path below remains the
        # specification.
        if self._inplace_ok and updates:
            write = getattr(self._storage, 'write_inplace', None)
            if write is not None and write(updates):
                return

        data_list = list(underlying)           # COW materialization

        for idx, new_val in updates:
            data_list[idx] = new_val

        # Rebuild through a dtype-keyed dispatch (not the instance class):
        # typed vectors keep their backend across mutation — an int column
        # stays ArrayStorage('q') and remains e.g. Parquet-writable — and the
        # dispatch stays correct even right after an in-place kind promotion,
        # when the instance class lags the new dtype.
        nullable = self._dtype.nullable if self._dtype is not None else True
        self._storage = _storage_for_dtype(self._dtype, data_list, nullable)



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
        
        # Element-wise: unknown in, unknown out (docs/null-semantics.md).
        if isinstance(other, Vector):
            if len(self) != len(other):
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            other_schema = other.schema()
            nullable = (
                (self._dtype.nullable if self._dtype is not None else True)
                or (other_schema.nullable if other_schema is not None else True)
            )
            fast = _accel_compare(self._storage, other._storage, op, nullable)
            if fast is not None:
                return fast
            return Vector._from_iterable_known_dtype(
                (None if (x is None or y is None) else bool(op(x, y)) for x, y in zip(self, other, strict=True)),
                Schema(bool, nullable),
            )
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            if len(self) != len(other):
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            vals = [None if (x is None or y is None) else bool(op(x, y)) for x, y in zip(self, other, strict=True)]
            return Vector._from_iterable_known_dtype(
                vals, Schema(bool, any(v is None for v in vals)))
        # Scalar comparison
        if other is None and op in (operator.eq, operator.ne):
            warnings.warn(
                "Null comparison: `v == None` yields null for every element. "
                "Use `v.is_na()` to test for nulls.",
                stacklevel=2
            )
        nullable = (self._dtype.nullable if self._dtype is not None else True) or other is None
        fast = _accel_compare(self._storage, other, op, nullable)
        if fast is not None:
            return fast
        return Vector._from_iterable_known_dtype(
            (None if (x is None or other is None) else bool(op(x, other)) for x in self),
            Schema(bool, nullable),
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

    def _logical_elementwise(self, other, kleene_func):
        """Kleene three-valued logical op (docs/null-semantics.md)."""
        other = self._check_duplicate(other)
        if self.ndims() == 2:
            return self.copy(tuple(
                col._logical_elementwise(other, kleene_func) for col in self.cols()
            ))
        if isinstance(other, Vector) and other.ndims() == 2:
            return other.copy(tuple(
                self._logical_elementwise(col, kleene_func) for col in other.cols()
            ))
        op_name = _KLEENE_OP_NAMES.get(kleene_func)
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            if len(self) != len(other):
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            if op_name is not None and isinstance(other, Vector):
                fast = _accel_logical(self._storage, other._storage, op_name)
                if fast is not None:
                    return fast
            vals = [kleene_func(x, y) for x, y in zip(self, other, strict=True)]
        else:
            if op_name is not None and (other is None or type(other) is bool):
                fast = _accel_logical(self._storage, other, op_name)
                if fast is not None:
                    return fast
            vals = [kleene_func(x, other) for x in self]
        return Vector._from_iterable_known_dtype(
            vals, Schema(bool, any(v is None for v in vals)))

    # &, |, ^ dispatch by dtype: bool vectors get Kleene logic; int vectors
    # get Python's bitwise operators (values obey Python, absence obeys the
    # null doctrine). Every other dtype raises — `1.5 & 2.5` is a TypeError
    # in Python, so it is one here too (docs/null-semantics.md).

    def _bitwise_kind_error(self, op_symbol):
        kind = self._dtype.kind if self._dtype is not None else None
        kind_name = kind.__name__ if kind is not None else 'object'
        return SerifTypeError(
            f"Unsupported operand type(s) for '{op_symbol}': Vector<{kind_name}>. "
            f"'{op_symbol}' is Kleene-logical on bool vectors and bitwise on "
            f"int vectors; other dtypes raise, as in plain Python."
        )

    def _tablewise_bitwise(self, other, op_dunder):
        """Per-column recursion for &, |, ^ on a 2-D block: each column's own
        dtype dispatch (Kleene / bitwise / raise) applies. Names preserved
        from the left side, matching Table arithmetic."""
        other = self._check_duplicate(other)
        result_cols = [getattr(col, op_dunder)(other) for col in self.cols()]
        for orig_col, result_col in zip(self.cols(), result_cols):
            result_col._name = orig_col._name
            result_col._wild = False
        from ..table import Table
        return Table(result_cols)

    def __and__(self, other):
        if self.ndims() == 2:
            return self._tablewise_bitwise(other, '__and__')
        kind = self._dtype.kind if self._dtype is not None else None
        if kind is int:
            return self._elementwise_operation(other, operator.and_, '__and__', '&')
        if kind is bool:
            return self._logical_elementwise(other, _kleene_and)
        raise self._bitwise_kind_error('&')

    def __or__(self, other):
        if self.ndims() == 2:
            return self._tablewise_bitwise(other, '__or__')
        kind = self._dtype.kind if self._dtype is not None else None
        if kind is int:
            return self._elementwise_operation(other, operator.or_, '__or__', '|')
        if kind is bool:
            return self._logical_elementwise(other, _kleene_or)
        raise self._bitwise_kind_error('|')

    def __xor__(self, other):
        if self.ndims() == 2:
            return self._tablewise_bitwise(other, '__xor__')
        kind = self._dtype.kind if self._dtype is not None else None
        if kind is int:
            return self._elementwise_operation(other, operator.xor, '__xor__', '^')
        if kind is bool:
            return self._logical_elementwise(other, _kleene_xor)
        raise self._bitwise_kind_error('^')

    def __rand__(self, other):
        return self.__and__(other)

    def __ror__(self, other):
        return self.__or__(other)

    def __rxor__(self, other):
        return self.__xor__(other)


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
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            result_dtype = _pre_compute_op_schema(self._dtype, other, op_func)
            if result_dtype is not None:
                fast = _accel_binop(self._storage, other._storage, op_func, result_dtype)
                if fast is not None:
                    return fast
            try:
                result_values = tuple(
                    None if (x is None or y is None) else op_func(x, y)
                    for x, y in zip(self, other, strict=True)
                )
            except TypeError:
                lhs = self._dtype.kind.__name__ if self._dtype is not None else 'object'
                rhs_schema = other.schema()
                rhs = rhs_schema.kind.__name__ if rhs_schema is not None else 'object'
                raise SerifTypeError(
                    f"Unsupported operand type(s) for '{op_symbol}': "
                    f"Vector<{lhs}> and Vector<{rhs}>."
                )
            if result_dtype is None:
                result_dtype = infer_dtype(result_values)
            return Vector(result_values, dtype=result_dtype, name=None)

        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
            if len(self) != len(other):
                raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
            try:
                result_values = tuple(
                    None if (x is None or y is None) else op_func(x, y)
                    for x, y in zip(self, other, strict=True)
                )
            except TypeError:
                lhs = self._dtype.kind.__name__ if self._dtype is not None else 'object'
                raise SerifTypeError(
                    f"Unsupported operand type(s) for '{op_symbol}': "
                    f"Vector<{lhs}> and {type(other).__name__} elements."
                )
            result_dtype = infer_dtype(result_values)
            return Vector(result_values, dtype=result_dtype, name=None)

        # Scalar path
        result_dtype = _pre_compute_op_schema(self._dtype, other, op_func)
        if result_dtype is not None:
            fast = _accel_binop(self._storage, other, op_func, result_dtype)
            if fast is not None:
                return fast
        try:
            result_values = tuple(
                None if x is None else op_func(x, other)
                for x in self._storage
            )
            if result_dtype is None:
                result_dtype = infer_dtype(result_values)
            return Vector(result_values, dtype=result_dtype, name=None)
        except TypeError:
            lhs = self._dtype.kind.__name__ if self._dtype is not None else 'object'
            raise SerifTypeError(
                f"Unsupported operand type(s) for '{op_symbol}': "
                f"'{lhs}' and '{type(other).__name__}'."
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
                None if x is None else op_func(x) for x in storage
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
        # For boolean vectors, use logical NOT instead of bitwise NOT.
        # NOT unknown is unknown (docs/null-semantics.md) — the old
        # `not None` mapped nulls to True.
        if self._dtype and self._dtype.kind is bool:
            fast = _accel_invert(self._storage, self._dtype.nullable)
            if fast is not None:
                return fast
            return Vector._from_iterable_known_dtype(
                (None if x is None else (not x) for x in self),
                Schema(bool, self._dtype.nullable),
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
        """Reverse addition: other + self.

        Routed through _elementwise_operation like every other reverse op so
        the result dtype is properly promoted — the old hand-rolled path
        stamped results with self's dtype, so 1.5 + Vector([1, 2]) produced
        floats labeled int and crashed the int storage backend.
        """
        return self._elementwise_operation(other, _reverse_add, '__radd__', '+')

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

    """
    Recursive Vector Operations
    """
    def max(self):
        if self.ndims() == 2:
            return self.copy((c.max() for c in self.cols()), name=None)
        ok, fast = _accel_reduce(self._storage, 'max_')
        if ok:
            return fast
        non_none = [v for v in self._storage if v is not None]
        return max(non_none) if non_none else None

    def min(self):
        if self.ndims() == 2:
            return self.copy((c.min() for c in self.cols()), name=None)
        ok, fast = _accel_reduce(self._storage, 'min_')
        if ok:
            return fast
        non_none = [v for v in self._storage if v is not None]
        return min(non_none) if non_none else None

    def first(self):
        """
        First element by position. Returns None if empty.

        Positional, NOT null-skipping: a leading None yields None (use
        .dropna().first() to skip nulls). On a 2-D block, returns the first
        element of each column (the first row). For an ordered pick, sort first:
        t.sort_by('date').first().
        """
        if self.ndims() == 2:
            return self.copy((c.first() for c in self.cols()), name=None)
        return self._storage[0] if len(self._storage) else None

    def last(self):
        """
        Last element by position (mirror of first()). Returns None if empty.
        """
        if self.ndims() == 2:
            return self.copy((c.last() for c in self.cols()), name=None)
        n = len(self._storage)
        return self._storage[n - 1] if n else None

    def sum(self):
        if self.ndims() == 2:
            return self.copy((c.sum() for c in self.cols()), name=None)
        ok, fast = _accel_reduce(self._storage, 'sum_')
        if ok:
            return fast
        # Exclude None values from sum
        values = (v for v in self._storage if v is not None)
        if self.schema().kind is not float:
            return sum(values)

        first = next(values, None)
        if first is None:
            return 0
        try:
            return math.fsum(chain((first,), values))
        except (OverflowError, ValueError):
            # math.fsum rejects mixtures such as +inf and -inf. Preserve
            # Python's non-finite behavior while finite sums remain stable
            # across every supported Python version.
            return sum(v for v in self._storage if v is not None)

    def _no_verdict(self, method_name, on_empty):
        if on_empty is not None:
            return on_empty
        n = len(self._storage)
        detail = "empty vector" if n == 0 else f"length {n}, all null"
        raise SerifEmptyReductionError(
            f"{method_name}() over zero valid values ({detail}): no verdict "
            f"is possible. Pass on_empty=True or on_empty=False to choose "
            f"the empty-case verdict, or fillna()/dropna() upstream."
        )

    def all(self, on_empty=None):
        """
        True if every valid (non-null) element is truthy.

        A verdict needs evidence: over zero valid values (empty vector, or
        all null after skipping) all() raises SerifEmptyReductionError
        unless on_empty supplies the empty-case verdict — the value you
        pass (True or False) is the value returned. See
        docs/null-semantics.md.
        """
        _check_on_empty('all', on_empty)
        if self.ndims() == 2:
            return self.copy((c.all(on_empty=on_empty) for c in self.cols()), name=None)
        seen_valid = False
        for v in self._storage:
            if v is None:
                continue
            if not v:
                return False
            seen_valid = True
        if seen_valid:
            return True
        return self._no_verdict('all', on_empty)

    def any(self, on_empty=None):
        """
        True if any valid (non-null) element is truthy.

        A verdict needs evidence: over zero valid values (empty vector, or
        all null after skipping) any() raises SerifEmptyReductionError
        unless on_empty supplies the empty-case verdict — the value you
        pass (True or False) is the value returned. See
        docs/null-semantics.md.
        """
        _check_on_empty('any', on_empty)
        if self.ndims() == 2:
            return self.copy((c.any(on_empty=on_empty) for c in self.cols()), name=None)
        seen_valid = False
        for v in self._storage:
            if v is None:
                continue
            if v:
                return True
            seen_valid = True
        if seen_valid:
            return False
        return self._no_verdict('any', on_empty)

    def mean(self):
        if self.ndims() == 2:
            return self.copy((c.mean() for c in self.cols()), name=None)
        ok, fast = _accel_reduce(self._storage, 'mean')
        if ok:
            return fast
        # Exclude None values from mean
        non_none = [v for v in self._storage if v is not None]
        return sum(non_none) / len(non_none) if non_none else None

    def stdev(self, population=False):
        if self.ndims() == 2:
            return self.copy((c.stdev(population) for c in self.cols()), name=None)
        ok, fast = _accel_reduce(self._storage, 'stdev', population=population)
        if ok:
            return fast
        # Exclude None values from stdev
        non_none = [v for v in self._storage if v is not None]
        if len(non_none) < 2:
            return None
        m = sum(non_none) / len(non_none)
        # use in-place sum over generator for fastness. I AM SPEED!
        # This is no longer 10x slower than numpy — numpy IS the fast path
        # above; this is the zero-dependency fallback and the specification.
        num = sum((x-m)*(x-m) for x in non_none)
        return (num/(len(non_none) - 1 + population))**0.5

    def count(self):
        if self.ndims() == 2:
            return self.copy((c.count() for c in self.cols()), name=None)
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
        key_fn = lambda i: (
            _null_sort_flag(storage.is_null(i), reverse, na_last),
            storage[i] if not storage.is_null(i) else 0,
        )

        order = sorted(range(n), key=key_fn, reverse=reverse)

        # Permute through the storage protocol — no Vector() constructor,
        # no type inference, works for every backend.
        return self._clone(_take(storage, order))


    def _check_duplicate(self, other):
        if id(self) == id(other):
            return self.copy()
        return other


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
                result._name = None
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
            raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
        return sum(x*y for x, y in zip(self._storage, other._storage, strict=True))

    def __rmatmul__(self, other):
        other = self._check_duplicate(other)
        if len(self.shape) > 1:
            return Vector(tuple(x @ other for x in self.cols()))
        if len(self) != len(other):
            raise SerifValueError(f"Length mismatch: {len(self)} != {len(other)}")
        return sum(x*y for x, y in zip(self._storage, other._storage, strict=True))


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
        if self._dtype is not None and self._dtype.kind in (bool, int) and isinstance(other, int):
            warnings.warn("The behavior of >> and << have been overridden for concatenation. Use .bit_lshift()/.bit_rshift() to shift bits.")

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
        if self._dtype is not None and self._dtype.kind in (bool, int) and isinstance(other, int):
            warnings.warn("The behavior of >> and << have been overridden for concatenation. Use .bit_lshift()/.bit_rshift() to shift bits.")

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
        elif len(self) == 0:
            # `not self` would trip Vector.__bool__'s ambiguity guard and
            # mask the intended error below.
            return Vector((other,),
                dtype=self._dtype)
        raise SerifTypeError("Cannot add a column of constant values. Try using Vector.filled(value, length).")

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
            return Vector((Vector(tuple(other)), self))
        # Scalar case: create a single-element vector for other
        return Vector((Vector((other,)), self))

    
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

