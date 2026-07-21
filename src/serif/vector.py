import warnings
from collections.abc import Iterable

from .errors import SerifTypeError
from .errors import SerifValueError
from .display import _printr
from .naming import _sanitize_user_name
from ._vector import construction as _construction
from ._vector import element_api as _element_api
from ._vector import mutation as _mutation
from ._vector import operators as _operators
from ._vector import reductions as _reductions
from ._vector import selection as _selection
from ._vector import transforms as _transforms
from ._vector.dtype import Schema
from ._vector.storage import TupleStorage

from datetime import date
from datetime import datetime
from itertools import chain

from typing import List

__all__ = ["Vector"]

# ============================================================
# Reverse arithmetic operation helpers
# ============================================================
# ============================================================
# Small helpers
# ============================================================

# ============================================================
# Vector construction helpers
# ============================================================

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
        return _construction.new(
            cls,
            initial=initial,
            dtype=dtype,
            name=name,
            **kwargs,
        )



    def __init__(self, initial=(), dtype=None, name=None, **kwargs):
        return _construction.initialize(
            self,
            initial=initial,
            dtype=dtype,
            name=name,
            **kwargs,
        )


    def _build_storage(self, data, nullable):
        return _construction.build_storage(self, data, nullable)


    def _clone(self, new_storage, dtype=..., name=...):
        """Clone with known storage, preserving subtype and schema by default."""
        return _construction.clone(
            self,
            new_storage,
            dtype=dtype,
            name=name,
        )


    @classmethod
    def _from_storage(cls, storage, dtype, name=None):
        """Create a Vector from fully known, pre-built storage."""
        return _construction.from_storage(
            cls,
            storage,
            dtype,
            name=name,
        )


    @classmethod
    def _from_iterable_known_dtype(cls, iterable, dtype, *, name=None):
        """Build from an iterable without repeating known-dtype inference."""
        return _construction.from_iterable_known_dtype(
            cls,
            iterable,
            dtype,
            name=name,
        )



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

    @classmethod
    def filled(cls, value, length, typesafe=False):
        """Create a vector containing length copies of value."""
        return _construction.filled(
            cls,
            value,
            length,
            typesafe=typesafe,
        )



    def copy(self, new_values=None, name=...):
        """Create an independent Vector shell, sharing frozen storage when possible."""
        return _construction.copy(
            self,
            new_values=new_values,
            name=name,
        )

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
        return _transforms.to_object(self)


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
        return _transforms.cast(self, target_type)


    def fillna(self, value):
        return _transforms.fillna(self, value)


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
        return _transforms.dropna(self)


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
        return _transforms.is_na(self)


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
        return _transforms.is_type(self, types)


    def __iter__(self):
        """ iterate over the underlying tuple """
        return iter(self._storage)

    def __len__(self):
        """ length of the underlying tuple """
        return len(self._storage)

    def __getitem__(self, key):
        """Get a scalar, slice, mask selection, or positional selection."""
        return _selection.getitem(self, key)



    def _require_mutable(self):
        """Raise if this vector is a frozen table-owned column."""
        return _mutation.require_mutable(self)


    def _require_mutable_metadata(self):
        """Reject metadata mutation through a table-owned column."""
        return _mutation.require_mutable_metadata(self)


    def __setitem__(self, key, value):
        """Assign through a mutable Vector using copy-on-write semantics."""
        return _mutation.setitem(self, key, value)


    def _setitem_impl(self, key, value):
        """Plan, validate, and apply an assignment.

        Table batch and owner-write paths call this hook after establishing
        their own ownership boundary.
        """
        return _mutation.setitem_impl(self, key, value)

    def _elementwise_compare(self, other, op):
        return _operators.elementwise_compare(self, other, op)

    def __eq__(self, other):
        return _operators.eq(self, other)

    def __ge__(self, other):
        return _operators.ge(self, other)

    def __gt__(self, other):
        return _operators.gt(self, other)

    def __le__(self, other):
        return _operators.le(self, other)

    def __lt__(self, other):
        return _operators.lt(self, other)

    def __ne__(self, other):
        return _operators.ne(self, other)

    def _logical_elementwise(self, other, kleene_func):
        """Kleene three-valued logical op (docs/null-semantics.md)."""
        return _operators.logical_elementwise(self, other, kleene_func)

    def _bitwise_kind_error(self, op_symbol):
        return _operators.bitwise_kind_error(self, op_symbol)

    def __and__(self, other):
        return _operators.bit_and(self, other)

    def __or__(self, other):
        return _operators.bit_or(self, other)

    def __xor__(self, other):
        return _operators.bit_xor(self, other)

    def __rand__(self, other):
        return self.__and__(other)

    def __ror__(self, other):
        return self.__or__(other)

    def __rxor__(self, other):
        return self.__xor__(other)

    def _elementwise_operation(self, other, op_func, op_name: str, op_symbol: str):
        """Handle an element-wise operation with scalar broadcasting."""
        return _operators.elementwise_operation(
            self,
            other,
            op_func,
            op_name,
            op_symbol,
        )

    def _unary_operation(self, op_func, op_name: str):
        """Apply a unary operation to each element."""
        return _operators.unary_operation(self, op_func, op_name)

    def __add__(self, other):
        return _operators.add(self, other)

    def __mul__(self, other):
        return _operators.mul(self, other)

    def __sub__(self, other):
        return _operators.sub(self, other)

    def __neg__(self):
        return _operators.neg(self)

    def __pos__(self):
        return _operators.pos(self)

    def __abs__(self):
        return _operators.abs(self)

    def __invert__(self):
        return _operators.invert(self)

    def __truediv__(self, other):
        return _operators.truediv(self, other)

    def __floordiv__(self, other):
        return _operators.floordiv(self, other)

    def __mod__(self, other):
        return _operators.mod(self, other)

    def __pow__(self, other):
        return _operators.pow(self, other)

    def __radd__(self, other):
        """Reverse addition: other + self.

        Routed through the shared elementwise implementation so the result
        dtype is promoted before storage is constructed.
        """
        return _operators.radd(self, other)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __rsub__(self, other):
        return _operators.rsub(self, other)

    def __rtruediv__(self, other):
        return _operators.rtruediv(self, other)

    def __rfloordiv__(self, other):
        return _operators.rfloordiv(self, other)

    def __rmod__(self, other):
        return _operators.rmod(self, other)

    def __rpow__(self, other):
        return _operators.rpow(self, other)




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
        return _reductions.max(self)

    def min(self):
        return _reductions.min(self)

    def first(self):
        """
        First element by position. Returns None if empty.

        Positional, NOT null-skipping: a leading None yields None (use
        .dropna().first() to skip nulls). On a 2-D block, returns the first
        element of each column (the first row). For an ordered pick, sort first:
        t.sort_by('date').first().
        """
        return _reductions.first(self)

    def last(self):
        """
        Last element by position (mirror of first()). Returns None if empty.
        """
        return _reductions.last(self)

    def sum(self):
        return _reductions.sum(self)

    def all(self, on_empty=None):
        """
        True if every valid (non-null) element is truthy.

        A verdict needs evidence: over zero valid values (empty vector, or
        all null after skipping) all() raises SerifEmptyReductionError
        unless on_empty supplies the empty-case verdict — the value you
        pass (True or False) is the value returned. See
        docs/null-semantics.md.
        """
        return _reductions.all(self, on_empty=on_empty)

    def any(self, on_empty=None):
        """
        True if any valid (non-null) element is truthy.

        A verdict needs evidence: over zero valid values (empty vector, or
        all null after skipping) any() raises SerifEmptyReductionError
        unless on_empty supplies the empty-case verdict — the value you
        pass (True or False) is the value returned. See
        docs/null-semantics.md.
        """
        return _reductions.any(self, on_empty=on_empty)

    def mean(self):
        return _reductions.mean(self)

    def stdev(self, population=False):
        return _reductions.stdev(self, population=population)

    def count(self):
        return _reductions.count(self)

    def unique(self):
        return _transforms.unique(self)


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
        return _transforms.sort_by(
            self,
            reverse=reverse,
            na_last=na_last,
        )



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
        from ._table import columns as _table_columns
        return _table_columns.compose_vector(self, other)

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
        return _operators.bit_lshift(self, other)

    def bit_rshift(self, other):
        """
        Bitwise right shift (>>).
        Explicit method since '>>' operator is used for column addition.
        """
        return _operators.bit_rshift(self, other)


    def __getattr__(self, name):
        """Proxy attribute access to the underlying scalar dtype."""
        return _element_api.resolve(self, name)
