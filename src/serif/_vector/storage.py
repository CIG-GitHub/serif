"""
Storage backends for Vector data.

The storage protocol
--------------------
A storage backend must provide:

    __len__()          — element count
    __getitem__(idx)   — value at idx, None if null (int index only)
    __iter__()         — yields values, None at null positions
    is_null(idx)       — True if the element at idx is null
    slice(slc)         — new storage covering the slice
    take(indices)      — new storage gathering the given positions, in order
    to_tuple()         — materialize as a tuple (None at null positions)

The protocol is read-only: mutation happens by REBUILDING storage
(Vector.__setitem__ materializes, edits, and re-wraps), never in place.
The ONE exception is a batch() scope: entering it calls private_copy()
(buffers duplicated, so nothing else shares them), after which
write_inplace() may land point writes directly. Backends without those
two methods simply keep rebuilding — write_inplace returning False (or
being absent) DECLINES to the rebuild path, whose behavior is the
specification.

Base-class code (Vector and friends) must go through this protocol.
Reaching into a backend's internals (_data, _mask, _buf, _offsets) is
allowed ONLY behind an isinstance() check of the concrete class — a bare
`storage._data` in generic code is a bug waiting for the next backend.
"""

from __future__ import annotations
from array import array
from typing import Any
from typing import Iterator
from collections.abc import Iterable
from .nullable import BitMask


class ArrayStorage:
    """
    Contiguous numeric storage using array.array.

    None cannot live in array.array, so nulls are tracked with a separate
    BitMask (1=valid, 0=null). mask=None means no nulls present.
    Let array.array raise on bad typecodes or overflow — not duplicated here.
    """

    __slots__ = ('_data', '_mask')

    def __init__(self, data: array, mask: BitMask | None = None):
        self._data = data
        self._mask = mask

    @classmethod
    def from_iterable(cls, values: Iterable[Any], typecode: str, nullable: bool) -> ArrayStorage:
        data_list = []
        null_flags = []
        has_nulls = False
        for val in values:
            if val is None:
                has_nulls = True
                null_flags.append(True)
                data_list.append(0)  # sentinel — position is masked
            else:
                null_flags.append(False)
                data_list.append(val)

        data = array(typecode, data_list)  # raises TypeError/OverflowError on bad values
        mask = BitMask.from_iterable(null_flags) if has_nulls else None
        return cls(data, mask)

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> Any:
        if self._mask is not None and self._mask.is_null(idx):
            return None
        return self._data[idx]

    def __iter__(self) -> Iterator[Any]:
        if self._mask is not None:
            for i in range(len(self._data)):
                yield None if self._mask.is_null(i) else self._data[i]
        else:
            yield from self._data

    def is_null(self, idx: int) -> bool:
        return self._mask is not None and self._mask.is_null(idx)

    def slice(self, slc: slice) -> ArrayStorage:
        new_data = self._data[slc]
        new_mask = self._mask[slc] if self._mask is not None else None
        return ArrayStorage(new_data, new_mask)

    def take(self, indices) -> ArrayStorage:
        new_data = array(self._data.typecode, (self._data[i] for i in indices))
        if self._mask is not None:
            null_flags = [self._mask.is_null(i) for i in indices]
            new_mask = BitMask.from_iterable(null_flags) if any(null_flags) else None
        else:
            new_mask = None
        return ArrayStorage(new_data, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def private_copy(self) -> ArrayStorage:
        """Same values, freshly-owned buffers — the un-sharing step of
        batch(): raw writes may then land here without being visible to
        any other holder of the original storage."""
        mask = (BitMask(bytearray(self._mask._buf), len(self._mask))
                if self._mask is not None else None)
        return ArrayStorage(array(self._data.typecode, self._data), mask)

    def write_inplace(self, updates) -> bool:
        """Apply [(idx, value)] point writes directly into the buffer.

        Legal only on a privately-owned storage (see private_copy). Values
        are already schema-validated by Vector.__setitem__; this checks
        only what the BUFFER can hold, and returns False to DECLINE
        anything else (unsupported typecode, int outside int64) — the
        rebuild path then applies the same updates, including its
        OverflowError degradation to TupleStorage. All-or-nothing: the
        pre-check runs before the first write.
        """
        tc = self._data.typecode
        if tc == 'q':
            def fits(v):
                return isinstance(v, int) and -2**63 <= v < 2**63
        elif tc == 'd':
            def fits(v):
                return isinstance(v, (int, float))
        else:
            return False
        if any(v is not None and not fits(v) for _, v in updates):
            return False

        mask = self._mask
        if mask is None and any(v is None for _, v in updates):
            mask = BitMask.from_size(len(self._data))
            self._mask = mask
        data = self._data
        for idx, v in updates:
            if v is None:
                data[idx] = 0          # sentinel — position is masked
                mask.set_null(idx)
            else:
                data[idx] = v
                if mask is not None:
                    mask.set_valid(idx)
        return True


class TupleStorage:
    """
    General-purpose storage using a Python tuple.

    None is stored inline — tuples hold anything, no sentinel or mask needed.
    """

    __slots__ = ('_data',)

    def __init__(self, data: tuple):
        self._data = data

    @classmethod
    def from_iterable(cls, values: Iterable[Any], nullable: bool = False) -> TupleStorage:
        if isinstance(values, tuple):
            return cls(values)
        return cls(tuple(values))

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> Any:
        return self._data[idx]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def is_null(self, idx: int) -> bool:
        return self._data[idx] is None

    def slice(self, slc: slice) -> TupleStorage:
        return TupleStorage(self._data[slc])

    def take(self, indices) -> TupleStorage:
        return TupleStorage(tuple(self._data[i] for i in indices))

    def __bool__(self) -> bool:
        return len(self._data) > 0

    def to_tuple(self) -> tuple:
        return self._data


# ---------------------------------------------------------------------------
# BoolStorage — byte-packed boolean buffer
# ---------------------------------------------------------------------------

class BoolStorage:
    """
    Byte-packed boolean storage: one 0/1 byte per element in a bytearray,
    nulls tracked with a separate BitMask (same split as ArrayStorage).

    Why one BYTE per element, not one bit (Arrow) or a tuple of pointers
    (the previous backend):
    - vs tuple: ~8 bytes of pointer per element down to 1 — and a plain
      byte index in pure Python instead of a pointer chase. bytearray is
      the stdlib's numpy.
    - vs bit-packed: pure-Python bit twiddling per access is SLOWER than
      the tuple it would replace; a zero-dependency library must not make
      its own fallback path worse. numpy views a bytearray zero-copy
      (frombuffer, dtype=bool); bit-packing would force an unpack copy
      per operation instead.
    - I/O: Arrow and Parquet BOOLEAN are bit-packed, so the boundary pays
      one pack/unpack pass — the same toll the decimal byte-swap already
      pays, and numpy collapses it to a single C call (packbits/unpackbits).

    __getitem__ surfaces real Python bools (python in → python out).
    """

    __slots__ = ('_data', '_mask')

    def __init__(self, data: bytearray, mask: BitMask | None = None):
        self._data = data
        self._mask = mask

    @classmethod
    def from_iterable(cls, values: Iterable[Any], nullable: bool = False) -> BoolStorage:
        data = bytearray()
        null_flags = []
        has_nulls = False
        for val in values:
            if val is None:
                has_nulls = True
                null_flags.append(True)
                data.append(0)  # sentinel — position is masked
            else:
                null_flags.append(False)
                data.append(1 if val else 0)
        mask = BitMask.from_iterable(null_flags) if has_nulls else None
        return cls(data, mask)

    @classmethod
    def from_raw(cls, data: bytearray, mask: BitMask | None = None) -> BoolStorage:
        """Wrap a pre-built 0/1 bytearray with zero copying (I/O fast paths)."""
        return cls(data, mask)

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return len(self._data) > 0

    def __getitem__(self, idx: int) -> Any:
        if self._mask is not None and self._mask.is_null(idx):
            return None
        return bool(self._data[idx])

    def __iter__(self) -> Iterator[Any]:
        if self._mask is not None:
            for i in range(len(self._data)):
                yield None if self._mask.is_null(i) else bool(self._data[i])
        else:
            for b in self._data:
                yield bool(b)

    def is_null(self, idx: int) -> bool:
        return self._mask is not None and self._mask.is_null(idx)

    def slice(self, slc: slice) -> BoolStorage:
        new_data = self._data[slc]
        new_mask = self._mask[slc] if self._mask is not None else None
        return BoolStorage(new_data, new_mask)

    def take(self, indices) -> BoolStorage:
        new_data = bytearray(self._data[i] for i in indices)
        if self._mask is not None:
            null_flags = [self._mask.is_null(i) for i in indices]
            new_mask = BitMask.from_iterable(null_flags) if any(null_flags) else None
        else:
            new_mask = None
        return BoolStorage(new_data, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)

    def private_copy(self) -> BoolStorage:
        """Same values, freshly-owned buffers (see ArrayStorage.private_copy)."""
        mask = (BitMask(bytearray(self._mask._buf), len(self._mask))
                if self._mask is not None else None)
        return BoolStorage(bytearray(self._data), mask)

    def write_inplace(self, updates) -> bool:
        """Point writes into the 0/1 byte buffer (see ArrayStorage.write_inplace)."""
        if any(v is not None and not isinstance(v, bool) for _, v in updates):
            return False
        mask = self._mask
        if mask is None and any(v is None for _, v in updates):
            mask = BitMask.from_size(len(self._data))
            self._mask = mask
        data = self._data
        for idx, v in updates:
            if v is None:
                data[idx] = 0          # sentinel — position is masked
                mask.set_null(idx)
            else:
                data[idx] = 1 if v else 0
                if mask is not None:
                    mask.set_valid(idx)
        return True


# ---------------------------------------------------------------------------
# StringStorage — Arrow-style contiguous UTF-8 string buffer
# ---------------------------------------------------------------------------

class StringStorage:
    """
    Contiguous UTF-8 string storage: one bytes buffer + uint32 offsets array.

    Layout
    ------
    _buf:     bytes          — all string data concatenated (UTF-8)
    _offsets: array('I')    — length n+1; string i lives at buf[off[i]:off[i+1]]
    _mask:    BitMask|None — null flags (1=valid, 0=null per new convention)

    Accessing string i:  buf[offsets[i]:offsets[i+1]].decode('utf-8')

    Benefits over TupleStorage for string columns
    ---------------------------------------------
    - Construction: one b''.join() pass instead of N .encode() calls living as
      separate heap objects.
    - Memory: ~4 bytes/string (offset) + raw UTF-8 bytes vs ~57 bytes/string
      Python str object overhead.
    - Parquet integration: raw BYTE_ARRAY bytes slot directly into _buf;
      decode is deferred until a value is actually accessed.
    - Multi-byte / emoji / non-ASCII: fully handled — offsets are byte positions,
      bytes.decode('utf-8') handles all of UTF-8 correctly.

    Limit: total buffer size must fit in uint32 (~4 GB per column).
    """

    __slots__ = ('_buf', '_offsets', '_mask')

    def __init__(self, buf: bytes, offsets: array, mask: BitMask | None = None):
        self._buf     = buf
        self._offsets = offsets  # array('I'), length = n+1
        self._mask    = mask

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_iterable(cls, values: Iterable[Any]) -> StringStorage:
        """
        Build from any iterable of str | None values.
        One pass: encodes and concatenates all strings, builds offset array.
        """
        buf_parts:  list[bytes] = []
        offsets:    list[int]   = [0]
        null_flags: list[bool]  = []
        has_nulls = False

        for val in values:
            if val is None:
                has_nulls = True
                null_flags.append(True)
                offsets.append(offsets[-1])     # zero-length sentinel for null
            else:
                encoded = val.encode('utf-8')
                buf_parts.append(encoded)
                null_flags.append(False)
                offsets.append(offsets[-1] + len(encoded))

        buf  = b''.join(buf_parts)
        arr  = array('I', offsets)
        mask = BitMask.from_iterable(null_flags) if has_nulls else None
        return cls(buf, arr, mask)

    @classmethod
    def from_raw(cls, buf: bytes, offsets: array,
                 mask: BitMask | None = None) -> StringStorage:
        """
        Wrap pre-built components with zero copying.
        Used by the Parquet reader which builds _buf directly from page bytes.
        offsets must already be an array('I') of length n+1.
        """
        return cls(buf, offsets, mask)

    # ------------------------------------------------------------------
    # Core interface (matches ArrayStorage / TupleStorage)
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._offsets) - 1

    def __bool__(self) -> bool:
        return len(self._offsets) > 1

    def __getitem__(self, idx: int) -> Any:
        # Normalize negative indices: offsets[idx]:offsets[idx+1] straddles the
        # buffer ends for idx=-1 and would silently return ''.
        n = len(self._offsets) - 1
        if idx < 0:
            idx += n
        if not 0 <= idx < n:
            raise IndexError('string index out of range')
        if self._mask is not None and self._mask.is_null(idx):
            return None
        return self._buf[self._offsets[idx]:self._offsets[idx + 1]].decode('utf-8')

    def __iter__(self) -> Iterator[Any]:
        buf     = self._buf
        offsets = self._offsets
        mask    = self._mask
        for i in range(len(offsets) - 1):
            if mask is not None and mask.is_null(i):
                yield None
            else:
                yield buf[offsets[i]:offsets[i + 1]].decode('utf-8')

    def is_null(self, idx: int) -> bool:
        return self._mask is not None and self._mask.is_null(idx)

    def slice(self, slc: slice) -> StringStorage:
        """Return a new StringStorage covering the slice."""
        return self.take(range(len(self))[slc])

    def take(self, indices) -> StringStorage:
        """Gather the given positions into a new StringStorage.

        Copies raw byte chunks between buffers — no decode/encode round-trip.
        """
        buf_parts:  list[bytes] = []
        new_offs:   list[int]   = [0]
        null_flags: list[bool]  = []
        has_nulls = False

        buf     = self._buf
        offsets = self._offsets
        mask    = self._mask

        for i in indices:
            is_null = mask is not None and mask.is_null(i)
            null_flags.append(is_null)
            if is_null:
                has_nulls = True
                new_offs.append(new_offs[-1])   # zero advance
            else:
                chunk = buf[offsets[i]:offsets[i + 1]]
                buf_parts.append(chunk)
                new_offs.append(new_offs[-1] + len(chunk))

        new_buf  = b''.join(buf_parts)
        new_arr  = array('I', new_offs)
        new_mask = BitMask.from_iterable(null_flags) if has_nulls else None
        return StringStorage(new_buf, new_arr, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)


# ---------------------------------------------------------------------------
# DecimalStorage — Arrow-format 16-byte big-endian decimal buffer
# ---------------------------------------------------------------------------

class DecimalStorage:
    """
    Fixed-width 16-byte big-endian decimal storage (decimal128).

    Layout
    ------
    _buf:       bytearray    — n*16 bytes, big-endian two's complement.
                               Null positions hold 16 zero bytes (sentinel).
    _scale:     int          — fixed exponent: actual = unscaled / 10^scale
    _precision: int          — max significant digits (Parquet schema metadata)
    _mask:      BitMask|None— null flags (1=valid, 0=null); None = no nulls

    Matches the physical layout of both Parquet FIXED_LEN_BYTE_ARRAY + DECIMAL
    and (after a byte-swap) Arrow decimal128, so I/O fast paths hand the buffer
    in with minimal copying.  All values share the same scale — mixed-scale
    inputs degrade to object dtype through the normal promote_dtype path.
    """

    __slots__ = ('_buf', '_scale', '_precision', '_mask')

    def __init__(self, buf: bytearray, scale: int, precision: int,
                 mask: BitMask | None = None):
        self._buf       = buf
        self._scale     = scale
        self._precision = precision
        self._mask      = mask

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_iterable(cls, values, scale: int, precision: int,
                      nullable: bool = False) -> 'DecimalStorage':
        """
        Build from an iterable of Decimal | None with a fixed scale.

        Each value is shifted to `scale` decimal places (ROUND_HALF_EVEN).
        Null positions store a 16-byte zero sentinel.
        """
        from decimal import Decimal, ROUND_HALF_EVEN
        buf        = bytearray()
        null_flags: list[bool] = []
        has_nulls  = False
        multiplier = Decimal(10) ** scale

        for val in values:
            if val is None:
                has_nulls = True
                null_flags.append(True)
                buf.extend(b'\x00' * 16)
            else:
                unscaled = int(
                    (val * multiplier).to_integral_value(rounding=ROUND_HALF_EVEN)
                )
                buf.extend(unscaled.to_bytes(16, 'big', signed=True))
                null_flags.append(False)

        mask = BitMask.from_iterable(null_flags) if has_nulls else None
        return cls(buf, scale, precision, mask)

    @classmethod
    def from_raw_be(cls, buf, scale: int, precision: int,
                    mask: BitMask | None = None) -> 'DecimalStorage':
        """
        Wrap a pre-built big-endian buffer (Parquet reader). Near-zero copy.
        ``buf`` may be bytes or bytearray.
        """
        return cls(bytearray(buf), scale, precision, mask)

    # ------------------------------------------------------------------
    # Core interface (matches ArrayStorage / TupleStorage / StringStorage)
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._buf) // 16

    def __bool__(self) -> bool:
        return len(self._buf) > 0

    def __getitem__(self, idx: int):
        n = len(self)
        if idx < 0:
            idx += n
        if not 0 <= idx < n:
            raise IndexError('decimal index out of range')
        if self._mask is not None and self._mask.is_null(idx):
            return None
        from decimal import Decimal
        unscaled = int.from_bytes(self._buf[idx * 16:(idx + 1) * 16], 'big', signed=True)
        return Decimal(unscaled).scaleb(-self._scale)

    def __iter__(self):
        from decimal import Decimal
        scale = self._scale
        mask  = self._mask
        buf   = self._buf
        n     = len(self)
        for i in range(n):
            if mask is not None and mask.is_null(i):
                yield None
            else:
                unscaled = int.from_bytes(buf[i * 16:(i + 1) * 16], 'big', signed=True)
                yield Decimal(unscaled).scaleb(-scale)

    def is_null(self, idx: int) -> bool:
        return self._mask is not None and self._mask.is_null(idx)

    def slice(self, slc: slice) -> 'DecimalStorage':
        indices  = range(*slc.indices(len(self)))
        new_buf  = bytearray()
        for i in indices:
            new_buf.extend(self._buf[i * 16:(i + 1) * 16])
        new_mask = self._mask[slc] if self._mask is not None else None
        return DecimalStorage(new_buf, self._scale, self._precision, new_mask)

    def take(self, indices) -> 'DecimalStorage':
        new_buf    = bytearray()
        null_flags: list[bool] = []
        has_nulls  = False
        for i in indices:
            new_buf.extend(self._buf[i * 16:(i + 1) * 16])
            is_null = self._mask is not None and self._mask.is_null(i)
            null_flags.append(is_null)
            if is_null:
                has_nulls = True
        new_mask = BitMask.from_iterable(null_flags) if has_nulls else None
        return DecimalStorage(new_buf, self._scale, self._precision, new_mask)

    def to_tuple(self) -> tuple:
        return tuple(self)
