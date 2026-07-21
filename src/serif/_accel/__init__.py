"""
OPTIONAL accelerated compute — transport, never representation.

serif stays zero-dependency: acceleration is OPPORTUNISTIC. numpy and
pyarrow are the two backends — each activates only when it happens to
be installed, each declines per-call, and nothing anywhere requires
either. numpy owns the fixed-width work (its lanes ARE serif's numeric
buffers); pyarrow owns what numpy structurally cannot touch —
variable-width UTF-8 content (see arrow.py). The doctrine is the one
proven by the pyarrow reader (serif/io/_arrow.py):

  * python in → python out. Whatever computes a result, what surfaces is
    concrete Python values (int, float, bool, None) — never numpy or
    pyarrow scalars. np.float64 IS a float subclass, so leaks are
    invisible to equality checks; conformance tests assert
    `type(x) is float`.

  * Accelerators may widen TRANSPORT, never SEMANTICS. Every accelerated
    operation returns results IDENTICAL to the pure path, or DECLINES
    (returns None) and the pure path runs. Declines are per-call — an
    unsupported storage type, typecode, or value range costs one
    isinstance check, not a mode switch.

  * No new representation. Backends operate on serif's EXISTING buffers:
    ArrayStorage's array.array and BoolStorage's bytearray are viewed
    zero-copy via np.frombuffer; BitMask's packed LSB-first bits are one
    np.unpackbits(bitorder='little') from a bool array and one
    np.packbits from coming back. StringStorage IS the arrow StringArray
    layout (validity bitmap, offsets, UTF-8 buffer), so pyarrow wraps it
    with zero copies. The storage layout being arrow-shaped keeps paying.

Modules: api (the remaining selection and Table call-through boundary), mask
(row gathering — filter, take, padded take), group (single-key bucketing for
partitions), join (vectorized single-key join probe), and arrow (the remaining
Table Arrow kernels). Migrated Vector implementations live beside their
semantic modules under serif/_vector/_python, _numpy, and _arrow.

_USE_NUMPY here and _USE_ARROW in arrow.py are private switches for
tests/benchmarks, not API.
"""

from .._execution import DECLINED
from .._execution import _load_numpy
from .._vector._numpy.storage import NP_DTYPES
from .._vector._numpy.storage import valid_bits


_np = _load_numpy()

_USE_NUMPY = _np is not None
