"""Optional NumPy physical implementations for Vector operations."""

from ..._execution import _load_numpy


_np = _load_numpy()
_USE_NUMPY = _np is not None
