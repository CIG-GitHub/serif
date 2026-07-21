"""Optional Arrow physical implementations for Vector operations."""

from ..._execution import _load_arrow


_pa, _pc = _load_arrow()
_USE_ARROW = _pa is not None
