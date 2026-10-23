"""Optional compiled Rust physical implementations for Table operations."""

from ..._execution import _load_rust


_rs = _load_rust()
_USE_RUST = _rs is not None
