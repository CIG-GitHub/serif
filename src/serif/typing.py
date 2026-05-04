# Re-export from canonical location for backward compatibility.
# DataType and friends now live in serif._vector.dtype
from ._vector.dtype import DataType, infer_dtype, infer_kind, validate_scalar

__all__ = ["DataType", "infer_dtype", "infer_kind", "validate_scalar"]

