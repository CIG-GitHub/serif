from .storage import ArrayStorage
from .storage import TupleStorage
from .storage import DecimalStorage
from .dtype import Schema
from .dtype import infer_dtype
from .dtype import validate_scalar


def __getattr__(name):
    """Preserve the historical ``serif._vector`` convenience exports lazily."""
    if name == "Vector":
        from ..vector import Vector
        return Vector
    if name in {"_Int", "_Float"}:
        from .numeric import _Float, _Int
        return {"_Int": _Int, "_Float": _Float}[name]
    if name == "_String":
        from .string import _String
        return _String
    if name == "_Date":
        from .dates import _Date
        return _Date
    if name == "_Category":
        from .categorical import _Category
        return _Category
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "Vector",
    "_Int",
    "_Float",
    "_String",
    "_Date",
    "_Category",
    "ArrayStorage",
    "TupleStorage",
    "DecimalStorage",
    "Schema",
    "infer_dtype",
    "validate_scalar",
]
