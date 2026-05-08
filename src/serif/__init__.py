"""
serif: A Pythonic, zero-dependency vector and table library

Designed for Python users who need to work with datasets beyond Excel's limits
(>1000 rows) but want the ease-of-use and intuitive feel of Excel or SQL.

Main classes:
    - Vector: 1D vector with optional type safety
    - Table: 2D table (multiple columns of equal length)

Internals live in serif._vector:
    - _vector/base.py    — Vector mechanics
    - _vector/dtype.py   — DataType, infer_dtype, validate_scalar
    - _vector/storage.py — ArrayStorage, TupleStorage
    - _vector/numeric.py — _Int, _Float, sized subclasses, promotion table
    - _vector/string.py  — _String
    - _vector/dates.py   — _Date

Zero external dependencies - pure Python stdlib only.
"""

from ._vector import Vector
from ._vector import _Int
from ._vector import _Float
from ._vector import _String
from ._vector import _Date
from ._vector import _Category
from .table import Table
from .errors import SerifError
from .errors import SerifKeyError
from .errors import SerifValueError
from .errors import SerifTypeError
from .errors import SerifIndexError
from .csv import read_csv
from .display import set_repr_rows

__version__ = "0.1.2"
__all__ = [
    "Vector", 
    "Table",
    "read_csv",
    "set_repr_rows",
    "SerifError",
    "SerifKeyError",
    "SerifValueError",
    "SerifTypeError",
    "SerifIndexError"
]
