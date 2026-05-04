"""
serif: A Pythonic, zero-dependency vector and table library

Designed for Python users who need to work with datasets beyond Excel's limits
(>1000 rows) but want the ease-of-use and intuitive feel of Excel or SQL.

Main classes:
    - Vector: 1D vector with optional type safety
    - Table: 2D table (multiple columns of equal length)
    
Type-specific subclasses (auto-created):
    - _Float: Vector of floats with float method proxying
    - _Int: Vector of integers with int method proxying
    - _String: Vector of strings with string method proxying
    - _Date: Vector of dates with date method proxying

Zero external dependencies - pure Python stdlib only.
"""

from .alias_tracker import _ALIAS_TRACKER
from .alias_tracker import AliasError
from .vector import Vector
from .vector import _Float
from .vector import _Int
from .vector import _String
from .vector import _Date
from .table import Table
from .errors import SerifError
from .errors import SerifKeyError
from .errors import SerifValueError
from .errors import SerifTypeError
from .errors import SerifIndexError
from .csv import read_csv
from .typing import DataType
from .display import set_repr_rows

__version__ = "0.1.2"
__all__ = [
	"Vector", 
	"Table",
	"read_csv",
	"DataType",
	"set_repr_rows",
	"AliasError",
	"SerifError",
	"SerifKeyError",
	"SerifValueError",
	"SerifTypeError",
	"SerifIndexError"
]
