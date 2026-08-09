# ============================================================
# Container for numeric backends
# ============================================================
from ..vector import Vector
from . import math as _math
from .storage import ArrayStorage
from .storage import TupleStorage


class _Real(Vector):
    @property
    def math(self):
        """Access Python's ``math`` functions with Vector-aware semantics."""
        return _math.MathAccessor(self)


class _Float(_Real):
    typecode = 'd'


class _Int(_Real):

    def _build_storage(self, data, nullable):
        if not isinstance(data, (list, tuple)):
            data = list(data)
        try:
            return ArrayStorage.from_iterable(data, typecode='q', nullable=nullable)
        except OverflowError:
            return TupleStorage.from_iterable(data, nullable=nullable)
