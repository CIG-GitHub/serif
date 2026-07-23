# ============================================================
# Container for numeric backends
# ============================================================
from ..vector import Vector
from .storage import ArrayStorage
from .storage import TupleStorage


class _Float(Vector):
    typecode = 'd'


class _Int(Vector):

    def _build_storage(self, data, nullable):
        if not isinstance(data, (list, tuple)):
            data = list(data)
        try:
            return ArrayStorage.from_iterable(data, typecode='q', nullable=nullable)
        except OverflowError:
            return TupleStorage.from_iterable(data, nullable=nullable)
