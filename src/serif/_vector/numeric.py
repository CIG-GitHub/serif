# ============================================================
# Container for numeric backends
# ============================================================
from ..vector import Vector
from . import math as _math
from . import statistics as _statistics
from .storage import ArrayStorage
from .storage import TupleStorage


class _Real(Vector):
    def mean(self):
        """Alias for the canonical ``stats.mean()`` reduction."""
        return self.stats.mean()

    def std(self):
        """Alias for the canonical sample ``stats.stdev()`` reduction."""
        return self.stats.stdev()

    @property
    def math(self):
        """Access Python's ``math`` functions with Vector-aware semantics."""
        return _math.MathAccessor(self)

    @property
    def stats(self):
        """Access Python's ``statistics`` functions over known values."""
        return _statistics.StatisticsAccessor(self)


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
