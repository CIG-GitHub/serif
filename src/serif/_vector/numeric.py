# ============================================================
# Container for numeric backends
# ============================================================
from ..vector import Vector
from . import math as _math
from . import statistics as _statistics
from .element_api import _elementwise_method
from .element_api import _elementwise_property
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
    as_integer_ratio = _elementwise_method(
        float,
        'as_integer_ratio',
        tuple,
    )
    is_integer = _elementwise_method(float, 'is_integer', bool)
    hex = _elementwise_method(float, 'hex', str)
    real = _elementwise_property(float, 'real', float)
    imag = _elementwise_property(float, 'imag', float)


class _Int(_Real):
    bit_length = _elementwise_method(int, 'bit_length', int)
    bit_count = _elementwise_method(int, 'bit_count', int)
    to_bytes = _elementwise_method(int, 'to_bytes', bytes)
    real = _elementwise_property(int, 'real', int)
    imag = _elementwise_property(int, 'imag', int)

    def _build_storage(self, data, nullable):
        if not isinstance(data, (list, tuple)):
            data = list(data)
        try:
            return ArrayStorage.from_iterable(data, typecode='q', nullable=nullable)
        except OverflowError:
            return TupleStorage.from_iterable(data, nullable=nullable)
