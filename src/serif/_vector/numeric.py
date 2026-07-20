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


# Kind-level promotion for plain Python numeric types.
# Used by _pre_compute_op_schema in vector.py to resolve output dtype
# before touching any data.
_KIND_PROMOTION = {
    (bool,    bool):    bool,
    (bool,    int):     int,
    (int,     bool):    int,
    (int,     int):     int,
    (int,     float):   float,
    (float,   int):     float,
    (float,   float):   float,
    (int,     complex): complex,
    (float,   complex): complex,
    (complex, int):     complex,
    (complex, float):   complex,
    (complex, complex): complex,
    (str,     str):     str,
}
