"""Low-level contracts shared by optional physical backends.

This module owns the unique decline identity, optional-library import
boundaries, and the rule for advancing through optional backend attempts.
"""


# Distinct from every legitimate Serif result, including None.
DECLINED = object()


def first_supported(*attempts):
    """Return the first backend result that does not explicitly decline."""
    for attempt in attempts:
        result = attempt()
        if result is not DECLINED:
            return result
    return DECLINED


def _load_numpy():
    """Return NumPy when installed, otherwise None."""
    try:
        import numpy
    except ImportError:
        return None
    return numpy


def _load_arrow():
    """Return (pyarrow, pyarrow.compute) when installed, otherwise Nones."""
    try:
        import pyarrow
        import pyarrow.compute
    except ImportError:
        return None, None
    return pyarrow, pyarrow.compute
