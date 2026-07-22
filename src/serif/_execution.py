"""Low-level contracts shared by optional physical backends.

This module owns only the unique decline identity and optional-library import
boundaries. Semantic operation modules remain responsible for validation,
deterministic backend selection, and result wrapping.
"""


# Distinct from every legitimate Serif result, including None.
DECLINED = object()


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
