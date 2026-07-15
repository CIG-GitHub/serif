class SerifError(Exception):
    """Base exception for serif library."""
    pass


class SerifKeyError(SerifError, KeyError):
    """Raised when a column/key is missing."""
    pass


class SerifTypeError(SerifError, TypeError):
    """Raised for invalid types in API calls."""
    pass


class SerifValueError(SerifError, ValueError):
    """Raised for invalid values or mismatched lengths."""
    pass


class SerifIndexError(SerifError, IndexError):
    """Raised for invalid indexing operations."""
    pass


class SerifEmptyReductionError(SerifValueError):
    """Raised when all()/any() is asked for a verdict over zero valid values.

    A boolean reduction over an empty or all-null vector has no evidence to
    summarize; pass on_empty=True/False to choose the empty-case verdict
    (docs/null-semantics.md).
    """
    pass
