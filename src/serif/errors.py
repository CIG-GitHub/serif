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


class SerifEmptyReductionWarning(UserWarning):
    """Warned when all()/any() reduces zero valid values to the identity.

    A boolean reduction over an empty or all-null vector has no evidence to
    summarize; serif returns the Python identity (all() -> True, any() ->
    False) and warns. Pass on_empty=True/False to state the empty-case
    verdict yourself and silence the warning (docs/null-semantics.md).
    """
    pass
