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
    """Warned when all()/any() reduces empty input to the identity.

    With zero elements, serif returns the Python identity (all() -> True,
    any() -> False) and warns. Pass on_empty=True/False to choose the empty
    result and silence the warning. Nonempty all-null inputs return None
    without this warning.
    """
    pass
