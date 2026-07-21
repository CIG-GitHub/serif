"""Canonical pure-Python physical implementations for Vector reductions."""

import builtins as _builtins
import math
from itertools import chain


def max_(storage):
    values = [value for value in storage if value is not None]
    return _builtins.max(values) if values else None


def min_(storage):
    values = [value for value in storage if value is not None]
    return _builtins.min(values) if values else None


def first(storage):
    return storage[0] if len(storage) else None


def last(storage):
    n = len(storage)
    return storage[n - 1] if n else None


def sum_(storage, kind):
    values = (value for value in storage if value is not None)
    if kind is not float:
        return _builtins.sum(values)

    first_value = next(values, None)
    if first_value is None:
        return 0
    try:
        return math.fsum(chain((first_value,), values))
    except (OverflowError, ValueError):
        # math.fsum rejects mixtures such as +inf and -inf. Preserve
        # Python's non-finite behavior while finite sums remain stable.
        return _builtins.sum(
            value for value in storage if value is not None
        )


def all_(storage):
    seen_valid = False
    for value in storage:
        if value is None:
            continue
        if not value:
            return False
        seen_valid = True
    return True if seen_valid else None


def any_(storage):
    seen_valid = False
    for value in storage:
        if value is None:
            continue
        if value:
            return True
        seen_valid = True
    return False if seen_valid else None


def mean(storage):
    values = [value for value in storage if value is not None]
    return _builtins.sum(values) / len(values) if values else None


def stdev(storage, population=False):
    values = [value for value in storage if value is not None]
    if len(values) < 2:
        return None
    mean_value = _builtins.sum(values) / len(values)
    numerator = _builtins.sum(
        (value - mean_value) * (value - mean_value)
        for value in values
    )
    return (numerator / (len(values) - 1 + population)) ** 0.5


def count(storage):
    return _builtins.sum(1 for value in storage if value is not None)
