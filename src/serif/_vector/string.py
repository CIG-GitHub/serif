from .base import Vector
from .base import _elementwise_proxy


class _String(Vector):
    def count(self, *args, **kwargs):
        """
        Two personalities, disambiguated by arity — both Python-faithful:

        - count()           → number of non-null elements (the Vector
                              aggregate, same as every other dtype)
        - count(sub[, ...]) → per-element str.count(sub, ...)

        Zero-arg str.count doesn't exist in Python (always a TypeError), so
        no previously-valid call changes meaning.
        """
        if not args and not kwargs:
            return super().count()
        return Vector(tuple((s.count(*args, **kwargs) if s is not None else None) for s in self._storage))

    def before(self, sep):
        """Return the part of each string before the first occurrence of sep."""
        return Vector(tuple((s.partition(sep)[0] if s is not None else None) for s in self._storage))

    def after(self, sep):
        """Return the part of each string after the first occurrence of sep."""
        return Vector(tuple((s.partition(sep)[2] if s is not None else None) for s in self._storage))

    def before_last(self, sep):
        """Return the part of each string before the last occurrence of sep."""
        return Vector(tuple((s.rpartition(sep)[0] if s is not None else None) for s in self._storage))

    def after_last(self, sep):
        """Return the part of each string after the last occurrence of sep."""
        return Vector(tuple((s.rpartition(sep)[2] if s is not None else None) for s in self._storage))

    def categorize(self, categories=None):
        """
        Convert this string vector into a categorical vector with an explicit,
        ordered category list.

        Parameters
        ----------
        categories : list | tuple | Vector of str
            Defines both membership and sort order. No duplicates allowed.
            None is not a valid category.

        Returns
        -------
        _Category
            A new vector backed by integer codes with category-aware comparisons
            and sorting.

        Raises
        ------
        SerifTypeError
            If this vector is not a string vector, or if categories contain
            non-string elements.
        SerifValueError
            If any non-None value in this vector is not in the category list,
            or if the category list contains duplicates.

        Notes
        -----
        Pass ``None`` to infer categories from the data in appearance order.
        This is equivalent to ``v.categorize(v.unique())`` but more explicit
        about intent. The resulting category order reflects the first occurrence
        of each value in the vector.
        """
        from .categorical import _Category
        if categories is None:
            seen = []
            seen_set = set()
            for v in self._storage:
                if v is not None and v not in seen_set:
                    seen.append(v)
                    seen_set.add(v)
            categories = seen
        return _Category.from_values(self._storage, categories, name=self._name)


# Plain per-element str methods, stamped onto the class at definition time.
# Vector.__getattr__'s MethodProxy would serve these identically; explicit
# attributes keep them visible to dir()/tab-completion. Methods with custom
# semantics (count, before/after, categorize) are defined above and excluded.
# str.maketrans is a static method — mapping it per-element is meaningless,
# so it is deliberately not stamped (MethodProxy still resolves it for
# anyone who insists).
_STR_PROXY_METHODS = (
    'capitalize', 'casefold', 'center', 'encode', 'endswith', 'expandtabs',
    'find', 'format', 'format_map', 'index', 'isalnum', 'isalpha', 'isascii',
    'isdecimal', 'isdigit', 'isidentifier', 'islower', 'isnumeric',
    'isprintable', 'isspace', 'istitle', 'isupper', 'join', 'ljust', 'lower',
    'lstrip', 'partition', 'removeprefix', 'removesuffix', 'replace', 'rfind',
    'rindex', 'rjust', 'rpartition', 'rsplit', 'rstrip', 'split',
    'splitlines', 'startswith', 'strip', 'swapcase', 'title', 'translate',
    'upper', 'zfill',
)

for _m in _STR_PROXY_METHODS:
    setattr(_String, _m, _elementwise_proxy(_m))
del _m
