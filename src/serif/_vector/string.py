from .base import Vector
from ..errors import SerifTypeError


class _String(Vector):
    def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
        # dtype already set by __new__
        super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

    def capitalize(self):
        """ Call the internal capitalize method on string """
        return Vector(tuple((s.capitalize() if s is not None else None) for s in self._storage))

    def casefold(self):
        """ Call the internal casefold method on string """
        return Vector(tuple((s.casefold() if s is not None else None) for s in self._storage))

    def center(self, *args, **kwargs):
        """ Call the internal center method on string """
        return Vector(tuple((s.center(*args, **kwargs) if s is not None else None) for s in self._storage))

    def count(self, *args, **kwargs):
        """ Call the internal count method on string """
        return Vector(tuple((s.count(*args, **kwargs) if s is not None else None) for s in self._storage))

    def encode(self, *args, **kwargs):
        """ Call the internal encode method on string """
        return Vector(tuple((s.encode(*args, **kwargs) if s is not None else None) for s in self._storage))

    def endswith(self, *args, **kwargs):
        """ Call the internal endswith method on string """
        return Vector(tuple((s.endswith(*args, **kwargs) if s is not None else None) for s in self._storage))

    def expandtabs(self, *args, **kwargs):
        """ Call the internal expandtabs method on string """
        return Vector(tuple((s.expandtabs(*args, **kwargs) if s is not None else None) for s in self._storage))

    def find(self, *args, **kwargs):
        """ Call the internal find method on string """
        return Vector(tuple((s.find(*args, **kwargs) if s is not None else None) for s in self._storage))

    def format(self, *args, **kwargs):
        """ Call the internal format method on string """
        return Vector(tuple((s.format(*args, **kwargs) if s is not None else None) for s in self._storage))

    def format_map(self, *args, **kwargs):
        """ Call the internal format_map method on string """
        return Vector(tuple((s.format_map(*args, **kwargs) if s is not None else None) for s in self._storage))

    def index(self, *args, **kwargs):
        """ Call the internal index method on string """
        return Vector(tuple((s.index(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isalnum(self, *args, **kwargs):
        """ Call the internal isalnum method on string """
        return Vector(tuple((s.isalnum(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isalpha(self, *args, **kwargs):
        """ Call the internal isalpha method on string """
        return Vector(tuple((s.isalpha(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isascii(self, *args, **kwargs):
        """ Call the internal isascii method on string """
        return Vector(tuple((s.isascii(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isdecimal(self, *args, **kwargs):
        """ Call the internal isdecimal method on string """
        return Vector(tuple((s.isdecimal(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isdigit(self, *args, **kwargs):
        """ Call the internal isdigit method on string """
        return Vector(tuple((s.isdigit(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isidentifier(self, *args, **kwargs):
        """ Call the internal isidentifier method on string """
        return Vector(tuple((s.isidentifier(*args, **kwargs) if s is not None else None) for s in self._storage))

    def islower(self, *args, **kwargs):
        """ Call the internal islower method on string """
        return Vector(tuple((s.islower(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isnumeric(self, *args, **kwargs):
        """ Call the internal isnumeric method on string """
        return Vector(tuple((s.isnumeric(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isprintable(self, *args, **kwargs):
        """ Call the internal isprintable method on string """
        return Vector(tuple((s.isprintable(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isspace(self, *args, **kwargs):
        """ Call the internal isspace method on string """
        return Vector(tuple((s.isspace(*args, **kwargs) if s is not None else None) for s in self._storage))

    def istitle(self, *args, **kwargs):
        """ Call the internal istitle method on string """
        return Vector(tuple((s.istitle(*args, **kwargs) if s is not None else None) for s in self._storage))

    def isupper(self, *args, **kwargs):
        """ Call the internal isupper method on string """
        return Vector(tuple((s.isupper(*args, **kwargs) if s is not None else None) for s in self._storage))

    def join(self, *args, **kwargs):
        """ Call the internal join method on string """
        return Vector(tuple((s.join(*args, **kwargs) if s is not None else None) for s in self._storage))

    def ljust(self, *args, **kwargs):
        """ Call the internal ljust method on string """
        return Vector(tuple((s.ljust(*args, **kwargs) if s is not None else None) for s in self._storage))

    def lower(self, *args, **kwargs):
        """ Call the internal lower method on string """
        return Vector(tuple((s.lower(*args, **kwargs) if s is not None else None) for s in self._storage))

    def lstrip(self, *args, **kwargs):
        """ Call the internal lstrip method on string """
        return Vector(tuple((s.lstrip(*args, **kwargs) if s is not None else None) for s in self._storage))

    def maketrans(self, *args, **kwargs):
        """ Call the internal maketrans method on string """
        return Vector(tuple((s.maketrans(*args, **kwargs) if s is not None else None) for s in self._storage))

    def partition(self, *args, **kwargs):
        """ Call the internal partition method on string """
        return Vector(tuple((s.partition(*args, **kwargs) if s is not None else None) for s in self._storage))

    def removeprefix(self, *args, **kwargs):
        """ Call the internal removeprefix method on string """
        return Vector(tuple((s.removeprefix(*args, **kwargs) if s is not None else None) for s in self._storage))

    def removesuffix(self, *args, **kwargs):
        """ Call the internal removesuffix method on string """
        return Vector(tuple((s.removesuffix(*args, **kwargs) if s is not None else None) for s in self._storage))

    def replace(self, *args, **kwargs):
        """ Call the internal replace method on string """
        return Vector(tuple((s.replace(*args, **kwargs) if s is not None else None) for s in self._storage))

    def rfind(self, *args, **kwargs):
        """ Call the internal rfind method on string """
        return Vector(tuple((s.rfind(*args, **kwargs) if s is not None else None) for s in self._storage))

    def rindex(self, *args, **kwargs):
        """ Call the internal rindex method on string """
        return Vector(tuple((s.rindex(*args, **kwargs) if s is not None else None) for s in self._storage))

    def rjust(self, *args, **kwargs):
        """ Call the internal rjust method on string """
        return Vector(tuple((s.rjust(*args, **kwargs) if s is not None else None) for s in self._storage))

    def rpartition(self, *args, **kwargs):
        """ Call the internal rpartition method on string """
        return Vector(tuple((s.rpartition(*args, **kwargs) if s is not None else None) for s in self._storage))

    def rsplit(self, *args, **kwargs):
        """ Call the internal rsplit method on string """
        return Vector(tuple((s.rsplit(*args, **kwargs) if s is not None else None) for s in self._storage))

    def rstrip(self, *args, **kwargs):
        """ Call the internal rstrip method on string """
        return Vector(tuple((s.rstrip(*args, **kwargs) if s is not None else None) for s in self._storage))

    def split(self, *args, **kwargs):
        """ Call the internal split method on string """
        return Vector(tuple((s.split(*args, **kwargs) if s is not None else None) for s in self._storage))

    def splitlines(self, *args, **kwargs):
        """ Call the internal splitlines method on string """
        return Vector(tuple((s.splitlines(*args, **kwargs) if s is not None else None) for s in self._storage))

    def startswith(self, *args, **kwargs):
        """ Call the internal startswith method on string """
        return Vector(tuple((s.startswith(*args, **kwargs) if s is not None else None) for s in self._storage))

    def strip(self, *args, **kwargs):
        """ Call the internal strip method on string """
        return Vector(tuple((s.strip(*args, **kwargs) if s is not None else None) for s in self._storage))

    def swapcase(self, *args, **kwargs):
        """ Call the internal swapcase method on string """
        return Vector(tuple((s.swapcase(*args, **kwargs) if s is not None else None) for s in self._storage))

    def title(self, *args, **kwargs):
        """ Call the internal title method on string """
        return Vector(tuple((s.title(*args, **kwargs) if s is not None else None) for s in self._storage))

    def translate(self, *args, **kwargs):
        """ Call the internal translate method on string """
        return Vector(tuple((s.translate(*args, **kwargs) if s is not None else None) for s in self._storage))

    def upper(self, *args, **kwargs):
        """ Call the internal upper method on string """
        return Vector(tuple((s.upper(*args, **kwargs) if s is not None else None) for s in self._storage))

    def zfill(self, *args, **kwargs):
        """ Call the internal zfill method on string """
        return Vector(tuple((s.zfill(*args, **kwargs) if s is not None else None) for s in self._storage))

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

    def categorize(self, categories):
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
        _Categorical
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
        ``v.categorize(v.unique())`` is valid but uses first-seen order, which
        is rarely the intended sort order. Prefer an explicit list.
        """
        from .categorical import _Categorical
        return _Categorical.from_values(self._storage, categories, name=self._name)


