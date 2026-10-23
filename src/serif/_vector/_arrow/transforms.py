"""Optional Arrow physical implementations for Vector transforms."""

from ..._execution import DECLINED
from . import _pa
from . import _pc
from . import _USE_ARROW
from .storage import bool_storage
from .storage import string_array
from ..storage import BoolStorage
from ..storage import StringStorage


def is_in_strings(storage, members, match_null):
    """String membership as BoolStorage, or DECLINED.

    `members` are the non-None members; only exact str members compare
    like Python's `==` here. Null lanes are True when match_null, else null.
    """
    if not _USE_ARROW or not isinstance(storage, StringStorage):
        return DECLINED
    if any(type(member) is not str for member in members):
        return DECLINED
    values = string_array(storage)
    if values is DECLINED:
        return DECLINED

    value_set = _pa.array(members, type=_pa.string())
    hits = _pc.fill_null(_pc.is_in(values, value_set=value_set), False)

    mask = storage._mask
    if mask is not None and mask.any_null():
        if match_null:
            hits = _pc.or_(hits, _pc.is_null(values))
            mask = None
    else:
        mask = None
    return BoolStorage(bool_storage(hits)._data, mask)
