import pytest
from serif import Vector
from serif import Table
from serif.errors import SerifKeyError, SerifValueError


def test_missing_column_raises_Vector_keyerror():
    t = Table({'a': [1, 2], 'b': [3, 4]})
    with pytest.raises(SerifKeyError):
        _ = t['missing']


def test_join_mismatched_lengths_raises_Vector_valueerror():
    left = Table({'id': [1, 2], 'date': ['a', 'b']})
    right = Table({'id': [2, 3]})
    with pytest.raises(SerifValueError):
        left.inner_join(right, left_on=['id', 'date'], right_on=['id'])




