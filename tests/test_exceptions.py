import pytest
from serif import Vector
from serif import Table
from serif.errors import (
    SerifIndexError,
    SerifKeyError,
    SerifTypeError,
    SerifValueError,
)


def test_missing_column_raises_Vector_keyerror():
    t = Table({'a': [1, 2], 'b': [3, 4]})
    with pytest.raises(SerifKeyError):
        _ = t['missing']


def test_join_mismatched_lengths_raises_Vector_valueerror():
    left = Table({'id': [1, 2], 'date': ['a', 'b']})
    right = Table({'id': [2, 3]})
    with pytest.raises(SerifValueError):
        left.inner_join(right, left_on=['id', 'date'], right_on=['id'])


@pytest.mark.parametrize('values', ([1, 'two'], [1, None]))
def test_declared_dtype_mismatch_raises_exact_serif_type_error(values):
    with pytest.raises(SerifTypeError) as exc:
        Vector(values, dtype=int)

    assert type(exc.value) is SerifTypeError


def test_vector_truthiness_raises_exact_serif_type_error():
    with pytest.raises(SerifTypeError) as exc:
        bool(Vector([1]))

    assert type(exc.value) is SerifTypeError


def test_eager_table_cols_out_of_range_raises_exact_serif_index_error():
    table = Table({'a': [1, 2]})

    with pytest.raises(SerifIndexError) as exc:
        table.cols(1)

    assert type(exc.value) is SerifIndexError
