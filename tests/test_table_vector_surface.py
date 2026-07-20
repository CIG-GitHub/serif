"""Conformance for Vector methods intentionally exposed on Table."""

import pytest

from serif import SerifTypeError, Table, Vector


def test_fillna_maps_over_cells_and_preserves_names():
    t = Table({'a': [1, None], 'b': [None, 4.0]})

    result = t.fillna(0)

    assert result.to_dict() == {'a': [1, 0], 'b': [0.0, 4.0]}
    assert t.to_dict() == {'a': [1, None], 'b': [None, 4.0]}


def test_isna_returns_same_shaped_named_table():
    t = Table({'a': [1, None], 'b': [None, 4]})

    result = t.isna()

    assert result.shape == t.shape
    assert result.to_dict() == {
        'a': [False, True],
        'b': [True, False],
    }


def test_dropna_keeps_complete_rows():
    t = Table({
        'a': [1, None, 3, 4],
        'b': ['x', 'y', None, 'z'],
    })

    assert t.dropna().to_dict() == {'a': [1, 4], 'b': ['x', 'z']}


def test_unique_is_stable_and_row_wise():
    t = Table({'a': [1, 1, 2, 1], 'b': ['x', 'x', 'y', 'z']})

    assert t.unique().to_dict() == {
        'a': [1, 2, 1],
        'b': ['x', 'y', 'z'],
    }


def test_cast_and_to_object_map_over_columns():
    t = Table({'a': [1, 2], 'b': [3, 4]})

    cast = t.cast(float)
    obj = t.to_object()

    assert cast.to_dict() == {'a': [1.0, 2.0], 'b': [3.0, 4.0]}
    assert all(col.schema().kind is float for col in cast.cols())
    assert all(col.schema().kind is object for col in obj.cols())


def test_is_type_and_pluck_map_over_cells():
    t = Table({
        'a': Vector([{'x': 1}, {'x': 2}]).to_object(),
        'b': Vector([{'x': 3}, None]).to_object(),
    })

    assert t.is_type(dict).to_dict() == {
        'a': [True, True],
        'b': [True, False],
    }
    assert t.pluck('x').to_dict() == {'a': [1, 2], 'b': [3, None]}


@pytest.mark.parametrize(
    ('operation', 'expected'),
    [
        (lambda t: -t, {'a': [-1, 2], 'b': [-3, -4]}),
        (lambda t: +t, {'a': [1, -2], 'b': [3, 4]}),
        (lambda t: abs(t), {'a': [1, 2], 'b': [3, 4]}),
        (lambda t: 10 + t, {'a': [11, 8], 'b': [13, 14]}),
        (lambda t: 10 - t, {'a': [9, 12], 'b': [7, 6]}),
        (lambda t: 12 / t, {'a': [12.0, -6.0], 'b': [4.0, 3.0]}),
    ],
)
def test_unary_and_reverse_arithmetic_preserve_table(operation, expected):
    t = Table({'a': [1, -2], 'b': [3, 4]})

    result = operation(t)

    assert result.shape == t.shape
    assert result.column_names() == ['a', 'b']
    assert result.to_dict() == expected


def test_invert_and_bit_shift_preserve_names():
    flags = Table({'a': [True, False], 'b': [False, True]})
    ints = Table({'a': [1, 2], 'b': [4, 8]})

    assert (~flags).to_dict() == {
        'a': [False, True],
        'b': [True, False],
    }
    assert ints.bit_lshift(1).to_dict() == {'a': [2, 4], 'b': [8, 16]}


def test_table_filled_rejects_ambiguous_construction():
    with pytest.raises(SerifTypeError, match="named filled columns"):
        Table.filled(0, 3)
