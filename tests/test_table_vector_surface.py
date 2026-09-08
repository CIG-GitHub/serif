"""Conformance for Vector methods intentionally exposed on Table."""

import warnings

import pytest

from serif import Schema, SerifEmptyReductionWarning, SerifTypeError, Table, Vector


def test_fillna_maps_over_cells_and_preserves_names():
    t = Table({'a': [1, None], 'b': [None, 4.0]})

    result = t.fillna(0)

    assert result.to_dict() == {'a': [1, 0], 'b': [0.0, 4.0]}
    assert t.to_dict() == {'a': [1, None], 'b': [None, 4.0]}


def test_is_na_returns_same_shaped_named_table():
    t = Table({'a': [1, None], 'b': [None, 4]})

    result = t.is_na()

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


def test_is_type_maps_over_cells():
    t = Table({
        'a': Vector([{'x': 1}, {'x': 2}]).to_object(),
        'b': Vector([{'x': 3}, None]).to_object(),
    })

    assert t.is_type(dict).to_dict() == {
        'a': [True, True],
        'b': [True, False],
    }
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


@pytest.mark.parametrize('method, expected', [
    ('all', [None, False, None]),
    ('any', [True, None, None]),
])
def test_table_verdicts_fold_each_column(method, expected):
    table = Table({
        'a': [True, None],
        'b': [False, None],
        'c': [None, None],
    })
    with warnings.catch_warnings():
        warnings.simplefilter('error', SerifEmptyReductionWarning)
        result = getattr(table, method)()
    assert result.ndims() == 1
    assert list(result) == expected
    assert result.schema() == Schema(bool, True)


@pytest.mark.parametrize('method', ['all', 'any'])
def test_table_unknown_verdicts_remain_logically_composable(method):
    table = Table({'a': [None], 'b': [None]})
    with warnings.catch_warnings():
        warnings.simplefilter('error', SerifEmptyReductionWarning)
        result = getattr(table, method)(on_empty=True)
    assert list(result) == [None, None]
    assert result.schema() == Schema(bool, True)
    assert list(result & False) == [False, False]
    assert list(result | True) == [True, True]


@pytest.mark.parametrize('method, identity', [('all', True), ('any', False)])
def test_table_empty_columns_use_empty_verdict_policy(method, identity):
    table = Table({'a': [], 'b': []})
    with pytest.warns(SerifEmptyReductionWarning):
        result = getattr(table, method)()
    assert list(result) == [identity, identity]
    assert result.schema() == Schema(bool, False)
    with warnings.catch_warnings():
        warnings.simplefilter('error', SerifEmptyReductionWarning)
        result = getattr(table, method)(on_empty=not identity)
    assert list(result) == [not identity, not identity]


@pytest.mark.parametrize('method', ['all', 'any'])
def test_table_without_columns_reduces_to_empty_bool_vector(method):
    with warnings.catch_warnings():
        warnings.simplefilter('error', SerifEmptyReductionWarning)
        result = getattr(Table({}), method)()
    assert list(result) == []
    assert result.schema() == Schema(bool, False)
