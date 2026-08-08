"""Constant reductions preserve values, schemas, and packed storage."""

import pytest

from serif import Schema
from serif import Table
from serif import Vector
from serif._vector.storage import ArrayStorage
from serif._vector.storage import StringStorage
from serif._vector.storage import TupleStorage


def test_grouped_constant_reduction_emits_once_per_group():
    table = Table({
        'group': ['A', 'B', 'A'],
        'amount': [10, 20, 30],
    })

    result = table.aggregate(
        'group',
        {
            'label': table.constant('Total'),
            'amount': table.amount.sum,
        },
    )

    assert list(result.group) == ['A', 'B']
    assert list(result.label) == ['Total', 'Total']
    assert list(result.amount) == [40, 20]
    assert result.label.schema() == Schema(str, False)
    assert isinstance(result.label._storage, StringStorage)


def test_constant_reduction_is_available_from_a_vector():
    table = Table({'amount': [10, 20, 30]})

    result = table.aggregate({
        'label': table.amount.constant('Grand Total'),
        'amount': table.amount.sum,
    })

    assert list(result.label) == ['Grand Total']
    assert list(result.amount) == [60]


def test_window_broadcasts_constant_reduction():
    table = Table({
        'group': ['A', 'B', 'A'],
        'amount': [10, 20, 30],
    })

    result = table.window(
        'group',
        {'label': table.constant('Grouped')},
    )

    assert list(result.label) == ['Grouped', 'Grouped', 'Grouped']
    assert result.label.schema() == Schema(str, False)
    assert isinstance(result.label._storage, StringStorage)


def test_constant_resolves_dtype_and_nullability_before_aggregation():
    table = Table({'group': ['A', 'B']})

    result = table.aggregate(
        'group',
        {
            'nullable_int': table.constant(None, dtype=int),
            'untyped_null': table.constant(None),
            'coerced_float': table.constant(1, dtype=float),
            'widened_int': table.constant(1, nullable=True),
        },
    )

    assert list(result.nullable_int) == [None, None]
    assert result.nullable_int.schema() == Schema(int, True)
    assert isinstance(result.nullable_int._storage, ArrayStorage)

    assert list(result.untyped_null) == [None, None]
    assert result.untyped_null.schema() == Schema(object, True)
    assert isinstance(result.untyped_null._storage, TupleStorage)

    assert list(result.coerced_float) == [1.0, 1.0]
    assert result.coerced_float.schema() == Schema(float, False)
    assert isinstance(result.coerced_float._storage, ArrayStorage)

    assert list(result.widened_int) == [1, 1]
    assert result.widened_int.schema() == Schema(int, True)


@pytest.mark.parametrize(
    'reduction, message',
    [
        (
            lambda table: table.constant(
                None,
                dtype=int,
                nullable=False,
            ),
            'Cannot store None in non-nullable int column',
        ),
        (
            lambda table: table.constant('one', dtype=int),
            "Incompatible value 'one' for column<int>",
        ),
        (
            lambda table: table.constant(1, nullable='sometimes'),
            'nullable must be True, False, or None',
        ),
    ],
)
def test_constant_rejects_impossible_schema(reduction, message):
    table = Table({'value': [1]})

    with pytest.raises(TypeError, match=message):
        reduction(table)


def test_empty_grouped_constant_keeps_declared_schema_and_storage():
    table = Table({'group': Vector([], dtype=str)})

    result = table.aggregate(
        'group',
        {'value': table.constant(None, dtype=int)},
    )

    assert len(result) == 0
    assert result.value.schema() == Schema(int, True)
    assert isinstance(result.value._storage, ArrayStorage)

