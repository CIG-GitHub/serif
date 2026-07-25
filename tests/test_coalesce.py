"""Vector and Table null coalescing."""

from datetime import date
from datetime import datetime
from decimal import Decimal
import math

import pytest

from serif import Schema
from serif import SerifTypeError
from serif import SerifValueError
from serif import Table
from serif import Vector
from serif._vector.categorical import _Category
from serif._vector.storage import ArrayStorage
from serif._vector.storage import StringStorage
from serif._vector.storage import TupleStorage


def test_vector_coalesce_is_variadic_and_left_biased():
    primary = Vector([None, 2, None, None], name='value')
    second = Vector([1, None, None, None])
    third = Vector([9, 9, 3, None])

    result = primary.coalesce(second, third)

    assert list(result) == [1, 2, 3, None]
    assert result.schema() == Schema(int, True)
    assert result.vector_name == 'value'


def test_vector_coalesce_promotes_before_selecting_values():
    primary = Vector([1, None])
    fallback = Vector([2.5, 3.5])

    result = primary.coalesce(fallback)

    assert isinstance(result._storage, ArrayStorage)
    assert result._storage._data.typecode == 'd'
    assert result.schema() == Schema(float, False)
    assert list(result) == [1.0, 3.5]


def test_vector_coalesce_uses_existing_numeric_and_temporal_promotions():
    bools = Vector([True, None])
    integers = Vector([2, 3])
    dates = Vector([date(2024, 1, 1), None])
    datetimes = Vector([
        datetime(2024, 2, 1, 12),
        datetime(2024, 2, 2, 12),
    ])

    numeric = bools.coalesce(integers)
    temporal = dates.coalesce(datetimes)

    assert numeric.schema() == Schema(int, False)
    assert list(numeric) == [1, 3]
    assert temporal.schema() == Schema(datetime, False)
    assert list(temporal) == [
        datetime(2024, 1, 1),
        datetime(2024, 2, 2, 12),
    ]


def test_vector_coalesce_rejects_incompatible_families_up_front():
    primary = Vector([1, 2])
    fallback = Vector(['unused', 'values'])

    with pytest.raises(SerifTypeError, match="incompatible kinds: int, str"):
        primary.coalesce(fallback)

    decimals = Vector([Decimal('1.0'), None])
    with pytest.raises(SerifTypeError, match="incompatible kinds: Decimal, int"):
        decimals.coalesce(Vector([1, 2]))


def test_all_null_object_is_polymorphic_but_non_null_object_is_escape_hatch():
    nulls = Vector([None, None])
    integers = Vector([1, 2])
    objects = Vector(
        [None, {'source': 'object'}],
        dtype=Schema(object, True),
    )

    typed = nulls.coalesce(integers)
    heterogeneous = objects.coalesce(integers)

    assert typed.schema() == Schema(int, False)
    assert list(typed) == [1, 2]
    assert isinstance(heterogeneous._storage, TupleStorage)
    assert heterogeneous.schema() == Schema(object, False)
    assert list(heterogeneous) == [1, {'source': 'object'}]


def test_nan_is_a_value_not_a_null():
    primary = Vector([float('nan'), None])
    fallback = Vector([1.0, 2.0])

    result = primary.coalesce(fallback)

    assert math.isnan(result[0])
    assert result[1] == 2.0


def test_only_none_is_null():
    numbers = Vector([0, None]).coalesce(Vector([9, 9]))
    booleans = Vector([False, None]).coalesce(Vector([True, True]))
    strings = Vector(['', None]).coalesce(Vector(['fallback', 'value']))

    assert list(numbers) == [0, 9]
    assert list(booleans) == [False, True]
    assert list(strings) == ['', 'value']


def test_matching_categories_are_preserved():
    categories = ['low', 'mid', 'high']
    primary = Vector(
        ['low', None, None],
        name='grade',
    ).categorize(categories)
    second = Vector(
        ['high', 'mid', None],
    ).categorize(categories)
    third = Vector(
        ['mid', 'high', 'high'],
    ).categorize(categories)

    result = primary.coalesce(second, third)

    assert isinstance(result, _Category)
    assert result.categories == tuple(categories)
    assert result.vector_name == 'grade'
    assert result.schema() == Schema(str, False)
    assert list(result) == ['low', 'mid', 'high']


@pytest.mark.parametrize(
    "fallback",
    [
        Vector(['high', 'mid']).categorize(['high', 'mid', 'low']),
        Vector(['high', 'mid']),
    ],
    ids=['different-categories', 'plain-string'],
)
def test_category_domain_mismatch_demotes_to_string(fallback):
    primary = Vector(['low', None]).categorize(['low', 'mid', 'high'])

    result = primary.coalesce(fallback)

    assert not isinstance(result, _Category)
    assert isinstance(result._storage, StringStorage)
    assert result.schema() == Schema(str, False)
    assert list(result) == ['low', 'mid']


def test_vector_coalesce_validates_arguments_and_lengths():
    vector = Vector([1, None])

    with pytest.raises(SerifValueError, match="at least one fallback"):
        vector.coalesce()
    with pytest.raises(SerifTypeError, match="argument 1 is int"):
        vector.coalesce(0)
    with pytest.raises(SerifValueError, match="length mismatch"):
        vector.coalesce(Vector([1]))
    with pytest.raises(SerifTypeError, match="argument 1 is Table"):
        vector.coalesce(Table({'x': [1, 2]}))


def test_empty_untyped_vectors_coalesce_without_inventing_a_dtype():
    primary = Vector([], name='empty')

    result = primary.coalesce(Vector([]))

    assert result.schema() is None
    assert result.vector_name == 'empty'
    assert len(result) == 0


def test_table_coalesce_is_variadic_positional_and_left_owned():
    primary = Table(
        [
            Vector([None, 2], name='amount'),
            Vector(['left', None], name='label'),
        ],
        name='primary',
    )
    second = Table(
        [
            Vector([1, None], name='swapped_label'),
            Vector([None, 'middle'], name=None),
        ],
        name='second',
    )
    third = Table(
        [
            Vector([9, 9], name=None),
            Vector(['right', 'right'], name='swapped_amount'),
        ],
    )

    result = primary.coalesce(second, third)

    assert result.table_name == 'primary'
    assert result.column_names() == ['amount', 'label']
    assert list(result.amount) == [1, 2]
    assert list(result.label) == ['left', 'middle']


def test_table_coalesce_allows_unnamed_primary_and_named_fallback():
    primary = Table([
        Vector([None, 2]),
        Vector([None, 'left']),
    ])
    fallback = Table({
        'amount': [1, 9],
        'label': ['right', 'right'],
    })

    result = primary.coalesce(fallback)

    assert result.column_names() == [None, None]
    assert list(result.cols(0)) == [1, 2]
    assert list(result.cols(1)) == ['right', 'left']


def test_table_coalesce_promotes_each_position_independently():
    primary = Table([
        Vector([1, None], name='number'),
        Vector(['a', None], name='text'),
    ])
    fallback = Table([
        Vector([2.5, 3.5]),
        Vector(['b', 'c']),
    ])

    result = primary.coalesce(fallback)

    assert result.number.schema() == Schema(float, False)
    assert list(result.number) == [1.0, 3.5]
    assert result.text.schema() == Schema(str, False)
    assert list(result.text) == ['a', 'c']


def test_table_coalesce_validates_argument_type_and_exact_shape():
    table = Table({'x': [None, 2]})

    with pytest.raises(SerifValueError, match="at least one fallback"):
        table.coalesce()
    with pytest.raises(SerifTypeError, match="argument 1 is Vector"):
        table.coalesce(Vector([1, 2]))
    with pytest.raises(SerifValueError, match="shape mismatch"):
        table.coalesce(Table({'x': [1]}))
    with pytest.raises(SerifValueError, match="shape mismatch"):
        table.coalesce(Table({'x': [1, 2], 'y': [3, 4]}))
