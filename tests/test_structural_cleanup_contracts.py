"""Public behavior that the structural cleanup must preserve."""

from datetime import date
import warnings

import pytest

from serif import Schema
from serif import SerifTypeError
from serif import Table
from serif import Vector
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage
from serif._vector.storage import StringStorage


def test_retained_dtype_capabilities_preserve_values_and_schemas():
    integers = Vector([1, None, 4])
    floats = Vector([1.0, None, 2.5])
    strings = Vector(['alpha', None, 'Beta'])
    dates = Vector([date(2024, 2, 10), None])

    results = (
        (integers.bit_length(), [1, None, 3], Schema(int, True)),
        (integers.real, [1, None, 4], Schema(int, True)),
        (floats.is_integer(), [True, None, False], Schema(bool, True)),
        (floats.hex(), [float.hex(1.0), None, float.hex(2.5)], Schema(str, True)),
        (strings.upper(), ['ALPHA', None, 'BETA'], Schema(str, True)),
        (dates.year, [2024, None], Schema(int, True)),
        (dates.isoformat(), ['2024-02-10', None], Schema(str, True)),
    )

    for result, expected_values, expected_schema in results:
        assert list(result) == expected_values
        assert result.schema() == expected_schema


def test_table_ownership_is_one_public_mutability_boundary():
    table = Table({'value': [1, 2, 3]})
    owned = table.value

    with pytest.raises(SerifTypeError, match='owning table'):
        owned[0] = 9
    with pytest.raises(SerifTypeError, match='metadata is frozen'):
        owned.alias('renamed')

    detached = owned.copy().alias('renamed')
    detached[0] = 9

    assert detached.vector_name == 'renamed'
    assert list(detached) == [9, 2, 3]
    assert table.column_names() == ['value']
    assert list(table.value) == [1, 2, 3]


def test_ownership_replaces_independent_mutability_flags():
    table = Table({'value': [1, 2, 3]})

    assert not hasattr(Vector, '_frozen')
    assert not hasattr(Vector, '_inplace_ok')
    assert not hasattr(Table, '_unlocked')
    assert table.value._owner() is table

    with table.batch() as editable:
        token = editable._batch_edit
        assert token is not None
        assert editable.value._owner is token

    assert token.active is False
    assert table._batch_edit is None
    assert table.value._owner() is table


def test_value_writes_leave_table_attribute_metadata_cached():
    table = Table({'value': [1, 2, 3], 'label': ['a', 'b', 'c']})
    column_map = table._column_map

    assert table.value[0] == 1
    assert table.label[0] == 'a'
    assert table._column_map is column_map

    table[0, 'value'] = 9

    assert table._column_map is column_map
    assert table.value[0] == 9
    assert table.label[0] == 'a'


def test_column_replacement_leaves_name_metadata_cached():
    table = Table({'value': [1, 2, 3]})
    column_map = table._column_map

    table.value = Vector([4, 5, 6])

    assert table._column_map is column_map
    assert table.column_names() == ['value']
    assert list(table.value) == [4, 5, 6]


def test_batch_rename_refreshes_metadata_at_the_write_point():
    table = Table({'value': [1, 2, 3]})
    previous_map = table._column_map

    with table.batch() as editable:
        editable.value.alias('renamed')

        assert editable._column_map is not previous_map
        assert list(editable.renamed) == [1, 2, 3]
        with pytest.raises(AttributeError):
            _ = editable.value

    assert table.column_names() == ['renamed']
    assert list(table.renamed) == [1, 2, 3]


def test_batch_rename_warns_only_for_duplicates_it_creates():
    with pytest.warns(UserWarning, match='Duplicate column name'):
        table = Table([
            Vector([1], name='x'),
            Vector([2], name='x'),
            Vector([3], name='other'),
        ])

    with pytest.warns(UserWarning, match='Duplicate column name') as captured:
        with table.batch() as editable:
            editable.other.alias('x')

    assert len(captured) == 1


def test_batch_rename_does_not_rewarn_unrelated_duplicates():
    with pytest.warns(UserWarning, match='Duplicate column name'):
        table = Table([
            Vector([1], name='x'),
            Vector([2], name='x'),
            Vector([3], name='other'),
        ])

    with warnings.catch_warnings():
        warnings.simplefilter('error')
        with table.batch() as editable:
            editable.other.alias('renamed')

    assert list(table.renamed) == [3]


def test_metadata_reads_do_not_rebuild_the_cached_map(monkeypatch):
    table = Table({'value': [1, 2, 3]})

    def unexpected_rebuild(_table):
        raise AssertionError('metadata read rebuilt column map')

    monkeypatch.setattr(
        'serif._table.columns.build_column_map',
        unexpected_rebuild,
    )

    assert list(table.value) == [1, 2, 3]
    assert 'value' in dir(table)
    assert table.column_names() == ['value']


def test_wild_metadata_state_is_removed():
    assert not hasattr(Vector, '_wild')
    assert not hasattr(Table, '_wild')


@pytest.mark.parametrize(
    'values,storage_type',
    [
        ([1, None, 3], ArrayStorage),
        ([1.5, None, 3.5], ArrayStorage),
        ([True, None, False], BoolStorage),
        (['a', None, 'c'], StringStorage),
    ],
)
def test_owner_writes_preserve_canonical_storage(values, storage_type):
    table = Table({'value': values})
    replacement = next(value for value in values if value is not None)

    table[1, 'value'] = replacement

    assert isinstance(table.value._storage, storage_type)
    assert list(table.value) == [values[0], replacement, values[2]]
