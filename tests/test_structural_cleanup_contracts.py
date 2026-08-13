"""Public behavior that the structural cleanup must preserve."""

from datetime import date

import pytest

from serif import Schema
from serif import SerifTypeError
from serif import Table
from serif import Vector
from serif._vector.storage import ArrayStorage
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


@pytest.mark.parametrize(
    'values,storage_type',
    [
        ([1, None, 3], ArrayStorage),
        ([1.5, None, 3.5], ArrayStorage),
        (['a', None, 'c'], StringStorage),
    ],
)
def test_owner_writes_preserve_canonical_storage(values, storage_type):
    table = Table({'value': values})
    replacement = next(value for value in values if value is not None)

    table[1, 'value'] = replacement

    assert isinstance(table.value._storage, storage_type)
    assert list(table.value) == [values[0], replacement, values[2]]
