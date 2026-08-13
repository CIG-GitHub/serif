"""Join schemas are independent of whether any row pairs are emitted."""

import pytest

from serif import Schema
from serif import Table
from serif import Vector


def _left(keys):
    return Table([
        Vector(keys, dtype=Schema(int, False), name='Join Key'),
        Vector(
            [f'left-{key}' for key in keys],
            dtype=Schema(str, True),
            name='Left Value',
        ),
    ])


def _right(keys):
    return Table([
        Vector(keys, dtype=Schema(int, False), name='Join Key'),
        Vector(
            [float(key) for key in keys],
            dtype=Schema(float, False),
            name='Right Value',
        ),
    ])


@pytest.mark.parametrize(
    'flavor, left_keys, right_keys, expected_rows, expected_schemas',
    [
        (
            'inner_join',
            [],
            [1],
            0,
            [Schema(int, False), Schema(str, True), Schema(float, False)],
        ),
        (
            'inner_join',
            [1],
            [],
            0,
            [Schema(int, False), Schema(str, True), Schema(float, False)],
        ),
        (
            'inner_join',
            [],
            [],
            0,
            [Schema(int, False), Schema(str, True), Schema(float, False)],
        ),
        (
            'left_join',
            [],
            [1],
            0,
            [Schema(int, False), Schema(str, True), Schema(float, True)],
        ),
        (
            'left_join',
            [1],
            [],
            1,
            [Schema(int, False), Schema(str, True), Schema(float, True)],
        ),
        (
            'left_join',
            [],
            [],
            0,
            [Schema(int, False), Schema(str, True), Schema(float, True)],
        ),
        (
            'full_join',
            [],
            [1],
            1,
            [Schema(int, True), Schema(str, True), Schema(float, True)],
        ),
        (
            'full_join',
            [1],
            [],
            1,
            [Schema(int, True), Schema(str, True), Schema(float, True)],
        ),
        (
            'full_join',
            [],
            [],
            0,
            [Schema(int, True), Schema(str, True), Schema(float, True)],
        ),
    ],
)
def test_join_with_empty_inputs_preserves_output_schema(
    flavor,
    left_keys,
    right_keys,
    expected_rows,
    expected_schemas,
):
    result = getattr(_left(left_keys), flavor)(
        _right(right_keys),
        'Join Key',
        'Join Key',
    )

    assert result.shape == (expected_rows, 3)
    assert result.column_names() == [
        'Join Key',
        'Left Value',
        'Right Value',
    ]
    assert [column.schema() for column in result.cols()] == expected_schemas
    assert result.join_key.schema() == expected_schemas[0]
    assert result.left_value.schema() == expected_schemas[1]
    assert result.right_value.schema() == expected_schemas[2]


def test_unmatched_inner_join_preserves_output_schema():
    result = _left([1]).inner_join(
        _right([2]),
        'Join Key',
        'Join Key',
    )

    assert result.shape == (0, 3)
    assert result.column_names() == [
        'Join Key',
        'Left Value',
        'Right Value',
    ]
    assert [column.schema() for column in result.cols()] == [
        Schema(int, False),
        Schema(str, True),
        Schema(float, False),
    ]


def test_empty_join_keeps_differently_named_keys():
    left = Table([
        Vector([], dtype=Schema(int, False), name='Left Key'),
        Vector([], dtype=Schema(str, False), name='Left Value'),
    ])
    right = Table([
        Vector([], dtype=Schema(int, False), name='Right Key'),
        Vector([], dtype=Schema(float, False), name='Right Value'),
    ])

    result = left.inner_join(right, 'Left Key', 'Right Key')

    assert result.shape == (0, 4)
    assert result.column_names() == [
        'Left Key',
        'Left Value',
        'Right Key',
        'Right Value',
    ]
    assert [column.schema() for column in result.cols()] == [
        Schema(int, False),
        Schema(str, False),
        Schema(int, False),
        Schema(float, False),
    ]
    assert result.left_key.schema() == Schema(int, False)
    assert result.right_key.schema() == Schema(int, False)
