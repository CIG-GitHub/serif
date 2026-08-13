"""Operation-derived schemas for aggregates with no groups."""

from serif import Schema
from serif import Table
from serif import Vector


def _empty_source():
    return Table([
        Vector([], dtype=Schema(int, False), name='Group Key'),
        Vector([], dtype=Schema(int, True), name='Value'),
        Vector([], dtype=Schema(float, False), name='Score'),
        Vector([], dtype=Schema(str, True), name='Label'),
    ])


def test_empty_aggregate_derives_known_scalar_and_block_schemas():
    source = _empty_source()
    picked = source['Label', 'Score']

    result = source.aggregate(
        'Group Key',
        {
            'total': source.value.sum,
            'first_label': source.label.first,
            'mean_score': source.score.mean,
            'row_count': source.value.count,
            'verdict': source.value.all,
            'math_sum': source.score.math.fsum,
            'stats_mean': source.score.stats.mean,
            'group_size': len,
            'picked_': picked.first,
        },
    )

    assert result.shape == (0, 11)
    assert result.column_names() == [
        'Group Key',
        'total',
        'first_label',
        'mean_score',
        'row_count',
        'verdict',
        'math_sum',
        'stats_mean',
        'group_size',
        'picked_Label',
        'picked_Score',
    ]
    assert [column.schema() for column in result.cols()] == [
        Schema(int, False),
        Schema(int, False),
        Schema(str, True),
        Schema(float, False),
        Schema(int, False),
        Schema(bool, False),
        Schema(float, False),
        Schema(float, False),
        Schema(int, False),
        Schema(str, True),
        Schema(float, False),
    ]
    assert result.group_key.schema() == Schema(int, False)
    assert result.first_label.schema() == Schema(str, True)
    assert result.row_count.schema() == Schema(int, False)
    assert result.picked_label.schema() == Schema(str, True)
    assert result.picked_score.schema() == Schema(float, False)


def test_empty_aggregate_preserves_explicit_object_group_key_schema():
    source = Table([
        Vector(
            [],
            dtype=Schema(object, False),
            name='Object Key',
        ),
        Vector([], dtype=Schema(int, False), name='Value'),
    ])

    result = source.aggregate(
        'Object Key',
        {'row_count': source.value.count},
    )

    assert result.shape == (0, 2)
    assert result.object_key.schema() == Schema(object, False)
    assert result.row_count.schema() == Schema(int, False)


def test_unresolved_empty_custom_aggregate_remains_a_usable_column():
    source = _empty_source()
    result = source.aggregate(
        'Group Key',
        {'Mystery Value': lambda group: object()},
    )

    assert result.shape == (0, 2)
    assert result.column_names() == ['Group Key', 'Mystery Value']
    assert result.mystery_value.schema() is None

    selected = result['Mystery Value', 'Group Key']
    assert selected.shape == (0, 2)
    assert selected.column_names() == ['Mystery Value', 'Group Key']
    assert selected.mystery_value.schema() is None
    assert selected.group_key.schema() == Schema(int, False)

    renamed = result.rename_columns({'Mystery Value': 'Unknown Result'})
    assert renamed.shape == (0, 2)
    assert renamed.unknown_result.schema() is None
    assert renamed.group_key.schema() == Schema(int, False)

    joined = Table({'Group Key': [1]}).left_join(
        result,
        'Group Key',
        'Group Key',
    )
    assert joined.column_names() == ['Group Key', 'Mystery Value']
    assert list(joined.group_key) == [1]
    assert list(joined.mystery_value) == [None]
