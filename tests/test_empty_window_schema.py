"""Operation-derived schemas for windows over empty inputs."""

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


def test_empty_window_derives_known_reducer_schemas():
    source = _empty_source()

    result = source.window(
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
        },
    )

    assert result.shape == (0, 9)
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
    ]
    assert result.group_key.schema() == Schema(int, False)
    assert result.first_label.schema() == Schema(str, True)
    assert result.row_count.schema() == Schema(int, False)


def test_empty_window_preserves_group_schema_and_unresolved_output():
    source = Table([
        Vector([], dtype=Schema(object, False), name='Object Key'),
        Vector([], dtype=Schema(int, False), name='Unused Value'),
    ])

    result = source.window(
        'Object Key',
        {'Mystery Value': lambda group: object()},
    )

    assert result.shape == (0, 2)
    assert result.column_names() == ['Object Key', 'Mystery Value']
    assert result.object_key.schema() == Schema(object, False)
    assert result.mystery_value.schema() is None

    selected = result['Mystery Value', 'Object Key']
    assert selected.shape == (0, 2)
    assert selected.column_names() == ['Mystery Value', 'Object Key']
    assert selected.mystery_value.schema() is None
    assert selected.object_key.schema() == Schema(object, False)

    renamed = result.rename_columns({'Mystery Value': 'Unknown Result'})
    assert renamed.shape == (0, 2)
    assert renamed.unknown_result.schema() is None
    assert renamed.object_key.schema() == Schema(object, False)
