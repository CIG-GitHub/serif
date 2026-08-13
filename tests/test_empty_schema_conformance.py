"""End-to-end zero-row schema conformance across execution backends."""

from contextlib import contextmanager

import pytest

from serif import Schema
from serif import Table
from serif import Vector
from serif._table._arrow import aggregation as arrow_aggregation
from serif._table._arrow import grouping as arrow_grouping
from serif._table._arrow import joins as arrow_joins
from serif._table._numpy import grouping as numpy_grouping
from serif._table._numpy import joins as numpy_joins
from serif._vector._numpy import selection as numpy_selection


_NUMPY_MODULES = (numpy_grouping, numpy_joins, numpy_selection)
_ARROW_MODULES = (arrow_aggregation, arrow_grouping, arrow_joins)
_NUMPY_AVAILABLE = all(module._USE_NUMPY for module in _NUMPY_MODULES)
_ARROW_AVAILABLE = (
    _NUMPY_AVAILABLE
    and all(module._USE_ARROW for module in _ARROW_MODULES)
)


@contextmanager
def _backend(name):
    modules = [
        *((module, '_USE_NUMPY') for module in _NUMPY_MODULES),
        *((module, '_USE_ARROW') for module in _ARROW_MODULES),
    ]
    saved = [getattr(module, attribute) for module, attribute in modules]
    use_numpy = name in ('numpy', 'pyarrow')
    use_arrow = name == 'pyarrow'
    try:
        for module in _NUMPY_MODULES:
            module._USE_NUMPY = use_numpy
        for module in _ARROW_MODULES:
            module._USE_ARROW = use_arrow
        yield
    finally:
        for (module, attribute), value in zip(modules, saved):
            setattr(module, attribute, value)


def _keys(kind, left=True):
    if kind is int:
        return [1, 2] if left else [3, 4]
    return ['a', 'b'] if left else ['c', 'd']


def _source(key_kind):
    keys = _keys(key_kind)
    return Table([
        Vector(keys, dtype=Schema(key_kind, False), name='Group Key'),
        Vector([10, None], dtype=Schema(int, True), name='Metric Value'),
        Vector([1.5, 2.5], dtype=Schema(float, False), name='Score'),
        Vector([True, False], dtype=Schema(bool, False), name='Enabled'),
        Vector(['A', None], dtype=Schema(str, True), name='Display Label'),
        Vector([1, 2], dtype=Schema(int, False), name='Row Count'),
        Vector([None, 0.5], dtype=Schema(float, True), name='Ratio'),
        Vector(['x', 'y'], dtype=Schema(str, False), name='Category'),
    ])


def _run_pipeline(key_kind):
    source = _source(key_kind)
    zero_by_eight = source[Vector([False, False])]
    projection = zero_by_eight['Display Label', 'Group Key']

    zero_by_two = zero_by_eight.aggregate(
        'Group Key',
        {'Metric Sum': zero_by_eight.metric_value.sum},
    )
    windowed = zero_by_eight.window(
        'Group Key',
        {'Metric Mean': zero_by_eight.metric_value.mean},
    )
    renamed = zero_by_two.rename_columns({
        'Group Key': 'Segment Key',
        'Metric Sum': 'Total Metric',
    })
    appended = renamed << Table({
        'ignored key': [],
        'ignored metric': [],
    })

    dimension = Table([
        Vector(
            _keys(key_kind, left=False),
            dtype=Schema(key_kind, False),
            name='Segment Key',
        ),
        Vector(
            ['C', 'D'],
            dtype=Schema(str, False),
            name='Dimension Label',
        ),
    ])
    chained = appended.left_join(
        dimension,
        'Segment Key',
        'Segment Key',
    )

    probe_left = Table([
        Vector(
            _keys(key_kind),
            dtype=Schema(key_kind, False),
            name='Probe Key',
        ),
        Vector([10, 20], dtype=Schema(int, False), name='Left Payload'),
    ])
    probe_right = Table([
        Vector(
            _keys(key_kind, left=False),
            dtype=Schema(key_kind, False),
            name='Probe Key',
        ),
        Vector(
            [1.5, 2.5],
            dtype=Schema(float, False),
            name='Right Payload',
        ),
    ])
    unmatched_join = probe_left.inner_join(
        probe_right,
        'Probe Key',
        'Probe Key',
    )
    empty_full_join = probe_left[:0].full_join(
        probe_right[:0],
        'Probe Key',
        'Probe Key',
    )

    return {
        'zero_by_eight': zero_by_eight,
        'projection': projection,
        'zero_by_two': zero_by_two,
        'windowed': windowed,
        'renamed': renamed,
        'appended': appended,
        'chained': chained,
        'unmatched_join': unmatched_join,
        'empty_full_join': empty_full_join,
    }


def _assert_schema(table, names, schemas, attributes):
    assert table.shape == (0, len(names))
    assert table.column_names() == names
    assert [column.schema() for column in table.cols()] == schemas
    for index, attribute in enumerate(attributes):
        assert getattr(table, attribute) is table.cols(index)


def _assert_contract(results, key_kind):
    key_schema = Schema(key_kind, False)
    _assert_schema(
        results['zero_by_eight'],
        [
            'Group Key',
            'Metric Value',
            'Score',
            'Enabled',
            'Display Label',
            'Row Count',
            'Ratio',
            'Category',
        ],
        [
            key_schema,
            Schema(int, True),
            Schema(float, False),
            Schema(bool, False),
            Schema(str, True),
            Schema(int, False),
            Schema(float, True),
            Schema(str, False),
        ],
        [
            'group_key',
            'metric_value',
            'score',
            'enabled',
            'display_label',
            'row_count',
            'ratio',
            'category',
        ],
    )
    _assert_schema(
        results['projection'],
        ['Display Label', 'Group Key'],
        [Schema(str, True), key_schema],
        ['display_label', 'group_key'],
    )
    _assert_schema(
        results['zero_by_two'],
        ['Group Key', 'Metric Sum'],
        [key_schema, Schema(int, False)],
        ['group_key', 'metric_sum'],
    )
    _assert_schema(
        results['windowed'],
        ['Group Key', 'Metric Mean'],
        [key_schema, Schema(float, True)],
        ['group_key', 'metric_mean'],
    )
    for name in ('renamed', 'appended'):
        _assert_schema(
            results[name],
            ['Segment Key', 'Total Metric'],
            [key_schema, Schema(int, False)],
            ['segment_key', 'total_metric'],
        )
    _assert_schema(
        results['chained'],
        ['Segment Key', 'Total Metric', 'Dimension Label'],
        [key_schema, Schema(int, False), Schema(str, True)],
        ['segment_key', 'total_metric', 'dimension_label'],
    )
    _assert_schema(
        results['unmatched_join'],
        ['Probe Key', 'Left Payload', 'Right Payload'],
        [key_schema, Schema(int, False), Schema(float, False)],
        ['probe_key', 'left_payload', 'right_payload'],
    )
    _assert_schema(
        results['empty_full_join'],
        ['Probe Key', 'Left Payload', 'Right Payload'],
        [Schema(key_kind, True), Schema(int, True), Schema(float, True)],
        ['probe_key', 'left_payload', 'right_payload'],
    )


def _assert_identical(expected, actual):
    assert actual.keys() == expected.keys()
    for name in expected:
        expected_table = expected[name]
        actual_table = actual[name]
        assert type(actual_table) is type(expected_table)
        assert actual_table.shape == expected_table.shape
        assert actual_table.column_names() == expected_table.column_names()
        for expected_column, actual_column in zip(
            expected_table.cols(),
            actual_table.cols(),
        ):
            assert actual_column.vector_name == expected_column.vector_name
            assert actual_column.schema() == expected_column.schema()
            assert type(actual_column._storage) is type(expected_column._storage)
            assert list(actual_column) == list(expected_column)


def test_pure_python_empty_schema_pipeline_contract():
    with _backend('python'):
        results = _run_pipeline(int)

    _assert_contract(results, int)


@pytest.mark.parametrize(
    'backend, key_kind, available',
    [
        ('numpy', int, _NUMPY_AVAILABLE),
        ('pyarrow', str, _ARROW_AVAILABLE),
    ],
)
def test_accelerated_empty_schema_pipeline_conforms(
    backend,
    key_kind,
    available,
):
    if not available:
        pytest.skip(f'{backend} backend is not available')

    with _backend('python'):
        expected = _run_pipeline(key_kind)
    with _backend(backend):
        actual = _run_pipeline(key_kind)

    _assert_identical(expected, actual)
    _assert_contract(actual, key_kind)
