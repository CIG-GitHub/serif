"""Schema contracts for zero-row selection, append, and rename."""

from serif import Schema
from serif import Table
from serif import Vector
from serif.table import MaskedTable


def _typed_table():
    return Table([
        Vector(
            [10, 20],
            dtype=Schema(int, False),
            name='Order ID',
        ),
        Vector(
            [1.5, None],
            dtype=Schema(float, True),
            name='Net Amount',
        ),
        Vector(
            ['A', 'B'],
            dtype=Schema(str, False),
            name='Display Name',
        ),
    ])


def _assert_schema(table, names, schemas):
    assert table.shape == (0, len(names))
    assert table.column_names() == names
    assert [column.schema() for column in table.cols()] == schemas


def test_zero_row_slice_preserves_column_schema_and_accessors():
    result = _typed_table()[:0]

    _assert_schema(
        result,
        ['Order ID', 'Net Amount', 'Display Name'],
        [
            Schema(int, False),
            Schema(float, True),
            Schema(str, False),
        ],
    )
    assert result.order_id is result.cols(0)
    assert result.net_amount is result.cols(1)
    assert result.display_name is result.cols(2)


def test_zero_row_masked_table_preserves_column_schema_and_accessors():
    source = _typed_table()
    result = source[source.order_id > 99]

    assert isinstance(result, MaskedTable)
    _assert_schema(
        result,
        ['Order ID', 'Net Amount', 'Display Name'],
        [
            Schema(int, False),
            Schema(float, True),
            Schema(str, False),
        ],
    )
    assert result.order_id.schema() == Schema(int, False)
    assert result.net_amount.schema() == Schema(float, True)
    assert result.display_name.schema() == Schema(str, False)


def test_zero_row_mask_then_projection_preserves_selected_order_and_schema():
    source = _typed_table()

    result = source[
        source.order_id > 99,
        ('Display Name', 'Order ID'),
    ]

    _assert_schema(
        result,
        ['Display Name', 'Order ID'],
        [Schema(str, False), Schema(int, False)],
    )
    assert result.display_name.schema() == Schema(str, False)
    assert result.order_id.schema() == Schema(int, False)


def test_zero_row_rename_preserves_schema_and_rebuilds_accessors():
    result = _typed_table()[:0].rename_columns({
        'Order ID': 'Primary ID',
        'Net Amount': 'Net Total',
    })

    _assert_schema(
        result,
        ['Primary ID', 'Net Total', 'Display Name'],
        [
            Schema(int, False),
            Schema(float, True),
            Schema(str, False),
        ],
    )
    assert result.primary_id is result.cols(0)
    assert result.net_total is result.cols(1)
    assert result.display_name is result.cols(2)


def test_vector_append_resolves_schema_from_nonempty_known_side():
    result = Vector([], name='left') << Vector(
        [1, None],
        dtype=Schema(int, True),
        name='right',
    )

    assert list(result) == [1, None]
    assert result.vector_name == 'left'
    assert result.schema() == Schema(int, True)

    reverse = Vector(
        [1, 2],
        dtype=Schema(int, False),
        name='left',
    ) << Vector([], name='right')

    assert list(reverse) == [1, 2]
    assert reverse.vector_name == 'left'
    assert reverse.schema() == Schema(int, False)


def test_vector_append_resolves_schema_when_both_sides_are_empty():
    known_right = Vector([], dtype=Schema(float, True), name='right')
    from_right = Vector([], name='left') << known_right

    assert len(from_right) == 0
    assert from_right.vector_name == 'left'
    assert from_right.schema() == Schema(float, True)

    known_left = Vector([], dtype=Schema(str, False), name='left')
    from_left = known_left << Vector([], name='right')

    assert len(from_left) == 0
    assert from_left.vector_name == 'left'
    assert from_left.schema() == Schema(str, False)


def test_table_append_adopts_known_schema_from_populated_right_table():
    unresolved = Table({
        'Order ID': [],
        'Net Amount': [],
    })
    known = Table([
        Vector([1, 2], dtype=Schema(int, False), name='right_id'),
        Vector(
            [1.5, None],
            dtype=Schema(float, True),
            name='right_amount',
        ),
    ])

    result = unresolved << known

    assert result.column_names() == ['Order ID', 'Net Amount']
    assert result.shape == (2, 2)
    assert list(result.order_id) == [1, 2]
    assert list(result.net_amount) == [1.5, None]
    assert result.order_id.schema() == Schema(int, False)
    assert result.net_amount.schema() == Schema(float, True)


def test_table_append_ignores_unresolved_empty_right_schema():
    known = Table([
        Vector([1, 2], dtype=Schema(int, False), name='Order ID'),
        Vector(
            [1.5, None],
            dtype=Schema(float, True),
            name='Net Amount',
        ),
    ])
    unresolved = Table({
        'ignored id': [],
        'ignored amount': [],
    })

    result = known << unresolved

    assert result.column_names() == ['Order ID', 'Net Amount']
    assert result.shape == (2, 2)
    assert list(result.order_id) == [1, 2]
    assert list(result.net_amount) == [1.5, None]
    assert result.order_id.schema() == Schema(int, False)
    assert result.net_amount.schema() == Schema(float, True)


def test_table_append_preserves_known_schema_when_both_tables_are_empty():
    known = Table([
        Vector([], dtype=Schema(int, False), name='Order ID'),
        Vector([], dtype=Schema(float, True), name='Net Amount'),
    ])
    unresolved = Table({
        'ignored id': [],
        'ignored amount': [],
    })

    result = known << unresolved

    _assert_schema(
        result,
        ['Order ID', 'Net Amount'],
        [Schema(int, False), Schema(float, True)],
    )
    assert result.order_id.schema() == Schema(int, False)
    assert result.net_amount.schema() == Schema(float, True)
