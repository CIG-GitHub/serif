"""
Table repr footer: grouped dtype summary.

The footer aggregates per-column dtypes into counted groups in column
order (first appearance), so the footer reads like the table does:

    # 1000000×2 table <int>
    # 1000000×3 table <str, int, date>
    # 1000000×9 table <6×str, 2×int, date>
    # 1000000×40 table <18×str, 12×int, 6×float, +4>

A count prefix appears only when a dtype repeats in a heterogeneous table.
With five or more dtype groups, the first three are shown and the
remaining columns fold into '+N'.
"""

from datetime import date

from serif import Table
from serif.display import _group_dtypes


def footer(t):
    return repr(t).splitlines()[-1]


# ---------------------------------------------------------------------------
# _group_dtypes unit tests
# ---------------------------------------------------------------------------

def test_homogeneous_omits_count():
    assert _group_dtypes(['int', 'int']) == 'int'
    assert _group_dtypes(['str'] * 40) == 'str'


def test_all_distinct_no_counts():
    assert _group_dtypes(['str', 'int', 'date']) == 'str, int, date'


def test_counts_shown_when_repeated():
    dtypes = ['str'] * 6 + ['int'] * 2 + ['date']
    assert _group_dtypes(dtypes) == '6×str, 2×int, date'


def test_column_order_not_count_order():
    # int appears first, so it leads even though str has more columns
    dtypes = ['int'] + ['str'] * 3 + ['int']
    assert _group_dtypes(dtypes) == '2×int, 3×str'


def test_interleaved_groups_keep_first_appearance_order():
    assert _group_dtypes(['int', 'str', 'int', 'str']) == '2×int, 2×str'
    assert _group_dtypes(['str', 'int', 'str', 'int']) == '2×str, 2×int'


def test_exactly_four_groups_all_shown():
    dtypes = ['str'] * 18 + ['int'] * 12 + ['float'] * 6 + ['date'] * 4
    assert _group_dtypes(dtypes) == '18×str, 12×int, 6×float, 4×date'


def test_five_or_more_groups_folds_into_plus_n():
    dtypes = ['str'] * 18 + ['int'] * 12 + ['float'] * 6 + ['date'] * 3 + ['bool']
    assert _group_dtypes(dtypes) == '18×str, 12×int, 6×float, +4'


def test_plus_n_can_hide_a_large_trailing_group():
    # Column order is authoritative even when the folded tail dominates
    dtypes = ['date', 'bool', 'category', 'int'] + ['str'] * 15
    assert _group_dtypes(dtypes) == 'date, bool, category, +16'


def test_nullable_is_a_distinct_group():
    assert _group_dtypes(['int', 'int?']) == 'int, int?'
    assert _group_dtypes(['int?', 'int?']) == 'int?'


# ---------------------------------------------------------------------------
# End-to-end through Table repr
# ---------------------------------------------------------------------------

def test_footer_homogeneous_table():
    t = Table({'a': [1, 2], 'b': [3, 4]})
    assert footer(t) == '# 2×2 table <int>'


def test_footer_heterogeneous_distinct():
    t = Table({
        'name': ['x', 'y'],
        'n': [1, 2],
        'd': [date(2026, 1, 1), date(2026, 1, 2)],
    })
    assert footer(t) == '# 2×3 table <str, int, date>'


def test_footer_grouped_counts():
    data = {f's{i}': ['a', 'b'] for i in range(6)}
    data.update({f'i{i}': [1, 2] for i in range(2)})
    data['d'] = [date(2026, 1, 1), date(2026, 1, 2)]
    t = Table(data)
    assert footer(t) == '# 2×9 table <6×str, 2×int, date>'


def test_footer_replaces_mixed():
    t = Table({'a': [1, 2], 'b': [0.5, 1.5]})
    assert footer(t) == '# 2×2 table <int, float>'
    assert 'mixed' not in repr(t)


def test_footer_nullable_dtypes():
    t = Table({'a': [1, None], 'b': [0.5, None]})
    assert footer(t) == '# 2×2 table <int?, float?>'
