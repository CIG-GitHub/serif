"""
Table repr footer: grouped dtype summary.

The footer summarizes per-column dtypes as 'type:count' pairs, most common
first, so it reads as an at-a-glance dominance summary:

    # 1000000×2 table <int>
    # 1000000×3 table <str, int, date>
    # 1000000×9 table <str:6, int:2, date>
    # 1000000×40 table <str:18, int:12, float:6, date:4>
    # 1000000×95 table <str:50, int:20, float:10, date:5 ...+2>

A count of one is dropped (':1' is noise), and a homogeneous table drops the
count entirely — the total already lives in the R×C prefix. Ties keep column
(first-appearance) order. With six or more distinct dtypes, the first four are
shown and the rest fold into '..., +N'.
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
    # Every dtype appears once, so counts are dropped; ties keep column order.
    assert _group_dtypes(['str', 'int', 'date']) == 'str, int, date'


def test_counts_shown_when_repeated():
    dtypes = ['str'] * 6 + ['int'] * 2 + ['date']
    assert _group_dtypes(dtypes) == 'str:6, int:2, date'


def test_count_descending_order():
    # str has more columns, so it leads even though int appears first.
    dtypes = ['int'] + ['str'] * 3 + ['int']
    assert _group_dtypes(dtypes) == 'str:3, int:2'


def test_ties_keep_first_appearance_order():
    assert _group_dtypes(['int', 'str', 'int', 'str']) == 'int:2, str:2'
    assert _group_dtypes(['str', 'int', 'str', 'int']) == 'str:2, int:2'


def test_four_groups_all_shown():
    dtypes = ['str'] * 18 + ['int'] * 12 + ['float'] * 6 + ['date'] * 4
    assert _group_dtypes(dtypes) == 'str:18, int:12, float:6, date:4'


def test_five_groups_all_shown():
    # Five distinct dtypes still fit — folding would leave a banned '+1'.
    dtypes = ['str'] * 18 + ['int'] * 12 + ['float'] * 6 + ['date'] * 3 + ['bool']
    assert _group_dtypes(dtypes) == 'str:18, int:12, float:6, date:3, bool'


def test_six_or_more_groups_folds_into_plus_n():
    dtypes = (['str'] * 18 + ['int'] * 12 + ['float'] * 6 + ['date'] * 4
              + ['bool'] * 2 + ['category'])
    assert _group_dtypes(dtypes) == 'str:18, int:12, float:6, date:4 ...+2'


def test_fold_hides_the_rarest_groups():
    # The folded tail is always the smallest groups (descending order).
    dtypes = (['str'] * 50 + ['int'] * 20 + ['float'] * 10 + ['date'] * 5
              + ['bool'] * 3 + ['category'] * 2)
    assert _group_dtypes(dtypes) == 'str:50, int:20, float:10, date:5 ...+2'


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
    assert footer(t) == '# 2×9 table <str:6, int:2, date>'


def test_footer_replaces_mixed():
    t = Table({'a': [1, 2], 'b': [0.5, 1.5]})
    assert footer(t) == '# 2×2 table <int, float>'
    assert 'mixed' not in repr(t)


def test_footer_nullable_dtypes():
    t = Table({'a': [1, None], 'b': [0.5, None]})
    assert footer(t) == '# 2×2 table <int?, float?>'
