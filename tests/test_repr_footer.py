"""
Table repr footer: conceptual type-family summary.

The footer is NOT a schema dump — it orients: what am I looking at, what
kinds of data are present, is anything surprising? Exact per-column schema
already lives in the header tags and t._, so the footer is deliberately
lossy: one entry per type FAMILY, most-common first (ties keep column
order), no counts (the R×C prefix carries scale), and no folding — the
family vocabulary is closed, so the line is intrinsically bounded.

Per-family marker — bare, '?', or '*':
    int     every column of the family has the same schema, no nulls
    int?    same schema, nullable
    int*    heterogeneous: ANY within-family mix (nullability, decimal
            scale/precision) — one symbol, one meaning: "see t._"

    # 2×2 table <int>
    # 2×3 table <str, int?, date>
    # 2×4 table <float, int*, str>
"""

from datetime import date
from decimal import Decimal

from serif import Table, Vector
from serif.display import _family_summary
from serif._vector.categorical import _Category
from serif._vector.dtype import Schema
from serif._vector.storage import DecimalStorage


def footer(t):
    return repr(t).splitlines()[-1]


def summary(data):
    return _family_summary(Table(data).cols())


def _decimal_col(name, values, scale, precision):
    """A Decimal column backed by DecimalStorage (as the parquet reader
    builds), so scale/precision participate in the family-flavor check."""
    storage = DecimalStorage.from_iterable(values, scale, precision)
    return Vector._from_storage(storage, Schema(Decimal, False), name=name)


# ---------------------------------------------------------------------------
# _family_summary unit tests
# ---------------------------------------------------------------------------

def test_homogeneous_family():
    assert summary({'a': [1, 2], 'b': [3, 4]}) == 'int'
    assert summary({f's{i}': ['a', 'b'] for i in range(40)}) == 'str'


def test_uniformly_nullable_family_keeps_question_mark():
    assert summary({'a': [1, None], 'b': [None, 2]}) == 'int?'


def test_mixed_nullability_stars():
    assert summary({'a': [1, 2], 'b': [3, None]}) == 'int*'


def test_no_counts_ever():
    data = {f's{i}': ['a', 'b'] for i in range(6)}
    data.update({f'i{i}': [1, 2] for i in range(2)})
    data['d'] = [date(2026, 1, 1), date(2026, 1, 2)]
    assert summary(data) == 'str, int, date'


def test_most_common_family_first():
    # str has more columns, so it leads even though int appears first.
    data = {'i1': [1, 2]}
    data.update({f's{i}': ['a', 'b'] for i in range(3)})
    data['i2'] = [3, 4]
    assert summary(data) == 'str, int'


def test_ties_keep_first_appearance_order():
    assert summary({'a': [1], 'b': ['x'], 'c': [2], 'd': ['y']}) == 'int, str'
    assert summary({'a': ['x'], 'b': [1], 'c': ['y'], 'd': [2]}) == 'str, int'


def test_no_folding_every_family_shown():
    # The old footer folded 6+ distinct dtypes into '...+N'. Families need
    # no cap: the vocabulary is closed, so every family always shows.
    cols = Table({
        's': ['a'], 'i': [1], 'f': [1.5], 'b': [True],
        'd': [date(2026, 1, 1)],
    }).cols()
    cols = list(cols) + [
        _decimal_col('amt', [Decimal('1.25')], 2, 3),
        _Category.from_values(['x'], ['x', 'y'], name='c'),
    ]
    assert _family_summary(cols) == 'str, int, float, bool, date, Decimal, category'


def test_category_is_its_own_family():
    cols = [
        Vector(['plain'], name='s'),
        _Category.from_values(['x', 'y'], ['x', 'y'], name='c'),
    ]
    # One str column and one category column: neither collapses into the other.
    assert _family_summary(cols) == 'str, category'


def test_decimal_uniform_scale_is_bare():
    cols = [
        _decimal_col('a', [Decimal('1.25')], 2, 3),
        _decimal_col('b', [Decimal('2.50')], 2, 3),
    ]
    assert _family_summary(cols) == 'Decimal'


def test_decimal_mixed_scale_stars():
    cols = [
        _decimal_col('a', [Decimal('1.25')], 2, 3),
        _decimal_col('b', [Decimal('0.00000001')], 8, 9),
    ]
    assert _family_summary(cols) == 'Decimal*'


# ---------------------------------------------------------------------------
# End-to-end through Table repr
# ---------------------------------------------------------------------------

def test_footer_homogeneous_table():
    t = Table({'a': [1, 2], 'b': [3, 4]})
    assert footer(t) == '# 2×2 table <int>'


def test_footer_heterogeneous_families():
    t = Table({
        'name': ['x', 'y'],
        'n': [1, 2],
        'd': [date(2026, 1, 1), date(2026, 1, 2)],
    })
    assert footer(t) == '# 2×3 table <str, int, date>'


def test_footer_no_counts_for_repeated_families():
    data = {f's{i}': ['a', 'b'] for i in range(6)}
    data.update({f'i{i}': [1, 2] for i in range(2)})
    data['d'] = [date(2026, 1, 1), date(2026, 1, 2)]
    t = Table(data)
    assert footer(t) == '# 2×9 table <str, int, date>'


def test_footer_replaces_mixed():
    t = Table({'a': [1, 2], 'b': [0.5, 1.5]})
    assert footer(t) == '# 2×2 table <int, float>'
    assert 'mixed' not in repr(t)


def test_footer_uniformly_nullable():
    t = Table({'a': [1, None], 'b': [0.5, None]})
    assert footer(t) == '# 2×2 table <int?, float?>'


def test_footer_mixed_nullability_stars():
    t = Table({'a': [1, 2], 'b': [3, None], 'c': [0.5, 1.5]})
    assert footer(t) == '# 2×3 table <int*, float>'
