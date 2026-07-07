"""
Tests for first()/last() and block (fan-out) aggregations in aggregate(),
plus the flat-only contract (non-scalar cells must raise).
"""
import pytest
from serif import Table, Vector


# ---------------------------------------------------------------------------
# Vector.first / Vector.last — standalone, positional (NOT null-skipping)
# ---------------------------------------------------------------------------

class TestFirstLastScalar:
    def test_first_last_basic(self):
        v = Vector([10, 20, 30])
        assert v.first() == 10
        assert v.last() == 30

    def test_empty_returns_none(self):
        assert Vector([]).first() is None
        assert Vector([]).last() is None

    def test_positional_not_null_skipping(self):
        # first() is positional: a leading None yields None, it does NOT skip
        assert Vector([None, 5, 6]).first() is None
        assert Vector([5, 6, None]).last() is None

    def test_dropna_then_first_skips_nulls(self):
        # the explicit way to get the first non-null
        assert Vector([None, 5, 6]).dropna().first() == 5


# ---------------------------------------------------------------------------
# Block first/last — returns a row (one value per column)
# ---------------------------------------------------------------------------

class TestFirstLastBlock:
    def test_block_first_is_first_row(self):
        t = Table({'a': [1, 2, 3], 'b': [10, 20, 30]})
        assert list(t.first()) == [1, 10]

    def test_block_last_is_last_row(self):
        t = Table({'a': [1, 2, 3], 'b': [10, 20, 30]})
        assert list(t.last()) == [3, 30]

    def test_selection_block_last(self):
        t = Table({'a': [1, 2, 3], 'b': [10, 20, 30], 'c': [100, 200, 300]})
        assert list(t['a', 'c'].last()) == [3, 300]


# ---------------------------------------------------------------------------
# aggregate() block fan-out — the "most recent event per group" workflow
# ---------------------------------------------------------------------------

class TestAggregateBlockFanout:
    def _deals(self):
        return Table({
            'deal_id':   [1, 1, 2, 2, 2],
            'date':      ['2024-01', '2024-03', '2024-02', '2024-05', '2024-04'],
            'valuation': [100, 150, 200, 260, 240],
        })

    def test_latest_block_per_group(self):
        ts = self._deals().sort_by('date')  # ascending → .last = most recent
        res = ts.aggregate(
            groupby='deal_id',
            aggregations={'latest_': ts['date', 'valuation'].last},
        )
        # fan-out produces one column per selected column, raw-prefixed
        assert set(res.column_names()) == {'deal_id', 'latest_date', 'latest_valuation'}
        by_deal = {res.deal_id[i]: (res.latest_date[i], res.latest_valuation[i])
                   for i in range(len(res))}
        assert by_deal[1] == ('2024-03', 150)
        assert by_deal[2] == ('2024-05', 260)

    def test_earliest_block_per_group(self):
        ts = self._deals().sort_by('date')  # ascending → .first = earliest
        res = ts.aggregate(
            groupby='deal_id',
            aggregations={'first_': ts['date', 'valuation'].first},
        )
        by_deal = {res.deal_id[i]: (res.first_date[i], res.first_valuation[i])
                   for i in range(len(res))}
        assert by_deal[1] == ('2024-01', 100)
        assert by_deal[2] == ('2024-02', 200)

    def test_block_sum_fans_out(self):
        t = Table({
            'region': ['N', 'S', 'N'],
            'sales':  [100, 200, 150],
            'costs':  [60, 120, 90],
        })
        res = t.aggregate(
            groupby='region',
            aggregations={'total_': t['sales', 'costs'].sum},
        )
        by_region = {res.region[i]: (res.total_sales[i], res.total_costs[i])
                     for i in range(len(res))}
        assert by_region['N'] == (250, 150)
        assert by_region['S'] == (200, 120)

    def test_prefix_is_raw_prepend_no_separator_magic(self):
        # prefix without a trailing underscore prepends literally.
        # NOTE: t['x',] (trailing comma → 1-tuple) forces a 1-wide *block*;
        # t['x'] (plain string) would be a column → scalar path, named 'peak'.
        t = Table({'g': ['a', 'a'], 'x': [1, 2]})
        res = t.aggregate(groupby='g', aggregations={'peak': t['x',].max})
        assert 'peakx' in res.column_names()

    def test_block_and_scalar_together(self):
        ts = self._deals().sort_by('date')
        res = ts.aggregate(
            groupby='deal_id',
            aggregations={
                'n':       ts.valuation.count,
                'latest_': ts['date', 'valuation'].last,
            },
        )
        by_deal = {res.deal_id[i]: res.n[i] for i in range(len(res))}
        assert by_deal[1] == 2
        assert by_deal[2] == 3
        assert 'latest_valuation' in res.column_names()


# ---------------------------------------------------------------------------
# Flat-only contract — non-scalar cells must puke
# ---------------------------------------------------------------------------

class TestFlatOnly:
    def test_scalar_agg_returning_vector_raises(self):
        # .unique() returns a Vector — not a scalar → puke
        t = Table({'g': ['a', 'a', 'b'], 'x': [1, 1, 2]})
        with pytest.raises(TypeError, match="flat-only"):
            t.aggregate(groupby='g', aggregations={'u': t.x.unique})

    def test_callable_returning_vector_raises(self):
        t = Table({'g': ['a', 'a', 'b'], 'x': [1, 1, 2]})
        with pytest.raises(TypeError, match="flat-only"):
            t.aggregate(groupby='g', aggregations={'bad': lambda grp: grp.x})

    def test_scalar_aggregations_still_work(self):
        # regression guard: the flat-only check must not reject legit scalars
        t = Table({'g': ['a', 'a', 'b'], 'x': [1, 2, 3]})
        res = t.aggregate(groupby='g', aggregations={'s': t.x.sum})
        by_g = {res.g[i]: res.s[i] for i in range(len(res))}
        assert by_g['a'] == 3
        assert by_g['b'] == 3
