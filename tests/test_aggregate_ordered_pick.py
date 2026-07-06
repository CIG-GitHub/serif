"""
Ordered / correlated row-picks.

Two ways to get "the row that sorts first/last by some key":
  1. Standalone: first(order_by=...) / last(order_by=...) sort by the key and
     return that element (1-D) or row (block). Correlated — a block's whole
     row comes from one record.
  2. In aggregate: pre-sort the table, then use positional first/last. A stable
     global sort carries into every group, so no per-group ordering code exists.
"""
import pytest
from serif import Table, Vector


def _deals():
    return Table({
        'deal_id':   [1, 1, 2, 2, 2],
        'date':      ['2024-01', '2024-03', '2024-02', '2024-05', '2024-04'],
        'valuation': [100, 150, 200, 260, 240],
    })


# ---------------------------------------------------------------------------
# Standalone order_by on a block (column-name key)
# ---------------------------------------------------------------------------

class TestBlockOrderBy:
    def test_last_with_order_by_name(self):
        block = _deals()['date', 'valuation']
        assert list(block.last(order_by='date')) == ['2024-05', 260]

    def test_first_with_order_by_name(self):
        block = _deals()['date', 'valuation']
        assert list(block.first(order_by='date')) == ['2024-01', 100]

    def test_multi_key_order(self):
        t = Table({'day': [1, 2, 2], 'val': [10, 20, 30]})
        # sort by (day, val); last → (2, 30)
        assert list(t['day', 'val'].last(order_by=['day', 'val'])) == [2, 30]

    def test_order_by_none_is_positional(self):
        block = _deals()['date', 'valuation']
        assert list(block.first()) == ['2024-01', 100]   # first row as stored
        assert list(block.last()) == ['2024-04', 240]    # last row as stored


# ---------------------------------------------------------------------------
# Standalone order_by on a 1-D vector via an external key Vector.
# This is genuinely additive: sort_by() sorts a vector by its OWN values, so
# "the valuation on the latest date" can't be expressed without order_by.
# ---------------------------------------------------------------------------

class TestVectorOrderBy:
    def test_scalar_pick_by_external_key(self):
        deals = _deals()
        assert deals.valuation.last(order_by=deals.date) == 260   # 2024-05 (max date)
        assert deals.valuation.first(order_by=deals.date) == 100  # 2024-01 (min date)

    def test_string_order_by_on_bare_vector_raises(self):
        with pytest.raises(TypeError, match="column name"):
            Vector([1, 2, 3]).first(order_by='x')


# ---------------------------------------------------------------------------
# In-aggregate ordered pick = pre-sort + positional first/last
# ---------------------------------------------------------------------------

class TestPreSortAggregate:
    def test_most_recent_block_per_group(self):
        ts = _deals().sort_by('date')  # ascending → positional .last = most recent
        res = ts.aggregate('deal_id', {'latest_': ts['date', 'valuation'].last})
        by = {res.deal_id[i]: (res.latest_date[i], res.latest_valuation[i])
              for i in range(len(res))}
        assert by[1] == ('2024-03', 150)
        assert by[2] == ('2024-05', 260)

    def test_pick_is_correlated_not_columnwise_max(self):
        # Max valuation (999) is NOT on the latest date. Pre-sort by date, then
        # positional .last must return the valuation ON the latest-date row.
        deals = Table({
            'deal_id':   [1, 1, 1],
            'date':      ['2024-01', '2024-03', '2024-02'],
            'valuation': [100, 150, 999],
        })
        ts = deals.sort_by('date')
        res = ts.aggregate('deal_id', {'latest_': ts['date', 'valuation'].last})
        assert res.latest_date[0] == '2024-03'
        assert res.latest_valuation[0] == 150   # NOT 999
