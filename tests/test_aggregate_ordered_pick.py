"""
Ordered correlated row-picks in aggregate() via first_by(order)/last_by(order).

Unlike per-column reductions (.max fans out independent column maxima), an
ordered pick selects ONE row per group by the order key and emits that row's
value(s) — so a block's output row is internally consistent.
"""
import pytest
from serif import Table, Vector
from serif._vector import _OrderedPick


def _deals():
    return Table({
        'deal_id':   [1, 1, 2, 2, 2],
        'date':      ['2024-01', '2024-03', '2024-02', '2024-05', '2024-04'],
        'valuation': [100, 150, 200, 260, 240],
    })


# ---------------------------------------------------------------------------
# first_by / last_by build deferred specs, they do not compute standalone
# ---------------------------------------------------------------------------

def test_builders_return_specs():
    assert isinstance(Vector([1, 2, 3]).last_by('x'), _OrderedPick)
    assert isinstance(Table({'a': [1]}).first_by('a'), _OrderedPick)


# ---------------------------------------------------------------------------
# Scalar source
# ---------------------------------------------------------------------------

class TestScalarOrderedPick:
    def test_last_by_is_most_recent(self):
        deals = _deals()  # NOT pre-sorted
        res = deals.aggregate('deal_id', {'latest_val': deals.valuation.last_by('date')})
        by = {res.deal_id[i]: res.latest_val[i] for i in range(len(res))}
        assert by[1] == 150   # 2024-03
        assert by[2] == 260   # 2024-05

    def test_first_by_is_earliest(self):
        deals = _deals()
        res = deals.aggregate('deal_id', {'first_val': deals.valuation.first_by('date')})
        by = {res.deal_id[i]: res.first_val[i] for i in range(len(res))}
        assert by[1] == 100   # 2024-01
        assert by[2] == 200   # 2024-02

    def test_order_by_accepts_vector(self):
        t = Table({'g': ['a', 'a'], 'x': [10, 20]})
        key = Vector([5, 1])  # row with x=10 has the larger key
        res = t.aggregate('g', {'v': t.x.last_by(key)})
        assert res.v[0] == 10


# ---------------------------------------------------------------------------
# Block source — correlated fan-out
# ---------------------------------------------------------------------------

class TestBlockOrderedPick:
    def test_latest_block_no_presort(self):
        deals = _deals()
        res = deals.aggregate(
            'deal_id',
            {'latest_': deals['date', 'valuation'].last_by('date')},
        )
        assert set(res.column_names()) == {'deal_id', 'latest_date', 'latest_valuation'}
        by = {res.deal_id[i]: (res.latest_date[i], res.latest_valuation[i])
              for i in range(len(res))}
        assert by[1] == ('2024-03', 150)
        assert by[2] == ('2024-05', 260)

    def test_pick_is_correlated_not_columnwise_max(self):
        # THE distinction: the max valuation (999) is NOT on the latest date.
        # last_by('date') must return the valuation ON the latest-date row.
        deals = Table({
            'deal_id':   [1, 1, 1],
            'date':      ['2024-01', '2024-03', '2024-02'],
            'valuation': [100, 150, 999],  # 999 is on 2024-02, not the latest
        })
        res = deals.aggregate(
            'deal_id',
            {'latest_': deals['date', 'valuation'].last_by('date')},
        )
        assert res.latest_date[0] == '2024-03'
        assert res.latest_valuation[0] == 150   # NOT 999

    def test_multi_key_order(self):
        # tie on 'day' broken by 'val'; last_by → largest (day, val)
        t = Table({
            'g':   ['a', 'a', 'a'],
            'day': [1, 2, 2],
            'val': [10, 20, 30],
        })
        res = t.aggregate('g', {'pick_': t['day', 'val'].last_by(['day', 'val'])})
        assert res.pick_day[0] == 2
        assert res.pick_val[0] == 30


# ---------------------------------------------------------------------------
# Tie-breaking is stable (ties keep table row order)
# ---------------------------------------------------------------------------

class TestOrderedPickTies:
    def test_stable_ties(self):
        t = Table({'g': ['a', 'a', 'a'], 'k': [1, 1, 1], 'id': [100, 200, 300]})
        # all keys equal → stable sort preserves order
        last = t.aggregate('g', {'x': t.id.last_by('k')})
        first = t.aggregate('g', {'x': t.id.first_by('k')})
        assert last.x[0] == 300
        assert first.x[0] == 100


# ---------------------------------------------------------------------------
# window() does not support ordered picks yet — must reject, not misbehave
# ---------------------------------------------------------------------------

def test_window_rejects_ordered_pick():
    t = Table({'g': ['a', 'a'], 'x': [1, 2]})
    with pytest.raises(TypeError):
        t.window('g', {'v': t.x.last_by('x')})
