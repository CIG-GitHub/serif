"""
Ordered / correlated row-picks in aggregate.

There is no ordering argument on first/last — the idiom is "sort, then pick."
A stable global sort carries into every group (a group is a subsequence of the
sorted table), so pre-sorting then using positional first/last yields a
correlated pick: a block's whole output row comes from one record.
"""
from serif import Table


def _deals():
    return Table({
        'deal_id':   [1, 1, 2, 2, 2],
        'date':      ['2024-01', '2024-03', '2024-02', '2024-05', '2024-04'],
        'valuation': [100, 150, 200, 260, 240],
    })


def test_most_recent_block_per_group():
    ts = _deals().sort_by('date')  # ascending → positional .last = most recent
    res = ts.aggregate('deal_id', {'latest_': ts['date', 'valuation'].last})
    by = {res.deal_id[i]: (res.latest_date[i], res.latest_valuation[i])
          for i in range(len(res))}
    assert by[1] == ('2024-03', 150)
    assert by[2] == ('2024-05', 260)


def test_earliest_block_per_group():
    ts = _deals().sort_by('date')  # ascending → positional .first = earliest
    res = ts.aggregate('deal_id', {'first_': ts['date', 'valuation'].first})
    by = {res.deal_id[i]: (res.first_date[i], res.first_valuation[i])
          for i in range(len(res))}
    assert by[1] == ('2024-01', 100)
    assert by[2] == ('2024-02', 200)


def test_pick_is_correlated_not_columnwise_max():
    # Max valuation (999) is NOT on the latest date. Pre-sort by date, then
    # positional .last returns the valuation ON the latest-date row, not the max.
    deals = Table({
        'deal_id':   [1, 1, 1],
        'date':      ['2024-01', '2024-03', '2024-02'],
        'valuation': [100, 150, 999],
    })
    ts = deals.sort_by('date')
    res = ts.aggregate('deal_id', {'latest_': ts['date', 'valuation'].last})
    assert res.latest_date[0] == '2024-03'
    assert res.latest_valuation[0] == 150   # NOT 999
