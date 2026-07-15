"""
Ordered / correlated row-picks in aggregate.

There is no ordering argument on first/last — the idiom is "sort, then pick."
A stable global sort carries into every group (a group is a subsequence of the
sorted table), so pre-sorting then using positional first/last yields a
correlated pick: a block's whole output row comes from one record.
"""
from serif import Table


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
