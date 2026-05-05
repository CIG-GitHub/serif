import warnings
from serif import Vector
from serif import Table


def test_aggregate_over_no_warnings_and_correct_keys():
    year = Vector([2020, 2020, 2021, 2021], name='year')
    month = Vector([1, 2, 1, 2], name='month')
    val = Vector([10, 20, 30, 40], name='val')

    table = Table([year, month, val])

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        res = table.aggregate(
            groupby=[table.year, table.month],
            aggregations={'val_sum': table.val.sum}
        )

    assert len(w) == 0, f"Unexpected warnings: {[str(x.message) for x in w]}"
    assert len(res) == 4
    expected_keys = {(year[i], month[i]) for i in range(len(year))}
    actual_keys = set(zip(res['year']._storage, res['month']._storage))
    assert actual_keys == expected_keys




