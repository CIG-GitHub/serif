import warnings
from serif import Vector
from serif import Table


def test_aggregate_over_no_warnings_and_correct_keys():
    # Create a small table with year/month partition keys
    year = Vector([2020, 2020, 2021, 2021], name='year')
    month = Vector([1, 2, 1, 2], name='month')
    val = Vector([10, 20, 30, 40], name='val')

    table = Table([year, month, val])

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        res = table.aggregate(over=[year, month], sum_over=val)

    # No warnings should have been raised
    assert len(w) == 0, f"Unexpected warnings: {[str(x.message) for x in w]}"

    # Result should have 4 rows (unique year,month pairs)
    assert len(res) == 4

    # Partition key columns should match unique combinations
    res_year = res['year']
    res_month = res['month']
    expected_keys = {(year[i], month[i]) for i in range(len(year))}
    actual_keys = set(zip(res_year._storage, res_month._storage))
    assert actual_keys == expected_keys




