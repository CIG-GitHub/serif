# Joins & Aggregations

## Joins

Table supports three join types: `inner_join`, `join` (left join), and `full_join`.

### Inner Join

Returns only rows with matching keys in both tables.

```python
left = Table({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
right = Table({'id': [2, 3, 4], 'score': [85, 90, 95]})

result = left.inner_join(right, left_on='id', right_on='id')
# Returns rows for id 2 and 3
```

### Left Join

Returns all rows from the left table, with `None` for unmatched right table values.

```python
result = left.join(right, left_on='id', right_on='id')
# Returns all 3 left rows; id=1 has None for score
```

### Full Outer Join

Returns all rows from both tables, with `None` for unmatched values.

```python
result = left.full_join(right, left_on='id', right_on='id')
# Returns 4 rows (1, 2, 3, 4)
```

### Multiple Keys

```python
result = left.inner_join(right, 
    left_on=['year', 'month'], 
    right_on=['year', 'month'])
```

### Cardinality Expectations

Validate join behavior with the `expect` parameter:

```python
result = left.inner_join(right, 
    left_on='id', 
    right_on='customer_id',
    expect='many_to_one')  # Validates right side has unique keys
```

**Complexity:** O(n + m) where n and m are table lengths. Uses hash-based lookups.

---

## Aggregations

### Aggregate: Group and Summarize

Returns **one row per group**.

```python
t = Table({
    'customer': ['A', 'B', 'A', 'C', 'B'],
    'amount': [100, 200, 150, 300, 250]
})

result = t.aggregate(
    groupby=t.customer,
    aggregations={
        'amount_sum': t.amount.sum,
        'amount_mean': t.amount.mean,
        'amount_count': t.amount.count,
    }
)
# Returns 3 rows (one per unique customer)
# Columns: customer, amount_sum, amount_mean, amount_count
```

Output column names are always explicit — whatever keys you provide in `aggregations`.

To aggregate the entire table without grouping, omit `groupby` (or pass `None`):

```python
result = t.aggregate(aggregations={'grand_total': t.amount.sum})
# Returns a single-row Table
```

### Window: Running Aggregations

Returns **same row count** as input, with aggregated values broadcast back per group.

```python
result = t.window(
    groupby=t.customer,
    aggregations={'amount_sum': t.amount.sum}
)
# Returns 5 rows (original row count)
# Each row gets the group's total in amount_sum
```

### Multiple Partition Keys

```python
result = t.aggregate(
    groupby=[t.year, t.month],
    aggregations={'total_revenue': t.revenue.sum}
)
```

### Custom Aggregation

Pass any callable as an aggregation value. It receives the group as a `Table` and must return a scalar.

```python
from functools import reduce
from operator import mul

result = t.aggregate(
    groupby=t.category,
    aggregations={'product': lambda group: reduce(mul, group.value, 1)}
)
```

**Complexity:** O(n) to build partitions, then O(n × k) where k is cost per group aggregation.

---

## Common Patterns

### Join + Aggregate

```python
# Join sales and customers, then aggregate by region
sales = Table({'customer_id': [1, 2, 1, 3], 'amount': [100, 200, 150, 300]})
customers = Table({'id': [1, 2, 3], 'region': ['East', 'West', 'East']})

joined = sales.join(customers, left_on='customer_id', right_on='id')
result = joined.aggregate(
    groupby=joined.region,
    aggregations={'total': joined.amount.sum}
)
```

### Window for Running Totals

```python
t = Table({
    'date': [1, 2, 3, 4, 5],
    'sales': [100, 200, 150, 300, 250]
})

# Window over entire table (single partition via constant key)
result = t.window(
    groupby=Vector([1] * len(t)),
    aggregations={'sales_total': t.sales.sum}
)
```

