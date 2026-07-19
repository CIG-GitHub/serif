# serif
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
[![Tests](https://github.com/CIG-GitHub/serif/actions/workflows/tests.yml/badge.svg)](https://github.com/CIG-GitHub/serif/actions/workflows/tests.yml)

*Python in, Python out. Allowed to raise, not allowed to lie.*

Serif is typed vectors and tables for Python, built around two promises: touching your data should feel like ordinary Python, and ambiguous operations should fail loudly.

Values stay Python values — `int`, `float`, `str`, `date`, `None` — so there is nothing to convert, unwrap, or remember. And serif does not silently coerce, mutate, or guess. When an operation has no clear meaning, it raises.

Vector provides the foundation. Table is the primary interface for exploration, modeling, and analysis.

## 30-Second Example

```python
from serif import Table

t = Table({
    "price ($)": [10, 20, 30],
    "quantity":  [4, 5, 6]
})

t >>= {'total': t.price * t.quantity}
t >>= {'tax': t.total * 0.1}

t
# 'price ($)'   quantity   total      tax
#      .price  .quantity  .total     .tax
#       [int]      [int]   [int]  [float]
#          10          4      40      4.0
#          20          5     100     10.0
#          30          6     180     18.0
#
# 3×4 table <int:3, float>
```

## Installation

```bash
pip install serif
```

Zero dependencies — `pip freeze` in a fresh environment shows exactly one line.

## Quickstart

### Vectors

```python
from serif import Vector

a = Vector([1, 2, 3, 4, 5])

a * 2         # 2, 4, 6, 8, 10
a > 3         # False, False, False, True, True
a[a > 3]      # 4, 5 — boolean masks filter
a.sum()       # 15 — a plain Python int
```

### Tables

```python
from serif import Table

t = Table({
    "first name": ["ann", "bo", "cy"],
    "price ($)":  [10, 20, 30],
})

t.price * 2                    # names sanitize for dot access
t >>= {'total': t.price * 2}   # add a column
t[t.price > 15]                # masks filter rows

t._                            # what do I have again?
# .first_name   str   'first name'
# .price        int   'price ($)'
# .total        int
```

### None means missing

Element-wise, unknown in is unknown out. Aggregates summarize what you know. And a verdict needs evidence — `all()`/`any()` over zero valid values raise rather than guess.

```python
v = Vector([10, None, 30])

v + 1        # 11, None, 31
v > 15       # False, None, True
v.sum()      # 40
len(v)       # 3

Vector([None, None]).any()                 # raises SerifEmptyReductionError
Vector([None, None]).any(on_empty=False)   # False — you supplied the verdict
```

### Joins and aggregation

```python
customers = Table({'id': [1, 2, 3], 'name': ['Ann', 'Bo', 'Cy']})
scores    = Table({'id': [2, 3, 4], 'score': [85, 90, 95]})

customers.inner_join(scores, left_on='id', right_on='id')
# id, name, score — matched key columns are not duplicated

sales = Table({'customer': ['A', 'B', 'A'], 'amount': [100, 200, 150]})

sales.aggregate(
    groupby=sales.customer,
    aggregations={'total': sales.amount.sum, 'n': len},
)
# customer  total  n
#        A    250  2
#        B    200  1
```

### CSV and Parquet

```python
from serif import read_csv, read_parquet

t = read_csv("sales.csv")        # types inferred, headers sanitized for dot access
t.to_parquet("sales.parquet")    # ints, dates, and nulls arrive intact
u = read_parquet("sales.parquet")
recent = u[u.date >= cutoff]      # remaining columns load through this mask
```

`read_parquet` reads the footer first and materializes columns only when they
are touched. Boolean filtering stays ordinary Serif filtering: the concrete
mask is carried into columns that have not been read yet, so the first fully
materialized table can be the filtered one. Keep the source file readable and
unchanged until the table has materialized.

## Documentation

- [Design Philosophy](docs/design-philosophy.md) — the principles everything else answers to
- [Serif vs Python](docs/serif-vs-python.md) — the few intentional departures
- [Table Model](docs/table-model.md) — the column-major table
- [Indexing](docs/indexing.md) — slicing, masking, and selection rules
- [Null Semantics](docs/null-semantics.md) — `None`, three-valued logic, verdicts
- [Joins & Aggregations](docs/joins-aggregations.md) — detailed examples and patterns
- [Naming](docs/naming.md) — column names and sanitization
- [Repr](docs/repr.md) — what the display shows and why
- [Invariants](docs/invariants.md) — the promises the internals keep
- [Exceptions](docs/exceptions.md) — error types and when they raise
- [Aliasing & Fingerprints](docs/aliasing.md) — copy-on-write and change detection
- [Performance](docs/performance.md) — complexity of the core operations
- [Gotchas](docs/gotchas.md) — sharp edges worth knowing up front
- [Development](docs/development.md) — running tests, project structure

## License

MIT

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
