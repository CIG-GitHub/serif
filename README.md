# serif
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
[![Tests](https://github.com/CIG-GitHub/serif/actions/workflows/tests.yml/badge.svg)](https://github.com/CIG-GitHub/serif/actions/workflows/tests.yml)

A clean, typed, composable data layer for Python, built on **Vector** and **Table**.

Vector provides the foundation; Table is your primary tool for readable data modeling and analysis workflows.

## 30-Second Example

```python
from serif import Table

# Create a table with automatic column name sanitization
t = Table({
    "price ($)": [10, 20, 30],
    "quantity":  [4, 5, 6]
})

# Add calculated columns with dict syntax
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
# 3×4 table <mixed>
```

## Real-World Example: Interactive CSV Exploration

```python
from serif import read_csv

t = read_csv("sales.csv")  # Messy column names? No problem.

# Discover columns interactively (no print needed!)
#   t. + [TAB]      → shows all sanitized column names
#   t.pr + [TAB]    → t.price
#   t.qua + [TAB]   → t.quantity

# Compose expressions naturally
total = t.price * t.quantity

# Add derived columns
t >>= {'total': total}

# Inspect (original names preserved in display!)
t
# 'price ($)'  'quantity'   'total'
#      .price   .quantity    .total
#          10           4        40
#          20           5       100
#          30           6       180
#
# 3×3 table <int>
```

**The power**: You don't need to know the CSV contents upfront. Tab completion guides you, the repr shows you everything, and messy column names are automatically cleaned for dot-access.

## Installation

```bash
pip install serif
```

Zero external dependencies. In a fresh environment:

```bash
pip freeze
# serif==0.x.y
```

## Why serif?

- Explicit, predictable vector semantics
- Tables compose cleanly from vectors
- Readable "spreadsheet-like" workflows
- Table-owns-storage: building a table copies inputs so tables never share columns by accident
- Controlled mutation: column vectors are live views; in-place updates mutate only that table
- Immediate visual feedback via `__repr__`

## Quickstart

### Vectors: elementwise operations

```python
from serif import Vector

a = Vector([1, 2, 3, 4, 5])
b = Vector([10, 20, 30, 40, 50])

a + b           # Vector([11, 22, 33, 44, 55])
a * 2           # Vector([2, 4, 6, 8, 10])
a > 3           # Vector([False, False, False, True, True])
```

### Tables: compose vectors with `>>`

```python
from serif import Table

# Column names auto-sanitize to valid Python attributes
t = Table({
    "first name": [1, 2, 3],
    "price ($)":  [10, 20, 30]
})

t.first_name    # Vector([1, 2, 3])
t.price         # Vector([10, 20, 30])

# Add columns with >>= (recommended)
t >>= (t.first_name * t.price).alias("total")

t
# 'first name'  'price ($)'  total
#  .first_name       .price  .total
#            1           10      10
#            2           20      40
#            3           30      90
#
# 3×3 table <int>
```

### Boolean masking

```python
filtered = t[t.price > 15]

filtered
# 'first name'  'price ($)'  total
#  .first_name       .price  .total
#            2           20      40
#            3           30      90
#
# 2×3 table <int>
```

### Joins

```python
customers = Table({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
scores = Table({'id': [2, 3, 4], 'score': [85, 90, 95]})

result = customers.inner_join(scores, left_on='id', right_on='id')

result
#    id  name           id   score
#   .id  .name      .id__2  .score
# [int]  [str]       [int]   [int]
#     2  'Bob'           2      85
#     3  'Charlie'       3      90
#
# 2×4 table <mixed>
```

### Aggregations

```python
t = Table({'customer': ['A', 'B', 'A'], 'amount': [100, 200, 150]})

result = t.aggregate(
    groupby=t.customer,
    aggregations={
        'amount_sum': t.amount.sum,
        'amount_count': t.amount.count,
    }
)

result
# customer  amount_sum  amount_count
#    [str]       [int]         [int]
#       A           250             2
#       B           200             1
#
# 2×3 table <mixed>
```

**See [docs/joins-aggregations.md](docs/joins-aggregations.md) for detailed examples.**

## Key Features

### Automatic `__repr__`: Instant Visual Feedback

```python
# Dictionary syntax: quick and familiar
t = Table({'id': range(100), 'value': [x**2 for x in range(100)]})

# Or compose from vectors: showcases Vector's design philosophy
a = Vector(range(100), name='id')
t = a >> (a**2).alias('value')

t
# id  value
#  0      0
#  1      1
#  2      4
#  3      9
#  4     16
#... ...
# 95   9025
# 96   9216
# 97   9409
# 98   9604
# 99   9801
#
# 100×2 table <int>
```

Head/tail preview + type annotations + dimensions—no need for `.head()`, `.info()`, etc.

### Column Name Sanitization

Column names are sanitized to valid Python identifiers so you can access them with dot notation:

```python
t = Table({"2023-Q1 Revenue ($M)": [1, 2, 3]})
t.c2023_q1_revenue_m  # Deterministic, predictable access
```

**Rules:**
- Non-alphanumeric characters become `_`
- Leading digits get `c` prefix
- All lowercase

Unnamed columns use system names: `t.col0_`, `t.col1_`, etc. Original column names are preserved; sanitization only affects attribute access.

### Typed Subclasses

Vector auto-creates typed subclasses with method proxying:

```python
from datetime import date

dates = Vector([date(2023, 6, 29), date(2024, 1, 2), date(2024, 12, 28)])
dates += 5       # Add 5 days to each date
dates.year       # Vector([2023, 2024, 2025]) - one crossed the year boundary!
```

Works for `int`, `float`, `str`, `date` types.

## Common Gotchas

### Don't use subscript lists—use boolean masks

```python
# ANTI-PATTERN
indices = [1, 5, 9]
result = v[indices]  # Slow, emits warning

# IDIOMATIC
mask = (v > threshold)
result = v[mask]
```

### Operator overloading: avoid `.index()` on Vector lists

```python
# WRONG: invokes elementwise equality
cols = [table.year, table.month]
idx = cols.index(table.year)  # Returns boolean vector!

# CORRECT: use enumerate
for idx, col in enumerate(cols):
    if col is table.year:  # identity check
        ...
```

### `None` handling

`None` is excluded from aggregations but counted in `len()`:

```python
v = Vector([10, None, 20])
v.sum()   # 30 (None excluded)
len(v)    # 3 (None counted)
```

## Just Write Python

Not every task fits neatly into a vectorized expression.
When a loop is the clearest approach, serif keeps it efficient.

`for row in table:` is fully supported and stays lightweight, so you can use
whichever style makes the code easiest to understand.


## Design Philosophy

serif prioritizes clarity and workflow ergonomics.


**What you get:**
- Readable, debuggable code
- No hidden state or aliasing bugs (copy-on-write)
- Deterministic operations
- Zero dependencies
- O(1) fingerprinting for change detection

**When to use serif:**
- Modeling-scale data (10K–1M rows)
- Correctness and maintainability matter most
- Interactive workflows (Jupyter notebooks, REPL)
- Projects where zero dependencies is important

## Further Documentation

- **[Performance & Complexity](docs/performance.md)** — O(n) analysis for joins, aggregations, indexing
- **[Exception Handling](docs/exceptions.md)** — Custom exception types and error handling
- **[Aliasing & Fingerprints](docs/aliasing.md)** — Copy-on-write and change detection
- **[Joins & Aggregations](docs/joins-aggregations.md)** — Detailed examples and patterns
- **[Development Guide](docs/development.md)** — Running tests, project structure

## Philosophy

- Clarity beats cleverness
- Explicit beats implicit
- Modeling should feel intuitive
- You should always know what your code is doing

## License
MIT

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
