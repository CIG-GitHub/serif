# Table Model

A Table is a list of equal-length vectors.  
It is column-major by design.

## 1. Construction
Tables are built via:
- column stacking: `v1 >> v2 >> v3`
- explicit constructor: `Table([v1, v2, ...])`
- dict constructor: `Table({'a': [...], 'b': [...]})` — keys become column names
- row-major lists of uniform length are transposed into columns:
  `Table([[1, 'x'], [2, 'y']])` is a 2-row table

However built, the table stores columns.

## 2. Access
`table[i]` → i-th row (tuple-like)  
`table['colname']` → first matching column vector  
`table[index]` (int) → row  
`table[mask]` → filtered table

## 3. Invariants
- all columns same length  
- no nested tables  
- repeated column names allowed  
- dtype is per-column, never per-row  

## 4. Row iteration
`for row in table:` yields a **ride-along Row view** — one Row object whose
index advances each step. This is deliberate: iteration allocates nothing
per row, and the view machinery is what enables zero-copy fast paths.

Consequences:
- Consume each row inside the loop (read fields, do math) — this covers the
  CSV/SQL iteration pattern.
- Do **not** stash the yielded rows: `list(table)` gives N references to the
  same view, all pointing at the final row. To materialize rows, copy
  explicitly: `[tuple(row) for row in table]`.

## 5. Why column-major
Real-world data workflows are column-major, even though Python lists are
row-major:
- CSVs are tall, not wide  
- SQL tables are column-defined  
- analytics operate column-wise  

Operations such as mean, stdev, masking, and sorting all follow this
grain: they operate more naturally and efficiently on column-major
layouts. Storing tables as a list of column vectors aligns the structure
with actual usage.

## 6. Combining tables
`>>` stacks columns, not rows.  
Row-wise combining requires explicit user intent.

## 7. Vector operations on tables

Table deliberately exposes the Vector operations with coherent 2-D meanings:

- element-wise methods (`fillna`, `isna`, `is_type`, `cast`, `to_object`,
  `pluck`) map over columns and preserve the table shape and names
- unary, reverse arithmetic, and bit-shift operations also map over columns
- `dropna()` keeps complete rows (no null cell in any column)
- `unique()` keeps the first occurrence of each distinct row
- reductions such as `sum()`, `mean()`, and `count()` return one value per column

`Table.filled()` raises because a table has no unambiguous column schema; build
named `Vector.filled(...)` columns instead.

## 8. Exporting duplicate names

Repeated names remain valid inside a Table, but a Python dict cannot represent
them. `to_dict()` therefore raises when two columns would map to the same key,
including a collision with an unnamed column's `col<idx>_` fallback. Rename the
columns or export ordered column pairs when duplicates must be preserved.

