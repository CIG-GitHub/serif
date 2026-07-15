# Table Model

A Table is a list of equal-length vectors.  
It is column-major by design.

## 1. Construction
Tables are built via:
- column stacking: `v1 >> v2 >> v3`
- explicit constructor: `Table([v1, v2, ...])`
- no row-based construction API is provided

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

## 5. Column-major memory alignment
Operations such as:
- mean  
- stdev  
- masking  
- sorting  

…operate more naturally and efficiently on column-major layouts.

## 6. Combining tables
`>>` stacks columns, not rows.  
Row-wise combining requires explicit user intent.

