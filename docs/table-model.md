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
`for row in table:` yields a distinct, read-only `Row` value for each
position. Retained rows are stable, so `list(table)` behaves like an ordinary
Python container and preserves every row.

An iterator prepares the column backing once, then each lightweight `Row`
shares that immutable snapshot with a fixed index. Mutating the table after
iteration starts does not change rows already yielded or later rows from that
iterator.

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

