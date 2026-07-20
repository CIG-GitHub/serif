# Serif Design Invariants

These rules define the contract of the library.  
Breaking any of them requires updating this file and providing strong rationale.

## 1. A Vector has exactly one dtype
All elements share a single Python-native kind. The common kinds are
`int`, `float`, `bool`, `str`, and `date`; `datetime`, `complex`, and
`bytes` are also supported.

Nothing mixes silently. Mixed content requires explicitly opting out of
the type system into object dtype (`v.to_object()`).  
No implicit coercion except Python-standard numeric coercions.

## 2. A Table is a list of column vectors
A table is:
- a list of equal-length Vectors  
- each vector is a column  
- row-major access is derived, not stored  
- nested tables are never allowed

## 3. Filtering is done via boolean masks
No string queries.  
No lambdas.  
No SQL-like syntax.  
Explicit mask vectors are the sanctioned path.

(Row selection by integer-index lists exists but is discouraged and
warns — see docs/gotchas.md.)

## 4. Math preserves shape
Elementwise operations require matching lengths.  
The output length equals the input length.  
Scalars broadcast deliberately; vectors never broadcast by length, align by
label, or recycle values.

## 5. Names do not propagate through math
Value-producing operations such as math, comparisons, `is_na()`, `is_type()`,
string methods, and `unique()` return unnamed vectors. Structural selections
and same-column operations — copy, slice, mask, sort, cast, `fillna()`, and
`dropna()` — retain the source name. Use `.alias()` to name a derived value
explicitly.

## 6. Column names do not need to be unique
Tables may contain repeated names.  
Dot-access always resolves to the first match.

This is acceptable because explicit column selection by index is always available.

## 7. Sanitized attribute names are purely syntactic sugar
Dot-access names are derived from column names by a deterministic
sanitization pipeline — the exact rules live in docs/naming.md, and
only there.

Sanitized names never affect actual column names.

## 8. The repr must never be ambiguous
`repr()` shows:
- head/tail  
- full shape  
- vector/table dtype(s)  
- clean alignment  
- not enough data to reconstruct via eval()  

Human readability > round-trippability.

## 9. Tables never infer dtypes from rows
Rows are derived from columns.  
Column dtypes define the table, not the reverse.

## 10. No automatic alignment or index-merging
Tables do not align on labels.  
No auto-joins.  
No auto-broadcasting across columns.

All joins/merges must be explicit.

## 11. Vector iteration yields scalars
`for x in v:` yields plain Python scalars in index order.  
Null positions yield `None`.

## 12. Table-owned columns are complete values
Data and metadata read out of a table are frozen. Element assignment,
`.vector_name = ...`, and `.alias(...)` all raise on table-owned columns.
Write data through table indexing and rename through `Table.rename()`.
A `.copy()` is an independent mutable and renameable Vector.

## 13. Persistent identity includes schema
`fingerprint()` is the deterministic DAG/cache identity. It includes shape,
names, dtypes, nullability, categorical order, decimal metadata, and values.
Unknown object values raise instead of using an unstable repr. There is no
separate process-local or value-only fingerprint path.

