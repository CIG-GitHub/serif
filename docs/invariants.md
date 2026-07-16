# Serif Design Invariants

These rules define the contract of the library.  
Breaking any of them requires updating this file and providing strong rationale.

## 1. A Vector has exactly one dtype
All elements share a single Python-native type:
- int  
- float  
- bool  
- str  
- date  

No mixed types.  
No implicit coercion except Python-standard numeric coercions.

## 2. A Table is a list of column vectors
A table is:
- a list of equal-length Vectors  
- each vector is a column  
- row-major access is derived, not stored  
- nested tables are never allowed

## 3. Filtering is only done via boolean masks
No string queries.  
No lambdas.  
No SQL-like syntax.  
Explicit mask vectors only.

## 4. Math preserves shape
Elementwise operations require matching lengths.  
The output length equals the input length.  
No auto-broadcasting.

## 5. Names do not propagate through math
Only copies inherit names.  
Derived vectors start unnamed.

`v.copy()` reproduces values, dtype, and name; every other operation
produces an unnamed result unless explicitly aliased.

## 6. Column names do not need to be unique
Tables may contain repeated names.  
Dot-access always resolves to the first match.

This is acceptable because explicit column selection by index is always available.

## 7. Sanitized attribute names are purely syntactic sugar
Dot-access uses:
- alphanumeric  
- underscores  
- single underscore collapsing  
- no leading underscore  

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

