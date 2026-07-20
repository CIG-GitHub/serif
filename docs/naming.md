# Naming Semantics

Names in Vector and Table are simple, non-magical metadata.

## Vector
- `vector_name` is optional
- math operations do not propagate names
- structural selection (`copy`, slice, mask, sort) preserves the name
- cast/fill/drop-null operations preserve the name
- `.alias(name)` renames a standalone vector and returns it for chaining

Names are human-facing, not structural.

A table-owned column's name is frozen along with its values. Rename through
the owner with `t.rename({'old': 'new'})`; use `t.col.copy().alias('new')` for
an independent named Vector.

## Table
- each column has its own name  
- duplicate names allowed  
- bare dot-access resolves to the first match; later duplicates get their
  own indexed accessors (below)  
- attribute names are sanitized versions of original names  

### Sanitization pipeline (in order)

1. lowercase
2. runs of characters outside `[a-z0-9_]` become a single `_`
   (underscore runs are preserved: `a__b` stays `a__b`)
3. leading and trailing `_` stripped
4. empty after sanitizing → positional accessor `col<idx>_`
5. leading digit → `c` prefix (`2023 Revenue` → `c2023_revenue`)
6. names matching `<base>__<digits>` get a trailing `_` — that shape is
   reserved for duplicate disambiguation
7. collision with a Vector/Table method or property → trailing `_`
   (for example, a column named `sum` is accessed as `.sum_`; a column named
   `name` remains `.name` because vector/table names live at `.vector_name`
   and `.table_name`)

### Duplicates

When two columns sanitize to the same accessor, the later column gets
`<base>__<idx>` where `idx` is its **column position** (not a running
counter) — a duplicate at position 5 is `.x__5`, not `.x__2`. Table
construction warns when this happens.

Sanitized names do not replace the actual names. `t._` shows every
column's accessor alongside its original name.

## Practical consequences
- weird user-provided column names are allowed
- dot-access provides ergonomic access without polluting data semantics
- exact and sanitized names resolve identically in one- and two-axis access
- users rename columns owner-first with `Table.rename()`

