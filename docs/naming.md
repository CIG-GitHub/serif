# Naming Semantics

Names in Vector and Table are simple, non-magical metadata.

## Vector
- `name` is optional  
- math operations do not propagate names  
- slicing does not preserve name  
- copying preserves name  

Names are human-facing, not structural.

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
   (a column named `name` is accessed as `.name_`, because `.name` is
   a Vector property)

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
- users may explicitly rename columns if needed  

