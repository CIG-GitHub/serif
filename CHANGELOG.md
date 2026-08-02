# Changelog

## 0.2.2 – Unreleased

### Fixed
- Categorical payload columns no longer degrade to strings during joins;
  category domains and ordering are preserved, and null padding produces
  nullable categoricals on optional join sides.

## 0.2.1 – Recursive Semantics & Direct Storage

This release is a project-wide refactor of Serif's semantic and physical
execution layers. Vector and Table now follow one explicit recursive operation
model; pure Python remains the semantic authority, optional backends implement
deterministic physical kernels, and construction, execution, and I/O keep data
in canonical Serif storage instead of repeatedly boxing whole columns into
Python containers.

### Breaking Changes
- Breaking, pre-1.0: `Vector.isna()` is now `Vector.is_na()`.
- Breaking, pre-1.0: `Table.rename({old: new})` is now
  `Table.rename_columns({old: new})`. Behavior is otherwise unchanged.
- Breaking, pre-1.0: `Vector.pluck()` and the fingerprinting API were removed.
- Breaking, pre-1.0: the optional dependency extra `serif[arrow]` is now
  `serif[pyarrow]`.
- Breaking, pre-1.0: `all()`/`any()` over zero valid values no longer raise.
  They return the Python identities — `True` and `False`, respectively — and
  emit `SerifEmptyReductionWarning`. `on_empty=` states the empty-case verdict
  explicitly and silences the warning. `SerifEmptyReductionError` was removed
  from the public exports.
- Comparing a Vector with scalar `None` now follows Python's missingness answer:
  `v == None` returns the same total boolean mask as `v.is_na()`, and
  `v != None` returns its inverse. These spellings warn so accidental scalar
  `None` comparisons remain visible. Vector-to-Vector comparisons still
  propagate null where either side is unknown.

### Added
- Applicable Vector operations now lift consistently over Tables while
  preserving shape and column names. This includes comparisons, unary and
  reverse arithmetic, logical and bitwise operations, casts, `to_object()`,
  `fillna()`, `is_na()`, and `is_type()`.
- `Vector.coalesce()` and same-shaped `Table.coalesce()` select the first
  non-null value positionally.
- `Vector.is_in()` and `Table.is_in()` provide element-wise membership using
  Python equality. Including `None` among the members explicitly includes
  missing positions.
- Tables gain row-aware `dropna()` and stable row-wise `unique()`.
- Table selection accepts a list of column names:
  `t[['col 1', 'col 2']]`.
- Table mutation supports Vector-valued masked assignment and fully validated
  two-dimensional assignment, including `t[mask, 'col'] = v[mask]`.
- Python keywords are treated as reserved dot-access names, so columns such as
  `class` sanitize to reachable accessors such as `.class_`.

### Changed
- Null behavior now follows the rule “Python governs values and answers; SQL
  governs row matching.” Joins never match a key containing null, while
  grouping and aggregation retain `None` as a group.
- Empty-verdict groups in `aggregate()` and `window()` receive the `all()` or
  `any()` identity and emit one warning per output column naming the affected
  group keys.
- Table iteration now yields distinct, stable row values. Retaining one row no
  longer causes it to change as iteration advances.
- Table-owned column metadata is frozen along with its values; rename columns
  through the Table.
- Table assignments are fully validated before mutation, preventing partial
  writes when a later value or column is invalid.
- `Table.to_dict()` raises on duplicate export keys instead of silently losing
  a column.
- The repr footer no longer repeats per-dtype column counts.
- Appending `None` correctly promotes established schemas to nullable, and
  untyped mixed-kind construction infers `object` without a spurious fallback
  warning.
- Public exception behavior is normalized: explicit dtype violations and
  ambiguous `bool(Vector(...))` raise `SerifTypeError`; out-of-range
  `Table.cols()` access raises `SerifIndexError` across eager, deferred, and
  lazy-Parquet tables.

### Performance
- Numeric, boolean, string, decimal, and validity data now build directly into
  canonical storage, including pre-allocation for dense known-size results.
- NumPy join/group indexers stay as arrays through selection, and Arrow grouped
  keys and sums stay in Serif storage instead of round-tripping through Python
  lists.
- Python grouping, joins, sorting, categorical transforms, and known-dtype
  operations no longer box complete columns into tuples or lists.
- CSV reading accumulates cells column-first, performs one inference pass per
  column, and constructs the result without an intermediate row matrix or
  constructor recopy.
- The pure Parquet reader decodes nullable numeric and other supported physical
  values directly into canonical storage, concatenating pages and row groups
  once.
- Parquet writing encodes directly from Serif storage and streams validated
  pages instead of materializing non-null Python lists or buffering the entire
  file.

### Internal
- The former Vector and Table monoliths were decomposed into thin public
  classes, semantic modules, row/column mechanics, and isolated Table algebra
  for transpose, joins, grouping, aggregation, and windows.
- Execution now uses an explicit `DECLINED` contract. Optional NumPy and Arrow
  backends may decline unsupported physical cases, but invalid operations and
  backend defects raise normally; pure Python is the mandatory final path.
- Physical implementations are separated into Python, NumPy, and Arrow backend
  packages, the legacy `_accel` package was retired, and structural tests lock
  the dependency direction.

## 0.2.0 – Optional Acceleration & Value Semantics

### Changed
Breaking, pre-1.0: **read through the column, write through the table**
(docs/mutation.md).
- Table-owned columns are frozen: `t.v[0] = 5`, `t['v'][0] = 5`, and
  mutating a column read out earlier all raise `SerifTypeError` (the
  message contains the fix). This closes the alias-mutation path for
  good — a vector read out of a table can never change the table, and
  vice versa.
- Write through the table instead: `t[t.v == 'old', 'v'] = 'new'`,
  `t[3, 'v'] = 5`, `t[0:100, 'v'] = 0`. Owner writes swap in a freshly
  rebuilt column (swap-on-write), so copies, slices, filtered results,
  and previously read-out columns are all stable snapshots by
  construction.
- Standalone (wild) vectors and `.copy()` results remain mutable.

### Added
- Optional NumPy acceleration for fixed-width elementwise operations, logical
  operations, reductions, boolean filtering, positional gathering, grouping,
  and joins. NumPy views Serif's existing buffers and declines to the pure
  implementation whenever it cannot preserve exact Python semantics.
- Optional PyArrow acceleration for variable-width string operations,
  string grouping and join probes, checked numeric operations, and Parquet
  decoding. PyArrow works with or without NumPy installed.
- Four supported dependency modes: pure Python, NumPy only, PyArrow only, and
  NumPy + PyArrow. Exact operations agree across modes; null behavior, errors,
  schemas, and concrete Python scalar types remain the same. Floating-point
  reductions retain their documented bounded-rounding allowance.
- Complete decimal128 Parquet round-tripping, including nullable columns and
  preservation of scale and precision, in both the pure and Arrow readers.
- Footer-backed Parquet reads: `read_parquet()` now returns a Table-compatible
  deferred source whose columns materialize independently. Ordinary boolean
  masks flow into unread columns, skip all-false row groups, and preserve the
  existing `MaskedTable` latch/mutation semantics. The pure reader uses bounded
  chunk reads; optional Arrow acceleration reads projected columns and uses
  bounded record batches for masked payloads.
- `Table.batch()` — bulk-edit scope for read-modify-write loops:
  `with t.batch() as m:` copies each column's buffers once on entry
  (un-sharing), then point writes land raw and O(1) (~4,200× faster
  than per-statement rebuilds on a 10k-write loop); exit refreezes
  everything, including column refs that escaped the scope. Observable
  semantics identical to table-addressed writes.
- `ArrayStorage`/`BoolStorage` gain `private_copy()` and
  `write_inplace()`; other backends decline and keep rebuilding.
  `BitMask` gains in-place `set_null`/`set_valid` for privately-owned
  masks.

## 0.1.8 – Readable Footer dtype Summary

### Changed
- Table repr footer now summarizes dtypes as `type:count` pairs, most common
  first, so it reads as an at-a-glance dominance summary. The old `N×dtype`
  form (`<18×str, 12×int, 6×float, +4>`) crowded counts against type names and
  reused `×` for both dimensions and multiplication; the new form
  (`<str:18, int:12, float:6, date:4>`) separates them and frees `×` to mean
  dimensions only.
  - A count of one is dropped (`date`, not `date:1`); a homogeneous table drops
    the count entirely (`<int>`) since the total is already in the `R×C` prefix.
  - Ties keep column (first-appearance) order.
  - With six or more distinct dtypes, the first four are shown and the rest fold
    into ` ...+N` — e.g. `<str:50, int:20, float:10, date:5 ...+2>` — where `...`
    signals more is hidden and N (always ≥ 2) counts the folded dtype groups.

## 0.1.7 – API Cleanup & Bit-Packed Masks

### Changed
Breaking, pre-1.0 API cleanup (#39, #40):
- Name property split: `Vector.name` → `Vector.vector_name`, `Table.table_name`.
  This frees `name` as a column — `t.name` now returns a column called `name`
  instead of the property shadowing it.
- `Table.join` → `Table.left_join`; the joins are now `inner_join` /
  `left_join` / `full_join`.
- `rename_column` / `rename_columns` → `Table.rename({old: new})`: returns a new
  table; string keys rename by name (an ambiguous duplicate name raises),
  integer keys rename by position.
- `Vector.new` → `Vector.filled(value, length)`.
- `Vector.isinstance` → `Vector.is_type` (element-wise; isinstance/subclass semantics).
- `Vector.alias` now (re)names any vector, not just unnamed ones — the chainable
  counterpart to the `.vector_name` setter.

Other:
- `peek()` replaced by the `t._` accessor: a schema listing, one row per column
  with its dot-accessor. (#36)
- repr lists column dtypes in the footer. (#35)

### Added
- `Table.drop(*names)` — drop column(s) by name (varargs or a list); returns a
  new table. (#40)
- Reserved-name collision warning: naming a column after a method/attribute
  (`sum`, `count`, …) warns at construction, since dot-access resolves to the
  method — reach the column with `t['sum']` or the `.sum_` accessor. (#40)
- Bit-packed null masks (`BitMask`): one bit per element in Apache Arrow
  validity layout (1=valid, LSB-first), replacing the byte-per-element mask —
  ~8× smaller and the groundwork for zero-copy Arrow interop. (#38)
- `DecimalStorage`: Arrow-format 16-byte decimal128 storage backend — the
  foundation for decimal columns, with full support arriving alongside pyarrow
  in 0.2.0. (#41)

### Removed
- `Vector.product()`. (#40)
- Deprecated `Vector.rename()` — set `.vector_name`, or use `.alias()`. (#40)

### Docs
- README refresh. (#37)


## 0.1.6 – Parquet, Null Doctrine & the Hardening Pass

### Added
- Zero-dependency Parquet I/O: `read_parquet` / `write_parquet` — writes a
  PLAIN/UNCOMPRESSED subset any Parquet reader consumes; reads UNCOMPRESSED
  and GZIP files from other tools (#27, #33)
- Arrow-style string storage (`StringStorage`): contiguous UTF-8 buffer +
  offset array, lazy per-value decode (#28)
- Join cardinality contracts: generic `on=` for shared key names,
  `expect_right_unique` / `expect_left_unique` (#24)

### Changed
- **Three-valued null semantics** (see `docs/null-semantics.md`): element-wise
  ops propagate null (unknown in, unknown out); `&`/`|` are Kleene on bool
  vectors, bitwise on int; null mask entries exclude rows (SQL WHERE);
  aggregates skip nulls (#26, #30)
- **Verdicts need evidence**: `all()` / `any()` over zero valid values raise
  `SerifEmptyReductionError` unless `on_empty=` supplies the empty-case
  verdict; aggregations re-raise with the group's coordinates (#32)
- Null masks standardized on 1=valid / 0=null (#26)
- Adding a wrong-length column to a Table raises instead of warning (#25)
- Strict CSV numeric inference (leading-zero identifiers stay strings;
  over-long rows raise); default encoding `utf-8-sig` (#29)
- Reverse-op dtype promotion (`1.5 + int_vector` → float); incompatible-type
  arithmetic raises `SerifTypeError` instead of degrading silently (#29)
- `date - date` yields int days; type inference is order-independent
  (`[None, 1, 2]` infers like `[1, 2, None]`) (#29)

### Fixed
- Parquet reader never misreads: unknown converted/logical types (DECIMAL,
  TIME, unsigned 32/64-bit, nanosecond timestamps), DataPage V2, and
  dictionary/RLE value encodings raise instead of decoding to plausible
  wrong values; DECIMAL is rejected on write and read pending real support;
  truncated files raise `SerifValueError` (#33)
- Thrift footer parse desync on long-form field ids; timestamp encoding uses
  integer math (microsecond-exact far from the epoch) (#29)
- String-vector `sort_by`/`dropna`/unary crashes, `StringStorage` negative
  indexing, reverse-sort null placement, `_Category` clone coherence (#29)
- `count()` arity on strings, `date + timedelta`, NaN/inf-safe repr (#29)

### Internal
- Storage protocol conformance suite; `take(indices)` on every backend (#29)
- Structural refactor of `table.py` and `_vector/base.py`; test-suite
  consolidation (#31)
- Row-as-vector: rows expose on-demand storage so base Vector methods work on
  them read-only (#29); dead-code purge (#29, #31)

## 0.1.5 – First/Last Aggregations & Nullable Fixes

### Added
- `first()` and `last()` aggregation functions
- Fanning — broadcast aggregation results back over the source table
- `ordered_pick()` for selecting values by ordering criteria

### Fixed
- `max()` and `min()` raising errors on `None` values in nullable vectors

### Internal
- Derived tables now track their source type
- Removed unreachable/dead code paths

## 0.0.1 – Namespace Reservation
- Placeholder release to reserve package name while finalizing implementation.

## 0.1.0 – Initial Release
- First functional release of serif
- Core `Vector` and `Table` classes
- Boolean indexing, slicing, and masking
- CSV I/O with automatic column sanitization
- Joins and aggregations
- Interactive display with rich `__repr__`

## 0.1.1 – Core Usability Improvements
> **Note:** This version was tagged but not immediately published to PyPI.

### Added
- Table sorting
- Column access by string name
- `Vector.peek()` for quick inspection

### Changed
- Vectors preserve element type when homogeneous, even if not explicitly supported
- Increased default `__repr__` row limit
- Improved type inference fallback behavior

## 0.1.2 – Column Access & Naming Semantics

### Changed
- Column dot-access now explicitly includes column index to remove ambiguity
- `(a + b).rename(...)` renamed to `(a + b).alias(...)` for semantic clarity
- Column naming behavior standardized across table operations
- Sanitized column names no longer rely on implicit iteration checks

### Fixed
- `None` handling in float `__repr__`
- Removed triple-underscore edge cases in display
- Warnings added for duplicate column keys (with test coverage)

### Internal
- Replaced `hasattr(__iter__)` checks with explicit `isinstance(...)`
- Refactored helper layout and imports for readability
- Added contributing guidelines, PR templates, and code of conduct

## 0.1.3 – Table Dictionary Export

### Added
- `Table.to_dict()` method for converting tables to dictionaries

## 0.1.4 – Aggregation Redesign & Categorical Vectors

### Added
- `category` vector type with null behavior, string-vs-category comparisons, and `set_category()`
- Table construction from 2D lists of lists
- No-groupby aggregations — omit `groupby` to aggregate the entire table as one group

### Changed
- Aggregation API redesigned: `aggregate()` and `window()` now use `groupby=` and `aggregations=` dict instead of the old keyword-per-aggregation style
- More expressive errors for incorrect aggregation syntax
- Row iterator optimized to avoid materializing intermediate objects

### Fixed
- Tables can now be constructed from dissimilar (mixed-type) vectors
- Documentation: corrected `aggregate()` and `window()` examples in README and docs that showed a non-existent API (`over=`, `sum_over=`, etc.)

### Internal
- Storage backend refactored and renamed (`_underlying` removed in favor of `storage`)
- Performance: fast-path vectors when type is known, reduced allocations, walk iterables once, removed deepcopy
- Data-type refactor; precomputed output types
- Removed alias tracker; cleaned up unreferenced files; tabs/spaces normalized
