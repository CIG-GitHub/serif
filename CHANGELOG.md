# Changelog

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
