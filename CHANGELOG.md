# Changelog

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
