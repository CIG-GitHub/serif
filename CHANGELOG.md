# Changelog

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
