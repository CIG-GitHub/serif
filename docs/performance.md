# Performance & Complexity

## Optional acceleration
- Serif has no required dependencies. When NumPy is installed, fixed-width
  operations work directly over Serif's existing numeric and byte-mask
  buffers. When PyArrow is installed, it accelerates variable-width string
  work and Parquet decoding that NumPy cannot naturally express.
- Accelerators are opportunistic and per-operation. Exact operations either
  return the pure Serif result or decline; null behavior, errors, schemas, and
  surfaced Python scalar types do not change. Floating-point reductions may
  differ in their final rounding within the bounds pinned by the conformance
  suite.

## Indexing
- **Slices:** O(k) where k is slice length
- **Boolean masks:** O(n) scan + O(m) result construction
- **Subscript lists** (`v[[1,5,9]]`): O(k) but **not recommended** for large vectors (emits warning)

## Parquet reads
- `read_parquet()` initially reads only file metadata; `len`, `shape`, column
  names, and `t._` do not decode column data.
- Accessing one column reads and caches that column only.
- `t[mask]` carries the concrete mask into unread columns. Entire row groups
  with no selected rows are skipped; partially selected groups are decoded
  one column at a time and only survivors enter the resulting Vector.
- The source file must remain readable and unchanged until the deferred table
  latches. If it changes, serif raises instead of mixing snapshots.

## Joins
- `inner_join`, `left_join`, `full_join`: O(n + m) hash table construction + lookups
- Multi-key joins: same complexity, tuple keys

## Aggregations
- `aggregate()`: O(n) partition build + O(groups × agg_cost)
- `window()`: O(n) partition build + O(n) result expansion

## General Operations
- **Copy-on-write:** Mutations rebuild the vector's storage with O(n) copy cost; other vectors sharing the old storage are unaffected
- **Fingerprinting:** lazy O(n) on first access, cached (O(1)) until the next mutation invalidates it
- **Semantic fingerprinting:** O(n) on every call; deterministic schema-aware identity for persistent DAG/cache keys

## Rule of Thumb
Vector handles **10K–1M rows** comfortably in pure Python and can extend that
range substantially when an optional accelerator matches the workload. Serif
still optimizes for modeling-scale, interactive work rather than replacing a
distributed or out-of-core engine.

## Performance Profile
- **Sweet spot:** Modeling-scale data (thousands to low millions of rows)
- Optimized for **workflow velocity**, not raw compute throughput
- Cached fingerprinting enables efficient change detection and caching
- Optional accelerators operate behind the same Vector and Table semantics

