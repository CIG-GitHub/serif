# Performance & Complexity

## Indexing
- **Slices:** O(k) where k is slice length
- **Boolean masks:** O(n) scan + O(m) result construction
- **Subscript lists** (`v[[1,5,9]]`): O(k) but **not recommended** for large vectors (emits warning)

## Joins
- `inner_join`, `join`, `full_join`: O(n + m) hash table construction + lookups
- Multi-key joins: same complexity, tuple keys

## Aggregations
- `aggregate()`: O(n) partition build + O(groups × agg_cost)
- `window()`: O(n) partition build + O(n) result expansion

## General Operations
- **Copy-on-write:** Mutations rebuild the vector's storage with O(n) copy cost; other vectors sharing the old storage are unaffected
- **Fingerprinting:** lazy O(n) on first access, cached (O(1)) until the next mutation invalidates it

## Rule of Thumb
Vector handles **10K–1M rows** comfortably. For 10M+ rows or compute-heavy numerical work, use NumPy/Polars.

## Performance Profile
- **Sweet spot:** Modeling-scale data (thousands to low millions of rows)
- Optimized for **workflow velocity**, not raw compute throughput
- Cached fingerprinting enables efficient change detection and caching
- Future "fast back-end" possibility without changing the Vector UX

