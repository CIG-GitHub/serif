# Aliasing & Fingerprints

For the rules on WHERE writes are allowed (owned columns are frozen;
write through the table), see [mutation.md](mutation.md). This document
covers the machinery underneath: why sharing storage is always safe.

## Alias Tracking

Serif prevents accidental shared-state bugs through automatic copy-on-write.

### How It Works

Distinct vectors never share mutable state, even when they share storage
internally:

```python
a = Vector([1, 2, 3])
b = a[:]     # a distinct Vector; internally shares a's immutable storage

b[0] = 99    # b rebuilds its own storage; a's is untouched
a            # 1, 2, 3
b            # 99, 2, 3
```

Outside a `batch()` ownership scope, Serif never mutates storage in place:
`__setitem__` materializes the data, applies the updates, and rebuilds a fresh
storage object. Any other Vector still pointing at the old storage is untouched
— copy-on-write by construction, with no registry or identity tracking needed.
`batch()` first privatizes supported buffers, so its temporary in-place writes
remain unobservable to every prior value.

Plain name-binding is ordinary Python: `b = a` makes two names for one
object, and a mutation through either name is visible through both,
exactly as with any Python object. Copy-on-write protects *vectors* from
each other; it does not (and should not) change what `=` means.

### When Copies Happen

- **Mutation:** every mutation rebuilds the mutated Vector's storage; sharers keep the old immutable storage
- **Table construction:** Table creates private column shells that share immutable storage; this is an O(1) value snapshot per column
- **`.copy()`:** creates an independent Vector shell and normally shares immutable storage; replacement values rebuild storage
- **`batch()`:** privately copies writable buffers once on entry, then permits in-place writes only inside that ownership scope

## Process-local fingerprints

`fingerprint()` enables **O(1) repeated change checks** without full data
comparisons. Its first call is O(n); the cached result is process-local and
hashes values only.

### Basic Usage

```python
v = Vector([1, 2, 3])
fp1 = v.fingerprint()

v[1] = 10
fp2 = v.fingerprint()

assert fp1 != fp2  # Fingerprint changed
```

### Use Cases

1. **Detect data changes without full comparisons**
   ```python
   if v.fingerprint() != cached_fingerprint:
       recompute_expensive_operation()
   ```

2. **Invalidate caches when upstream data mutates**
   ```python
   class CachedModel:
       def __init__(self, data):
           self.data = data
           self.fingerprint = data.fingerprint()
           self.cache = None
       
       def compute(self):
           if self.data.fingerprint() != self.fingerprint:
               self.cache = None  # Invalidate
               self.fingerprint = self.data.fingerprint()
           
           if self.cache is None:
               self.cache = expensive_computation(self.data)
           return self.cache
   ```

### Implementation

Fingerprints use a **rolling hash**, computed lazily on first access and cached. Mutation invalidates the cache; the next `fingerprint()` call recomputes in O(n). Repeated access on unchanged data is O(1).

### Limitations

- Fingerprints hash element **values** only — dtype is not part of the
  hash, so `Vector([1])` and `Vector([1.0])` share a fingerprint
  (`hash(1) == hash(1.0)`).
- Fingerprints answer "did this data change?", not "are these equal?".
  For comparison, remember `==` is **elementwise** (it returns a boolean
  vector, with `None` where either side is null).
- Python deliberately randomizes hashes for values such as strings, so this
  fingerprint is not a persistent cross-process identifier.

## Semantic fingerprints for DAGs and persistent caches

Use `semantic_fingerprint()` when identity must survive the current Python
process:

```python
input_key = table.semantic_fingerprint()
```

It returns a 64-character, versioned BLAKE2b digest over:

- vector/table dimensions and names
- dtype and nullability
- categorical order and decimal scale/precision
- canonically encoded Python values

The digest is deterministic across Python hash seeds and execution backends.
It is recomputed in O(n) on each call; persistent identity favors certainty
over a metadata cache that could become stale. Unknown object values raise
with a conversion remedy rather than falling back to a potentially
address-bearing `repr()`.

