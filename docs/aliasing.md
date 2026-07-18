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

Serif never mutates storage in place: `__setitem__` materializes the data, applies the updates, and rebuilds a fresh storage object. Any other Vector still pointing at the old storage is untouched — copy-on-write by construction, no registry or identity tracking needed.

Plain name-binding is ordinary Python: `b = a` makes two names for one
object, and a mutation through either name is visible through both,
exactly as with any Python object. Copy-on-write protects *vectors* from
each other; it does not (and should not) change what `=` means.

### When Copies Happen

- **Mutation:** every mutation rebuilds the mutated Vector's storage; sharers keep the old immutable storage
- **Table construction:** Table performs deep copy to prevent external aliasing
- **Explicit operations:** Methods like `.copy()` always create new data

## Fingerprints

Fingerprints enable **O(1) change detection** without full data comparisons.

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

3. **Track lineage in computational graphs**
   ```python
   def build_pipeline(data):
       steps = []
       
       cleaned = data.clean()
       steps.append(('clean', cleaned.fingerprint()))
       
       transformed = cleaned.transform()
       steps.append(('transform', transformed.fingerprint()))
       
       return transformed, steps
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

