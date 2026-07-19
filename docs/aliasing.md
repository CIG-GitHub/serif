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

## Deterministic fingerprints

`fingerprint()` returns a deterministic identity for a Vector or Table. It is
suitable for dependency graphs, persistent caches, and ordinary change
detection because it describes both the data and the analytical schema.

### Basic Usage

```python
v = Vector([1, 2, 3])
fp1 = v.fingerprint()

v[1] = 10
fp2 = v.fingerprint()

assert fp1 != fp2  # Fingerprint changed
```

### Use cases

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

### Contract

The 64-character, versioned BLAKE2b digest covers:

- vector/table dimensions and names
- dtype and nullability
- categorical order and decimal scale/precision
- canonically encoded Python values

The digest is deterministic across Python hash seeds and execution backends.
It is recomputed in O(n) on every call; persistent identity favors certainty
over metadata cache bookkeeping. Unknown object values raise with a conversion
remedy rather than falling back to a potentially address-bearing `repr()`.

Fingerprints answer "does this value and schema have the same identity?", not
"are these equal?". For comparison, remember `==` is **elementwise** (it
returns a boolean vector, with `None` where either side is null).

- Names and schema are intentional parts of identity, so equal-looking values
  with different analytical roles can have different fingerprints.
- Supported values use canonical encodings; arbitrary objects must first be
  converted to a supported Python type.
- Fingerprinting is O(n), so callers should retain the returned digest at the
  dependency-graph or cache layer when repeated checks are unnecessary.

