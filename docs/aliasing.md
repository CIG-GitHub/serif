# Aliasing

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
