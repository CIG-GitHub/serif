# Mutation

One sentence: **read through the column, write through the table.**

```python
t[t.v == 'Some Value', 'v'] = 'Some Other Value'   # ✓ write through the table
t.v[t.v == 'Some Value'] = 'Some Other Value'      # ✗ raises — write through a read-out column
```

Anything read *out* of a table is a **value**: it never changes, and it
cannot change the table. The table itself is a normal Python object that
you mutate by addressing it. That's the whole model; the rest of this
document is what it means in practice and why the second spelling above
cannot be allowed.

## What works

Everyday edits are one line, no ceremony. Each statement rebuilds the
target column once (O(n)) — nothing is ever written into a buffer that
anything else can see:

```python
t[t.v == 'old', 'v'] = 'new'      # conditional update
t[3, 'v'] = 5                     # one cell
t[0, :] = [1, 2.5, 'x']           # a row
t[0:100, 'v'] = 0                 # a region
t.v = t.v.fillna(0)               # column replacement
t.v += 1                          # no __iadd__: desugars to t.v = t.v + 1
t >>= {'w': t.v * 2}              # add columns
```

Standalone vectors were never frozen — a vector you built is yours:

```python
v = Vector([1, 2, 3])
v[0] = 99                         # fine — wild vectors are mutable
c = t.v.copy()
c[0] = 99                         # fine — a copy is independent, forever
```

## What raises

Any write addressed to a table-owned column:

```python
t.v[0] = 5          # ✗ SerifTypeError — and the error contains the fix
t['v'][0] = 5       # ✗ same path, string spelling
v = t.v
v[0] = 5            # ✗ the same operation, three lines apart
```

## Why: the two spellings are the same spelling

`t.v[mask] = x` desugars to getattr-then-setitem: fetch the column
object, then mutate it. Python cannot distinguish that from

```python
v = t.v         # three cells ago, in a notebook
v[mask] = x     # now
```

Same bytecode shape, same objects. If the first wrote through to the
table, the second would too — mutation flowing through a piece of data
someone carried away. That is the bug pandas spent a decade warning
about (`SettingWithCopyWarning`) without being able to fix, because the
fix requires choosing: either read-outs are live (and aliases mutate
your table from a distance), or read-outs are values (and you write
through the owner). Serif chooses values. The famous pandas silent
no-op:

```python
df[df.a > 10]['v'] = 0            # pandas: modifies a temporary, does nothing
t[t.a > 10, 'v'] = 0              # serif: one expression, does what it says
```

## Read-outs are snapshots (swap-on-write)

An owner write **replaces** the column with a freshly rebuilt one — the
old column object is never touched. So a column you read out earlier
does not observe later table writes:

```python
v = t.v
t[0, 'v'] = 99
v[0]                # the old value — v is a value, not a live view
```

The same holds for every derived object: copies, slices, filtered
results, and other tables sharing storage are all safe by construction,
because no write ever lands in shared memory. (See
[aliasing.md](aliasing.md) for the copy-on-write machinery underneath.)

What *does* observe a write is another name for the same table —
`u = t` — which is ordinary Python object identity, the same as a dict.
Serif protects everything derived from a table; it does not (and should
not) change what `=` means.

## Loops: `batch()`

Rebuilding a column per statement is fine for any number of rows and
quadratic only if you *write in a loop*. First ask whether the loop is
really a batch — one statement, one rebuild:

```python
t[[i1, i2, i3], 'v'] = [x1, x2, x3]
```

If the writes genuinely depend on each other (read-modify-write), use a
batch scope. Entering copies each column's buffers once, so every write
inside can land raw and O(1):

```python
with t.batch() as m:              # m IS t; columns thawed
    for i in hot_indices:
        m.v[i] = fix(m.v[i])
```

Observable semantics are identical to table-addressed writes — anything
snapshotted before the scope is untouched; only the speed differs.
Exiting refreezes everything, including column references that escaped
the scope. Nesting raises. An exception mid-scope leaves the table
partially mutated (no rollback) — a batch is imperative code and owns
its own consistency.

## The payoff

Because no operation anywhere can write into shared memory, every
derived object is a true value: a fingerprint, once computed, describes
frozen bytes ([aliasing.md](aliasing.md)); a filtered selection means
"the table as it was", permanently; and `copy()` is O(1) because sharing
frozen storage is always safe. Table-owns-storage and copy-on-write
coexist precisely because mutation is owner-addressed: the table may
rebuild what it owns *because* nobody else is allowed to write it.
