# Gotchas

Sharp edges worth knowing up front. Each of these is deliberate — a
consequence of a design rule, not an accident — but they can surprise on
first contact.

## Filter with masks, not index lists

```python
# ANTI-PATTERN: slow, and emits a warning
indices = [1, 5, 9]
result = v[indices]

# IDIOMATIC
mask = (v > threshold)
result = v[mask]
```

Boolean masks are the one way to filter. Subscripting with a list of
positions works but warns — in serif a warning always means something.

## `.index()` on a Python list of Vectors

`==` on vectors is elementwise, so `list.index()` — which uses `==`
internally — does not do what it does for scalars:

```python
# WRONG: invokes elementwise equality, returns a boolean vector
cols = [table.year, table.month]
idx = cols.index(table.year)

# CORRECT: identity check
for idx, col in enumerate(cols):
    if col is table.year:
        ...
```

## `len()` counts positions; `count()` counts values

```python
v = Vector([10, None, 20])
v.sum()     # 30 — aggregates skip None
v.count()   # 2  — non-null values
len(v)      # 3  — positions, like any Python container
```

The same distinction applies inside `aggregate()`: `{'n': len}` counts
group rows, `{'n': t.col.count}` counts non-null values in the group.
On fully-populated columns they agree; on nullable columns they do not —
choose the one you mean.

## `count()` on string vectors is arity-overloaded

`str.count(sub)` is core Python, so string vectors must support it — and
`count()` is also the universal non-null aggregate. Arity disambiguates:

```python
s = Vector(['aa', None, 'abca'])
s.count()      # 2 — zero args: the aggregate (non-null values)
s.count('a')   # 2, None, 2 — with args: elementwise str.count
```

No valid Python call changes meaning (zero-arg `str.count()` is a
`TypeError` in plain Python), but note the return types differ: the
aggregate returns an `int`, the elementwise form returns a `Vector`.

## `<<` and `>>` build tables

Shift operators are spoken for: `a >> b` column-binds vectors into a
Table, `t << v` appends a column. Integer bit-shifts are available as
`bit_lshift`/`bit_rshift`. See
[serif-vs-python.md](serif-vs-python.md) for the full list of
intentional departures.
