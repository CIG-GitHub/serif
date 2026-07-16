# Indexing Rules

Vector and Table use a strict, predictable indexing model built around
two principles:

1. **Column-major tables** — columns are primary, rows are derived.
2. **One meaning per form** — a single-axis subscript always means rows
   (or a named column); the two-axis form `t[rows, cols]` is supported
   and unambiguous; everything else composes.

## 1. Vector Indexing

A Vector has three primary forms of indexing:

### 1.1 Integer index

```python
v[i]
```

Returns a Python scalar of the underlying dtype.

### 1.2 Slice

```python
v[i:j:k]
```

Returns a new Vector of the same dtype.

### 1.3 Boolean mask

```python
v[mask]
```

Rules:
- `mask` must be a Vector(bool) of the same length.
- Returns a filtered Vector.
- Masks are produced by comparisons, `.like()`, `.isin()`, etc.

### 1.4 Discouraged and disallowed forms

- Lists/vectors of integer indices are accepted but **discouraged** — they
  are slow relative to masks and emit a warning (see docs/gotchas.md).
- Broadcasting and multi-dimensional indexing of vectors are disallowed.

Boolean masks are the sanctioned way to filter.

## 2. Table Indexing

A single-axis subscript selects rows (integer, slice, mask) or columns
(names). The two-axis form `t[rows, cols]` addresses cells and regions
directly. All forms compose.

### 2.1 Row Indexing

A single index always refers to **rows**.

#### Integer

```python
t[i]
```

Returns row `i` as a tuple-like record.

#### Slice

```python
t[i:j]
t[:]
```

Returns a new Table containing a subset of rows.

#### Boolean mask

```python
t[mask]
```

Filters rows across all columns.

Mask rules:
- must be a Vector(bool) of length equal to the number of rows
- mask creation is separate from filtering
- **only boolean masks filter rows** (never columns)

### 2.2 Column Selection

Columns may be selected via:

#### String name

```python
t['col']
t.col
```

Returns a Vector.

#### Tuple of names (multi-column select)

```python
t['a', 'b', 'c']
```

Returns a new Table containing those columns (in order).

**Name resolution:**
- Full column names are always valid.
- Disambiguated names are also valid (first occurrence of a duplicate name).
- Python passes this as a single tuple key — no conflict with row indexing.

#### Column index selection

Using the helper:

```python
t.cols(2)           # the column at position 2 (a Vector)
t.cols(slice(3, 8)) # columns 3–7, as a tuple of Vectors
t.cols()            # all columns, as a tuple of Vectors
```

`cols()` takes an int, a slice, or nothing. It returns Vectors (a single
one, or a tuple) — not a Table. For a multi-column *Table*, select by
name: `t['a', 'b']`.

#### Chaining is fully supported

```python
t['a', 'b'][10:20]
t[mask]['x', 'y']
```

### 2.3 Two-Axis Indexing

The tuple form `t[rows, cols]` is supported for reading and writing.
Rows are specified by integer or slice; columns by position, slice, or
name. Either axis order works — the axis types disambiguate.

```python
t[0, 0]              # a single cell (a Python scalar)
t[5, 'b']            # cell by row and column name
t[:, 'a']            # a whole column
t[1:3, 0:2]          # a rectangular region
```

Assignment mirrors reading:

```python
t[0, 0] = 99         # one cell
t[3, :] = None       # a whole row (promotes columns to nullable)
t[:, 'b'] = 42       # a whole column
t[0:2, 'a'] = 100    # a slice of a column
t[1:3, 0:2] = 999    # a rectangular region
```

#### Masks stay on the single-axis form

Boolean masks are not accepted inside the two-axis form:

```python
t[mask, 'col']   # raises — write t[mask]['col'] or t['col'][mask]
```

Masks filter rows via the single-axis form only.

## 3. Recommended Idioms

### Column-first selection (preferred for large datasets)

Because Table is column-major, selecting columns first reduces the memory
footprint and speeds up operations:

```python
t['a', 'b'][1000:2000]
t['a', 'b'][mask]
```

### Row-first selection (equally valid)

```python
t[1000:2000]['a', 'b']
```

Composability guarantees these two forms are equivalent.

### Mask → column select

```python
t[mask]['col']
t['col'][mask]
```

Either direction is legal and predictable.

## 4. Design Philosophy

- Table is column-major; columns are the primary structural axis.
- Every subscript form has exactly one meaning: single-axis selects rows
  or named columns; the two-axis form addresses cells and regions.
- Rows are indexed by integer, slice, or boolean mask.
- Columns are selected by name, or positionally via `.cols()`.
- Multi-column selection via `t['a', 'b', 'c']` is first-class and preferred.
- Both full names and disambiguated names are valid for column selection.
- No broadcasting, no silent reindexing.
- If an operation is legal in one context, it is legal everywhere else.

The model is intentionally strict, minimal, and easy to reason about.
It is designed for clarity, correctness, and high user ergonomics rather than
feature maximalism.

