# Vector vs Python: Intentional Differences

Vector is built for analytic workflows, not as a drop-in replacement
for Python lists. In a few places, we *intentionally* depart from Python's
semantics because Python's defaults would produce surprising or unsafe
behavior for data work.

These differences fall into a small, well-defined set:

## 1. No Hidden Sharing (Value Semantics Between Vectors)

```python
a = Vector([1, 2, 3])
b = a[:]        # slices and derived vectors are distinct vectors
b[1] = 99       # mutates b only — a is untouched
```

Distinct vectors never share mutable state. Internally they may share
immutable storage, but mutation always rebuilds the mutated vector's
storage (copy-on-write), so no vector can change another behind your
back — including columns already snapshotted into tables or pipelines.

Plain name-binding is unchanged Python: `b = a` makes two names for one
object, and mutating through either is visible through both. The value
semantics are between *vectors*, not between *names*.

## 2. Column Behavior Inside Tables

When a vector is inserted into a Table, the table receives an independent
value snapshot. The new column shell normally shares immutable storage in O(1);
subsequent mutation rebuilds only the owner being changed. Mutating the
original vector therefore cannot affect the table, and table writes cannot
affect an external vector.

The boundary includes metadata: a read-out table column cannot rename its
owner through `.vector_name` or `.alias()`. Rename through `Table.rename()`.

This ensures table operations are deterministic and isolating.

## 3. Overloaded Operators (`<<` and `>>`)

Vector overloads two operators that have no standard meaning
for data structures in Python:

- `a >> b` → column-bind two vectors into a Table
- `t << v` → append a column to an existing table

These operators are chosen because:

- they express "append/concatenate" visually
- they avoid conflict with Python arithmetic
- they keep table expressions compact in notebooks

If you are accustomed to native Python's meaning of shift operators,
be aware that Vector uses them exclusively for table assembly.

## 4. Boolean Operators Behave Numerically

Booleans follow Python's numeric rules:

- `True` → `1`
- `False` → `0`

This allows:

```python
(a > 0).sum()   # counts True elements
(b == c) * d    # boolean mask weighted by numeric vector
```

But certain unary operations (e.g., `-b` for boolean `b`) are disabled,
because they have no clear semantic meaning in data analysis.

## 5. Slices, Masks, and Column Indexing Use Data-Model Semantics

Vector supports a richer indexing model than normal Python:

- column-first indexing
- boolean masks returning filtered vectors
- mixed string + integer column selection in tables
- no aliasing through slicing

These follow analytic conventions (NumPy, R, Pandas, SQL)
rather than Python's built-in list rules.

## Summary

Vector is Pythonic in syntax but analytic in semantics.

If you are a Python programmer, the two areas to be most aware of are:

1. Distinct vectors never share mutable state (value semantics between vectors).
2. Shift operators (`<<` and `>>`) build tables, not bit-shifts.

Everything else behaves naturally once you work with the data model.

