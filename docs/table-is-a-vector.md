# Why Table Is a Vector

A `Table` is a `Vector` whose elements are column `Vector`s.

This is not an implementation convenience, an analogy, or a choice between
inheritance and composition. It is the algebraic model of Serif. The ordinary
`Vector` rules extend recursively to a `Table`, and to deeper nestings of
vectors, without inventing a second set of table semantics.

## The model

A scalar vector has one linear, storage-owning dimension:

```text
Vector[Scalar]
```

A table adds an outer vector dimension whose elements are columns:

```text
Vector[Vector[Scalar]]
```

The outer vector is the table. Its elements are the column vectors. A deeper
nested value follows the same rule:

```text
Vector[Vector[Vector[Scalar]]]
```

The model is recursive. Rank changes; the algebra does not.

## Reductions reduce the innermost dimension

A reduction consumes the innermost vector and preserves every outer
dimension.

For a scalar vector:

```text
Vector[Scalar].sum() -> Scalar
```

For a table:

```text
Vector[Vector[Scalar]].sum() -> Vector[Scalar]
```

Each column is summed independently, producing one result per column. The
same rule applies to `mean`, `min`, `max`, `all`, `any`, and every other
reduction, subject to the dtype and null semantics of each innermost vector.

For a deeper nesting:

```text
Vector[Vector[Vector[Scalar]]].sum()
    -> Vector[Vector[Scalar]]
```

Again, only the innermost dimension disappears. The operation does not need a
new meaning at each rank.

## Pointwise operations lift recursively

A pointwise operation applies to every scalar element while preserving the
shape of the nested vectors.

```text
Vector([1, 2, 3]) + 1
    -> Vector([2, 3, 4])

Table([[column a], [column b]]) + 1
    -> Table([[column a + 1], [column b + 1]])
```

The table case is not separate table arithmetic. It is the same operation
lifted through the outer vector and applied to each innermost column.
Comparisons, arithmetic, logical operations, casts, and other elementwise
transforms follow this rule.

Vector-to-vector operations likewise recurse through matching structure and
eventually operate on scalar leaves. Serif's existing shape, dtype, null, and
error rules still govern the innermost operation. Nesting does not create a
second coercion or broadcasting system.

## Why this is also the fast model

The innermost vector is the linear storage dimension. That is where Serif can
perform a tight pure-Python loop or apply an optional NumPy or Arrow kernel.

A table operation therefore does not require a separate table compute engine:

1. lift the operation through the outer vector;
2. run the vector operation on each column;
3. rebuild the same outer structure.

Making the innermost vector fast makes the table fast. Optional accelerators
remain narrow vector kernels operating on Serif's existing storage. They do
not need parallel implementations of table arithmetic or table reductions.

## What is actually Table-specific

Only operations whose meaning depends on table structure need table-specific
algebra:

- `T` changes the axes and therefore changes the nesting structure.
- Joins relate rows through one or more key columns and construct a new table
  shape.
- Aggregation partitions rows into groups before applying reductions and
  constructing the result table.

These operations cannot be obtained merely by lifting an ordinary vector
operation over columns. They belong to the Table layer.

Table construction, names, row access, display, selection, and owner-addressed
mutation still require table-aware mechanics. Those mechanics maintain the
outer structure; they do not define an alternate arithmetic, pointwise, or
reduction algebra.

## Architectural consequence

Serif must not maintain duplicate Vector and Table implementations of ordinary
pointwise operations, transforms, or reductions. There is one recursive
operation model:

```text
outer vectors preserve structure
        -> innermost vectors perform the operation
        -> storage kernels perform the scalar work
```

The Table layer owns only the behavior that cannot be derived from that model.
If a proposed Table implementation gives arithmetic or reductions a meaning
different from recursive Vector lifting, the implementation is wrong.

## The invariant

> A Table is a Vector. Pointwise operations lift through its columns,
> reductions consume its innermost dimension, and making the innermost Vector
> fast makes the Table fast. Only transpose, joins, and aggregation require
> distinct Table algebra.
