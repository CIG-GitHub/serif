# Null Semantics

serif is "Python semantics first" — but that doctrine governs **values**,
and a null is not a value. In a typed column a null is literally a mask bit
(`ArrayStorage`/`StringStorage` never store a `None` object at all); it is a
*position with no value* — "we don't know what this is." Python semantics
apply to the values you have. Absence follows the rules below, which are
SQL's three-valued logic where SQL is right about epistemics, and not-SQL
where SQL is just old.

## The doctrine (two rules)

> **Element-wise: unknown in, unknown out.**
> Comparisons, arithmetic, and `~` propagate null. `&` and `|` use Kleene
> logic — the known operand may settle the result.
>
> **Aggregate: summarize what you know.**
> Skip nulls. If nothing remains, return the operation's identity element
> if one exists; otherwise `None`.

Everything else in this document is a consequence of those two sentences.

## Element-wise operations

For a nullable column `v = Vector([1, None, 3])`:

```
v > 2        →  [False, None, True]      # bool? vector
v + 1        →  [2, None, 4]             # arithmetic already propagated
v == v2      →  None wherever either side is null
~(v > 2)     →  [True, None, False]      # NOT unknown is unknown
```

`v == None` yields null at every position (comparing anything to unknown is
unknown) and warns — use `v.isna()` to test for nulls. Note the SQL corollary:
null == null is **null**, not True. Identity of position is not equality of
value; `isna()` is the tool for "which cells are null."

### Kleene tables for `&` and `|` (bool vectors)

The known operand can settle the result; otherwise unknown propagates.

```
AND   | True   False  Null          OR    | True   False  Null
------+--------------------         ------+--------------------
True  | True   False  Null          True  | True   True   True
False | False  False  False         False | True   False  Null
Null  | Null   False  Null          Null  | True   Null   Null
```

`^` (xor) has no settling operand: null with anything is null.

### `&`, `|`, `^` on int vectors are bitwise

Dispatch is by dtype. On **bool** vectors these operators are Kleene
logical. On **int** vectors they are Python's bitwise operators —
`Vector([3]) & 1` is `[1]`, exactly as `3 & 1` is `1`. (Values obey Python;
only absence obeys the doctrine.)

## Aggregations

Aggregates skip nulls — they summarize the values you have. When nothing
remains (all-null or empty input), the result is the operation's identity
element, or `None` when no identity exists:

| aggregate | all-null / empty result | why                    |
|-----------|-------------------------|------------------------|
| `sum`     | `0`                     | additive identity      |
| `count`   | `0`                     | counting identity      |
| `all`     | `True`                  | identity of AND (vacuous truth) |
| `any`     | `False`                 | identity of OR         |
| `max`     | `None`                  | no identity exists     |
| `min`     | `None`                  | no identity exists     |
| `mean`    | `None`                  | no identity exists     |

`all([None, True]) is True` is not "None is truthy" — the null was skipped,
and no known value violated the condition. The element-wise layer already
reported the unknown before you aggregated; aggregation is where you decide
to summarize what's known.

## Filtering and assignment

Boolean masks may be nullable (comparisons on nullable columns produce
them). A null mask entry **excludes** the row — SQL WHERE semantics:

```
v[v > 6]      # rows known to be  > 6
v[~(v > 6)]   # rows known to be <= 6
```

Neither filter includes the null rows, so the two halves do **not** reunite
to the whole table — honestly: the missing rows are the ones you know
nothing about. (Under the previous False-at-null semantics, the complement
filter silently asserted "null <= 6" — a claim about data that doesn't
exist.) To claim the unknowns, `v[v.isna()]`.

Masked assignment follows the same rule: a null mask entry assigns nothing.

## Named deviations

**From Python:** `None > 6` raises in Python; in a vector it yields null.
`all([None])` is `False` in Python (None is falsy); serif's `all()` skips
the null and returns `True` (identity rule). In table land, `None` is
absence, not a falsy sentinel object.

**From SQL:** `SUM` of all-null is NULL in SQL, `0` here (the identity rule;
Excel and Python agree). `EVERY`/`bool_and` of all-null is NULL in SQL,
`True` here (same rule). SQL's choices here are widely regarded as warts;
the identity rule is one principle applied uniformly.

## Explicit null tools

`isna()` — which positions are null. `fillna(x)` — replace nulls with a
value. `dropna()` — remove null positions. These are the only operations
that *look at* nullness; everything else either propagates it (element-wise)
or skips it (aggregate).

## Open question (documented current behavior, not yet doctrine)

Join and groupby keys currently match nulls to each other (Python equality
on the key tuple: `None == None` groups together). SQL says null keys match
nothing. This is unresolved — today's behavior is the Python-equality one.
