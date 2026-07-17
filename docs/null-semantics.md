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
> Skip nulls. If nothing remains: true math folds return their identity
> (`sum` → `0`, `count` → `0`); statistics with no
> identity return `None` (`max`, `min`, `mean`, `stdev`); and the verdict
> reductions `all()`/`any()` refuse to guess — they raise unless
> `on_empty=` says what the verdict should be.

Everything else in this document is a consequence of those two rules.

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
only absence obeys the doctrine.) Every other dtype raises `SerifTypeError`:
`1.5 & 2.5` is a TypeError in Python, so it is one here too.

## Aggregations

Aggregates skip nulls — they summarize the values you have. When nothing
remains (all-null or empty input), there are three tiers:

| aggregate | all-null / empty result | why                        |
|-----------|-------------------------|----------------------------|
| `sum`     | `0`                     | additive identity          |
| `count`   | `0`                     | counting identity          |
| `max`     | `None`                  | no identity exists         |
| `min`     | `None`                  | no identity exists         |
| `mean`    | `None`                  | no identity exists         |
| `stdev`   | `None`                  | no identity exists         |
| `all`     | raise unless `on_empty=` | a verdict needs evidence  |
| `any`     | raise unless `on_empty=` | a verdict needs evidence  |

The math folds have a true answer for the empty case — the fold identity —
and the identity-less statistics return `None`, which propagates honestly
through any arithmetic downstream. `all([None, True]) is True` is not
"None is truthy" — the null was skipped, and no known value violated the
condition. The element-wise layer already reported the unknown before you
aggregated; aggregation is where you decide to summarize what's known.

### `all()` / `any()`: verdicts need evidence

A boolean reduction is a verdict, and its result lands in `if`/`assert`,
where Python coerces anything into a decision silently. That makes the
identity elements dangerous in exactly this one place:
`t[t.type == 'wire'].amount_ok.all()` with a typo'd filter would return
`True` — a validation that passes on zero evidence. The mirror image:
"any fraud flags?" over a never-populated column would return `False` — an
alarm that silently doesn't fire.

So when zero valid values survive the null-skip (an empty vector, or one
whose values are all null — one condition, not two), `all()` and `any()`
raise `SerifEmptyReductionError` unless you finish asking the question:

```
flags.all()                # zero valid values → raises
flags.all(on_empty=True)   # vacuous truth, opted into deliberately
flags.any(on_empty=False)  # the OR identity, opted into deliberately
```

The value you pass is the value you get back. There is deliberately no
`on_empty=None` "return a null verdict" option: in an `if`, `None` is
indistinguishable from `False`, so `on_empty=False` already covers it.
The one thing this closes off — a 2-D → 1-D reduction that wants to keep
truthiness, falseness, and emptiness as three distinct output values —
must be written by hand (like vectorized shift-via-operator: the surface
is spoken for, but you have the tools).

In `aggregate()`/`window()`, a group with zero valid values raises the
same error, re-raised with the group key and output column attached, so
you can tell a data problem ("this group isn't supposed to be empty")
from a legitimate sparse group. For the latter, qualify with a lambda:
`lambda g: g.flag.all(on_empty=False)`.

This is `Vector.__bool__`'s refusal, one step later: `if vec:` raises
because you haven't said which reduction you mean; `all()` over no
evidence raises because you haven't said what the verdict should be.

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
`all([None])` is `False` in Python (None is falsy) and `all([])` is `True`
(vacuous truth); serif's `all()` raises on both — the nulls are skipped,
no evidence remains, and a verdict from no evidence is the footgun. In
table land, `None` is absence, not a falsy sentinel object.

**From SQL:** `SUM` of all-null is NULL in SQL, `0` here (the identity
rule; Excel and Python agree). `EVERY`/`bool_and` of all-null is NULL in
SQL; serif raises unless qualified — SQL's NULL at least refuses to render
a verdict, but it then coerces silently in a `WHERE`; the raise makes the
refusal loud.

## Explicit null tools

`isna()` — which positions are null. `fillna(x)` — replace nulls with a
value. `dropna()` — remove null positions. These are the only operations
that *look at* nullness; everything else either propagates it (element-wise)
or skips it (aggregate).

## Open question (documented current behavior, not yet doctrine)

Join and groupby keys currently match nulls to each other (Python equality
on the key tuple: `None == None` groups together). SQL says null keys match
nothing. This is unresolved — today's behavior is the Python-equality one.
