# Null Semantics

serif is "Python semantics first" — but that doctrine governs **values**,
and a null is not a value. In a typed column a null is literally a mask bit
(`ArrayStorage`/`StringStorage` never store a `None` object at all); it is a
*position with no value* — "we don't know what this is." Python semantics
apply to the values you have. Absence follows the rules below. The layering:
**Python governs values and answers; SQL governs row-matching.** Joins and
filter masks borrow SQL's three-valued logic deliberately, because matching
rows on unknowns manufactures results out of ignorance (see: pandas). Those
two loans are the exceptions, and they are fenced off below; everywhere
else, when Python has an answer, serif gives Python's answer.

## The doctrine (three rules)

> **Element-wise: unknown in, unknown out.**
> Comparisons, arithmetic, and `~` propagate null. `&` and `|` use Kleene
> logic — the known operand may settle the result. (One carve-out, from
> the third rule: comparing against the literal `None` is not a
> comparison with an unknown — it names absence itself.)
>
> **Aggregate: summarize what you know.**
> Skip nulls. If nothing remains: true math folds return their identity
> (`sum` → `0`, `count` → `0`); statistics with no
> identity return `None` (`max`, `min`, `mean`, `stdev`); and the verdict
> reductions `all()`/`any()` return their identity too (`True`/`False`,
> as Python's `all([])`/`any([])` do) — but warn, unless `on_empty=`
> states the verdict deliberately.
>
> **Explicit absence collapses the unknowns.**
> The moment you name absence yourself — `is_na()`, `fillna()`,
> `v == None`, `on_empty=` — the question has a total answer and the
> unknowns leave the result. `v == None` asks "is this position
> missing" and returns a plain `bool` mask, exactly as mapping
> `== None` over a Python list would.

Everything else in this document is a consequence of those three rules.

## Element-wise operations

For a nullable column `v = Vector([1, None, 3])`:

```
v > 2        →  [False, None, True]      # bool? vector
v + 1        →  [2, None, 4]             # arithmetic already propagated
v == v2      →  None wherever either side is null
~(v > 2)     →  [True, None, False]      # NOT unknown is unknown
v == None    →  [False, True, False]     # missingness test — total, no nulls
```

`v == None` is a missingness test. The scalar `None` is not an unknown
value — columns never store a `None` object — it is the *symbol for
absence*, so comparing to it asks "is this position missing." The result
is a total, non-nullable `bool` mask, the same answer Python gives when
mapping `== None` over a plain list (`None == None` is `True`). It also
warns: `is_na()` is the deliberate spelling, and the warning exists to
catch the `None` you didn't know you had — a leaked `None` variable
silently turning a comparison into a missingness test should be loud.
`v != None` is `~v.is_na()`, same warning.

Between two *columns*, unknowns stay unknowns: `v == w` is null wherever
either side is null, because there you hold two data unknowns that might
or might not be equal. This is where SQL's "null == null is null" is
right, and it is why `v[v == w]` and `v[~(v == w)]` do not reunite to
the whole table.

The sharp edge between those two rules: a null *plucked out* of a column
crosses the line. `w[2]` reads out as Python `None`, so `v == w[2]` is a
missingness test — `True` at v's nulls — while `v == w` at position 2 is
null. The scalar is the symbol; the column cell is data. The warning
fires on every scalar-`None` comparison precisely so that crossing is
never silent.

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
| `all`     | `True`, warns unless `on_empty=`  | AND identity; a verdict from no evidence warns |
| `any`     | `False`, warns unless `on_empty=` | OR identity; a verdict from no evidence warns  |

Every fold with an identity returns it — the empty case has a true answer,
and it is Python's answer (`sum([])`, `all([])`, `any([])`). The
identity-less statistics return `None`, which propagates honestly through
any arithmetic downstream. `all([None, True]) is True` is not "None is
truthy" — the null was skipped, and no known value violated the condition.
The element-wise layer already reported the unknown before you aggregated;
aggregation is where you decide to summarize what's known.

### `all()` / `any()`: identity, but say so out loud

A boolean reduction is a verdict, and its result lands in `if`/`assert`,
where Python coerces anything into a decision silently. That makes the
identity elements risky in exactly this one place:
`t[t.type == 'wire'].amount_ok.all()` with a typo'd filter returns
`True` — a validation that passes on zero evidence. The mirror image:
"any fraud flags?" over a never-populated column returns `False` — an
alarm that doesn't fire.

So when zero valid values survive the null-skip (an empty vector, or one
whose values are all null — one condition, not two), `all()` and `any()`
return the identity **and warn** `SerifEmptyReductionWarning`. The warning
is silenced by finishing the question — `on_empty=` states the empty-case
verdict, and the value you pass is the value you get back:

```
flags.all()                # zero valid values → True, with a warning
flags.all(on_empty=True)   # vacuous truth, opted into deliberately — silent
flags.any(on_empty=False)  # the OR identity, opted into deliberately — silent
```

Anyone who wants the old hard failure back is one filter away:
`warnings.simplefilter('error', SerifEmptyReductionWarning)`.

There is deliberately no `on_empty=None` "return a null verdict" option:
in an `if`, `None` is indistinguishable from `False`, so `on_empty=False`
already covers it. The one thing this closes off — a 2-D → 1-D reduction
that wants to keep truthiness, falseness, and emptiness as three distinct
output values — must be written by hand (like vectorized
shift-via-operator: the surface is spoken for, but you have the tools).

In `aggregate()`/`window()`, empty-verdict groups get the identity and
one warning per output column naming the affected group keys, so you can
tell a data problem ("this group isn't supposed to be empty") from a
legitimate sparse group. For the latter, qualify with a lambda:
`lambda g: g.flag.all(on_empty=False)` — stated verdicts are silent.

This is `Vector.__bool__`'s refusal, one notch softer: `if vec:` raises
because you haven't said which reduction you mean; `all()` over no
evidence answers like Python but warns because you haven't said what the
verdict should be.

## Filtering and assignment

Boolean masks may be nullable (comparisons on nullable columns produce
them). A null mask entry **excludes** the row — SQL WHERE semantics, the
second deliberate SQL loan: a row must positively qualify to pass a
filter.

```
v[v > 6]      # rows known to be  > 6
v[~(v > 6)]   # rows known to be <= 6
```

Neither filter includes the null rows, so the two halves do **not** reunite
to the whole table — honestly: the missing rows are the ones you know
nothing about. (Under the previous False-at-null semantics, the complement
filter silently asserted "null <= 6" — a claim about data that doesn't
exist.) To claim the unknowns, `v[v.is_na()]`.

Masked assignment follows the same rule: a null mask entry assigns nothing.

## Named deviations

**From Python:** `None > 6` raises in Python; in a vector it yields null.
`all([])` is `True` and `any([])` is `False` in Python, and serif agrees —
the identity, plus a warning. The one remaining verdict deviation:
`all([None])` is `False` in Python (None is a falsy *object*), but `True`
here (the null is *skipped*, leaving the empty case). In table land,
`None` is absence, not a falsy sentinel object.

**From SQL:** `x = NULL` never matches in SQL; serif's `v == None` is a
missingness test that matches the null positions — Python's answer
(`None == None` is `True`), delivered with a warning. `SUM` of all-null
is NULL in SQL, `0` here (the identity rule; Excel and Python agree).
`EVERY`/`bool_and` of all-null is NULL in SQL; serif returns the
identity and warns — SQL's NULL at least refuses to render a verdict,
but it then coerces silently in a `WHERE`; the warning makes the empty
case loud without breaking Python's answer.

## Explicit null tools

`is_na()` — which positions are null. `fillna(x)` — replace nulls with a
value. `dropna()` — remove null positions. `v == None` / `v != None` —
the comparison spellings of `is_na()` / `~is_na()` (they warn; the named
methods are the deliberate form). These are the only operations that
*look at* nullness; everything else either propagates it (element-wise)
or skips it (aggregate).

## Null keys in joins and grouping

Joins and grouping ask different questions, so null keys follow different
rules.

**Joins:** the first deliberate SQL loan. A key containing a null never
matches another key — a join predicate must be positively true, and equality
involving an unknown *data* value is unknown, not true; matching rows on
unknowns manufactures results out of ignorance (see: pandas). In an inner
join, null-key rows therefore do not appear. In a left
or full join, they survive only as unmatched rows. For a multi-column key, a
null in any component makes the complete key non-matching. Repeated null keys
do not violate `expect_left_unique` or `expect_right_unique`, because they
cannot produce multiple matches.

**Grouping and windows:** null keys form groups. Grouping classifies rows
rather than asserting equality, and silently dropping null-key rows would
discard data. Rows with the same key, including the same null components, are
placed in one bucket; `(None, 'A')` and `(None, 'B')` remain distinct groups.
