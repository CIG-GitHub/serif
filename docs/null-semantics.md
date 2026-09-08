# Null Semantics

serif is "Python semantics first": known values use Python operations and
truthiness. A null is a *position with no value* — "we don't know what this
is." Comparisons propagate that uncertainty, and logical operations use
Kleene three-valued logic. Joins and filter masks require a positively true
match, following SQL row-matching semantics.

## The doctrine (three rules)

> **Unknowns remain unknown unless known values settle the answer.**
> Element-wise comparisons, arithmetic, and boolean inversion propagate
> null. Boolean `&` and `|`, and the reductions `all()` and `any()`, use
> Kleene logic: a known false settles AND; a known true settles OR.
>
> **Summaries use known values; logical reductions fold truth values.**
> Summaries such as `sum`, `count`, `min`, `max`, and statistics skip
> nulls. With no known values, they return an identity where one exists
> (`sum` → `0`, `count` → `0`) or `None` otherwise.
> `all()` / `any()` retain uncertainty. Only actually empty input yields
> their identities (`True` / `False`) with a warning unless `on_empty=`
> explicitly chooses the empty result.
>
> **Handle absence explicitly.**
> Use `is_na()` to test missingness, `fillna(x)` to replace nulls, and
> `dropna()` to remove them. `None` among `is_in()` members includes
> absence as a category. Scalar `== None` and `!= None` raise;
> `on_empty=` controls empty input only, not unknown values.

## Element-wise operations

For a nullable column `v = Vector([1, None, 3])`:

```
v > 2        →  [False, None, True]      # bool? vector
v + 1        →  [2, None, 4]             # arithmetic already propagated
v == v2      →  None wherever either side is null
~(v > 2)     →  [True, None, False]      # NOT unknown is unknown
v == None    →  SerifTypeError          # use v.is_na()
v != None    →  SerifTypeError          # use ~v.is_na()
v.is_na()    →  [False, True, False]     # total bool mask
```

Equality and inequality against scalar `None` raise `SerifTypeError`
with guidance to use `is_na()` or `~is_na()`. The rule includes reversed
operands (`None == v`, `None != v`), categoricals, tables, and empty
inputs. A scalar comparison must not accidentally become a missingness
test when a variable contains `None`.

Between two columns, `v == w` and `v != w` remain null wherever either
side is null: two unknown values might or might not be equal. Consequently,
`v[v == w]` and `v[~(v == w)]` do not reunite to the whole vector.

Extracting a null from a column produces Python `None`. If `w[2] is None`,
`v == w[2]` raises, while comparing the full columns still propagates null
at position 2. Use `v.is_na()` when missingness is the intended question.

### `is_in()`: membership, and absence as a category

`is_in()` preserves unknown membership unless absence is explicitly included.
Membership is Python `==`
(`Vector([1, 2]).is_in([2.0])` matches — the numeric tower is Python's),
and null positions yield null: whether an unknown value is in the group
is unknown. But `None` *among the members* names absence itself:
`v.is_in([1, None])` is `v.is_in([1]) | v.is_na()`, and the mask is
total. This is the classification reading, the same one grouping uses —
the group enumerates categories, and "missing" is a category you may
list. No warning: an enumerated group is deliberate. (A group sourced
from data can carry incidental nulls — `v.is_in(other.key)` — and they
claim your null rows under the same uniform rule; `dropna()` the group
if that is not what you meant.) Members whose *type* can never equal
the vector's dtype do warn: the answer — they match nothing — is
Python's, but a dead member is usually a typo'd group.

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

Summaries such as `sum`, `count`, `min`, `max`, and statistics skip
nulls. Logical reductions `all()` / `any()` fold them using Kleene logic.
Empty and all-null inputs therefore have different logical results:

| aggregate | nonempty all-null result | empty result |
|-----------|--------------------------|--------------|
| `sum`   | `0`                    | `0`        |
| `count` | `0`                    | `0`        |
| `max`   | `None`                 | `None`     |
| `min`   | `None`                 | `None`     |
| `mean`  | `None`                 | `None`     |
| `std`   | `None`                 | `None`     |
| `stdev` | `None`                 | `None`     |
| `all`   | `None`                 | `True`, warns unless `on_empty=` chooses a verdict |
| `any`   | `None`                 | `False`, warns unless `on_empty=` chooses a verdict |

`sum` and `count` have identities; the statistics listed above have no
answer without known observations. Logical folds also have identities,
but a nonempty collection of unknowns is not empty. For example,
`Vector([None, True]).all()` is `None`: the known true does not establish
that every element is true.

### `v.stats`: strip unknown observations before calculating

Real numeric vectors expose Python-style statistics through `v.stats`.
Univariate statistics remove null observations before calculating. If too few
known observations remain for the requested statistic, the result is `None`:
one known value is enough for population variance but not sample variance, and
two are required for quantiles. `multimode()` is the deliberate exception; it
returns `[]` over zero known observations, matching Python's meaningful empty
answer. Invalid known values still raise the underlying Python domain or type
error.

`covariance()`, `correlation()`, and `linear_regression()` first require equal
original lengths, then remove every position where either side is null. They
return `None` when fewer than two complete pairs remain. Weighted
`harmonic_mean()` follows the same pairwise rule for values and weights. This
is the same coordinate policy as `v.math.dist()`: dimensions describe the
original data, while the calculation summarizes only complete observations.

`v.mean()` aliases `v.stats.mean()` and `v.std()` aliases the sample
`v.stats.stdev()`. Use `v.stats.pstdev()` when the observations are the entire
population. NumPy may accelerate fixed-width statistics; discrete results
retain exact Python semantics, while floating reductions can differ in their
last bits because reduction order is not numerically associative.

### `all()` / `any()`: Kleene folds

`all()` folds AND: any known falsy value settles `False`; otherwise,
a null yields `None`, and all known truthy values yield `True`.
`any()` folds OR: any known truthy value settles `True`; otherwise,
a null yields `None`, and all known falsy values yield `False`.
Known nonboolean values retain Python truthiness.

| vector contents | `all()` | `any()` |
|-----------------|-----------|-----------|
| `[True, None]`  | `None`  | `True`  |
| `[False, None]` | `False` | `None`  |
| `[None, None]`  | `None`  | `None`  |
| `[True, False, None]` | `False` | `True` |

Use `v.dropna().all()` or `v.dropna().any()` to deliberately reduce only
known values. If dropping nulls leaves an empty vector, the empty-input
policy below applies.

#### Empty inputs

With zero elements, `all()` returns `True` and `any()` returns `False`,
as Python does. Both warn `SerifEmptyReductionWarning`: a filter that
accidentally selects no rows can otherwise pass a validation or suppress
an alarm. Set `on_empty=True` or `on_empty=False` to choose the empty
result and silence the warning:

```python
flags = Vector([])
flags.all()                # True, with a warning
flags.all(on_empty=True)   # True, silent
flags.any(on_empty=False)  # False, silent

Vector([None]).all(on_empty=True)   # None, silent
Vector([None]).any(on_empty=False)  # None, silent
```

`on_empty=None` retains the default identity-and-warning behavior; it
does not request a null result for empty input. Other values besides
`True`, `False`, and `None` raise `SerifTypeError`.
To make empty reductions raise, use
`warnings.simplefilter('error', SerifEmptyReductionWarning)`.

#### Python conditions

The reduction returns `bool | None`. Python treats `None` as false in
an `if` or `assert`:

```python
v = Vector([6, None])
verdict = (v > 5).all()     # None
if verdict:
    proceed()              # not entered
```

Entering the branch requires a positively true verdict. The other branch
means "not established that every value exceeds 5"; it can represent
unknown evidence as well as a known failure. Likewise,
`if not (v > 5).any():` enters for an unknown result, which does not
establish that no value exceeds 5. Use `verdict is None`,
`verdict is False`, and `verdict is True` when those cases need separate
handling. The reduction preserves uncertainty; Python conditions collapse
it through truthiness.

#### Tables, aggregates, and windows

Table reductions apply the same fold to each column and return a boolean
vector, nullable when an output is unknown. Even when every output is
`None`, the vector keeps its boolean type and supports `&`, `|`, and `~`.

Bound reductions in `aggregate()` and `window()` use the same semantics.
This also applies to per-column blocks in `aggregate()`. All-null groups
return `None` without an empty warning. Bound logical outputs retain their
boolean type even when all groups are unknown. Arbitrary lambdas use
ordinary result type inference.

An ungrouped aggregation over a table with zero rows still evaluates one
empty group: bound logical reductions return their identities and warn
once per output column, identifying the whole table. A callable's empty
reduction emits the vector-level warning. Qualify it explicitly when
needed: `lambda g: g.flag.all(on_empty=False)`.
Grouped aggregations and windows with no groups produce empty result
columns; no reduction is evaluated and no empty warning is emitted.
Their bound logical output schemas derive nullability from the source.

## Filtering and assignment

Boolean masks may be nullable (comparisons on nullable columns produce
them). A null mask entry **excludes** the row — SQL WHERE semantics:
a row must positively qualify to pass a filter.

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

**From Python:** scalar equality and inequality against `None` raise in
serif; use an explicit missingness test. Element-wise comparisons involving
null positions propagate null. Python's `all([None])` and `any([None])`
are `False`, treating `None` as a falsy object; serif returns `None`,
treating the position as unknown. Empty logical reductions retain Python's
identities, with an additional warning unless `on_empty=` chooses a verdict.

**From SQL:** `x = NULL` does not match rows; serif rejects `v == None`
and provides `is_na()` for missingness. Serif's `sum` over all-null input
returns `0`, its additive identity. Logical reductions use the Kleene folds
above, including the distinction between empty and nonempty all-null input.

## Explicit null tools

`is_na()` tests which positions are null; `~is_na()` tests which are
present. `fillna(x)` replaces nulls with a chosen value. `dropna()` removes
null positions. `is_in([..., None])` includes absence as a membership
category. Scalar `v == None` and `v != None` raise and direct callers to
`is_na()` or its complement.

`on_empty=` chooses a logical reduction's result for zero elements. It
does not replace null values or override a nonempty unknown verdict.

## Null keys in joins and grouping

Joins and grouping ask different questions, so null keys follow different
rules.

**Joins:** a key containing a null never
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
