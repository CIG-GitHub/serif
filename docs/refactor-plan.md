# Vector and Table Refactor Plan

## Purpose

This document is the durable handoff for the multi-PR Vector and Table
refactor. A later working session should read this file together with
`docs/table-is-a-vector.md` before proposing or making changes.

The refactor is structural first. Existing public behavior remains the
authority unless a semantic change is separately specified, tested, and
approved.

## Governing model

A `Table` is a `Vector` whose structural children are column `Vector`s.

Ordinary Vector algebra applies recursively:

- pointwise operations lift through outer vectors to scalar leaves;
- reductions consume the innermost dimension and preserve outer dimensions;
- making the innermost Vector operation fast makes the Table operation fast.

Table behavior falls into three categories.

### Lifted Vector algebra

- arithmetic;
- comparisons;
- logical operations;
- pointwise transforms;
- innermost reductions.

Table must not maintain alternate implementations of this algebra.

### Row-aware structural operations

- row selection and owner-addressed mutation;
- `sort_by()`;
- row-wise `dropna()`;
- row-wise `unique()`;
- deferred-mask propagation.

These coordinate columns with one shared row selection or permutation. They
use Vector primitives but require Table-aware orchestration. They are not an
alternate scalar algebra.

### Distinct Table algebra

- `T`;
- joins;
- aggregation and windowing.

These change axes, relate rows through keys, or partition rows before applying
reductions.

Public Table iteration and integer indexing expose rows even though columns
are the structural children used for recursive lifting. Implementations must
therefore use an explicit internal column traversal and must not infer the
algebra from public `__iter__` behavior.

Future homogeneous Matrix and Tensor types follow recursive lifting and
innermost reduction strictly. Table is heterogeneous by column, so the result
container and dtype rule for Table reductions must be specified separately
before Table reduction behavior is added or changed.

## Target ownership

The concrete public classes should live in the modules named for them:

```text
src/serif/
    vector.py          Vector state, identity, invariants, public method shells
    table.py           Table state, identity, and Table-specific public surface

    _vector/
        construction.py
        selection.py
        mutation.py
        operators.py
        reductions.py
        transforms.py
        element_api.py
        numeric.py
        string.py
        dates.py
        categorical.py
```

There is no `_vector/base.py` in the completed design. `Vector` is a concrete
type, not an abstract base class. The real class lives in `serif/vector.py`;
that file is not a re-export facade for a second hidden Vector definition.

Public methods remain on `Vector` and `Table`, but their bodies should be thin
delegates to semantic functions. The classes own identity, state, invariants,
metadata, and API shape. Private modules own operation implementations.

Prefer ordinary functions over mixins, monkey-patching, or service objects.
Semantic modules should operate against the smallest internal protocol they
need rather than importing concrete public classes merely for type checks.

## Dependency direction

Dependencies should flow toward lower-level contracts:

```text
public Vector/Table classes
        -> semantic operation modules
        -> execution dispatch
        -> optional kernels
        -> Serif storage

construction
        -> dtype rules
        -> Serif storage

Table semantic modules
        -> Vector semantic operations
```

Storage, dtype, execution, and accelerator modules must not import `Vector` or
`Table` to construct public results. Semantic operation modules determine
schemas, names, null behavior, and errors, then the public class wraps the
result.

I/O may depend on Table and Vector. Core Table and Vector modules must not
depend on a particular file format. A deferred Table contract may live in the
Table layer, while Parquet decoding and source management remain under `io/`.

## Backend contract for the later execution phase

Serif has one set of semantics with optional, per-operation acceleration.
NumPy and Arrow are not alternate Vector or Table representations.

- Semantic validation happens before acceleration.
- Backend selection is per operation and per call, never per Vector.
- An unsupported physical case returns the unique `DECLINED` sentinel.
- `None` never means decline because it is a valid Serif result.
- Unknown dtype, storage, or capability falls back to the canonical pure path.
- Invalid user operations raise; they do not fall back.
- Backend defects raise; dispatch must not catch broad exceptions and disguise
  them as unsupported cases.
- Accelerators return Serif storage or canonical Python scalars, never exposed
  NumPy or Arrow values.
- The pure implementation is the semantic authority, not a peer backend named
  `default`.
- Dispatch order and capability decisions must be deterministic and directly
  testable.

This contract is recorded now, but backend dispatch is not part of PR 1.

## Pull request sequence

The intended delivery is approximately five sequential pull requests, not one
repository-wide change and not one pull request per mechanical commit.

### PR 1: Decompose Vector semantics

Turn the current Vector monolith into a concrete public class with thin method
shells and private semantic modules. Preserve the existing acceleration calls
without redesigning dispatch.

### PR 2: Decompose Table lifting and row-aware structure

Route lifted algebra through Vector semantics. Extract column traversal,
selection, mutation, row views, naming coordination, sorting, row-wise
`dropna()`, row-wise `unique()`, and deferred-mask coordination.

### PR 3: Extract distinct Table algebra

Extract transpose, joins, aggregation, and windowing. Keep their orchestration
separate from the Vector kernels they use.

### PR 4: Introduce backend directories and execution dispatch

Keep `serif/vector.py` as the concrete public class: it owns identity, state,
invariants, and thin public method shells. It must not become an abstract type
or absorb backend policy.

Operation-facing modules such as `_vector/operators.py` and
`_vector/reductions.py` own semantic validation, dtype and null rules,
exceptions, deterministic backend selection, and public-result wrapping.
Physical implementations live below them, organized by execution mechanism:

```text
_vector/
    operators.py
    reductions.py
    selection.py

    _python/
        operators.py
        reductions.py
        selection.py

    _numpy/
        operators.py
        reductions.py
        selection.py

    _arrow/
        operators.py
        reductions.py
        selection.py
```

Only create backend files for operation families that actually have a useful
physical implementation. Construction, mutation, dtype rules, and other
semantic-only modules do not need empty backend mirrors.

The call shape is:

```text
public Vector method
        -> semantic operation module
        -> optional Arrow or NumPy physical kernel
        -> mandatory pure-Python implementation
        -> canonical Serif result wrapping
```

`_python` is the semantic authority and guaranteed final path, not an equal
peer named `default`. Arrow and NumPy receive already-validated inputs and
return Serif storage, canonical Python scalars, or `DECLINED`; they never
construct `Vector` or `Table`. A small shared execution layer may own the
unique `DECLINED` identity and optional-library availability, but dispatch
policy remains explicit beside each semantic operation rather than hidden in
a registry or service-object framework.

Migrate one kernel family at a time: operators, reductions, mask/take, then
grouping, joining, and Arrow string kernels. If the Vector and Table physical
migrations cannot remain reviewable together, split this phase into sequential
Vector-execution and Table-execution pull requests rather than mixing either
one into PR 2 or PR 3.

### PR 5: Reorganize physical foundations and clean up

Finish any Table execution migration split out of PR 4, remove the superseded
`_accel` layout after all callers have moved, and enforce the final import
direction. Split storage implementations or make dtype a package only where a
concrete maintenance benefit remains. This cleanup is optional in scope: file
splits should not happen merely because they appeared in an early tree.

## PR 1 non-goals

PR 1 must not:

- change public semantics;
- add or change Table reduction behavior;
- change `Table(Vector)`;
- redesign backend selection or fallback;
- split storage or dtype modules;
- reorganize Table implementations;
- add Matrix, Tensor, or fingerprint APIs;
- change public names, exceptions, warnings, null behavior, or result types;
- edit unrelated documentation.

The existing pure and accelerated paths must continue to produce the same
Serif values, schemas, names, storage preservation, warnings, and errors.

## PR 1 ordered commit plan

Each item is a separate user-reviewed commit. Codex implements only one
approved item at a time, then stops for inspection and user-run verification.
The user owns staging, commits, branches, pushes, and pull requests.

### Commit 1: Record the algebra and refactor plan

- Add `docs/table-is-a-vector.md`.
- Add this plan.
- Make no runtime changes.

Status: completed.

### Commit 2: Isolate the existing accelerator entry points

- Move the current `_accel_*` call-through helpers out of `_vector/base.py`
  into one private accelerator-facing module.
- Update current callers, including Table callers.
- Preserve the exact availability checks, call order, decline behavior, and
  pure fallbacks.
- Do not introduce the future generic dispatcher.

Purpose: remove accidental ownership of Table and accelerator coordination
from the Vector class module before extracting semantic operations.

### Commit 3: Extract Vector reductions

- Move reduction semantics and their private helpers to
  `_vector/reductions.py`.
- Leave the public reduction methods on `Vector` as thin delegates.
- Preserve empty-reduction, null-skipping, scalar-type, and floating-point
  conformance behavior.
- Do not add Table reduction behavior.

### Commit 4: Extract Vector operators

- Move arithmetic, comparison, logical, bitwise, unary, reverse-operation,
  operand-validation, and result-schema logic to `_vector/operators.py`.
- Leave dunder methods and named bit-shift methods on `Vector` as thin
  delegates.
- Preserve Python scalar semantics, shape rules, Kleene logic, name derivation,
  promotion, and exception types.

### Commit 5: Extract Vector transforms and element API

- Move casts, null transforms, type tests, stable uniqueness, sorting, and
  related helpers to `_vector/transforms.py`.
- Move scalar-method proxy machinery and the explicit per-dtype element API to
  `_vector/element_api.py` where doing so does not create a circular import.
- Retain `numeric.py`, `string.py`, `dates.py`, and `categorical.py` as homes
  for actual dtype-specific behavior.
- Leave public methods on `Vector` as thin delegates.

### Commit 6: Extract Vector selection and mutation

- Move selector parsing, scalar/slice/mask/take reads, and selection planning
  to `_vector/selection.py`.
- Move assignment planning, copy-on-write rebuilding, mutability enforcement,
  and assignment application to `_vector/mutation.py`.
- Share selector normalization where appropriate without merging read and
  write semantics.
- Preserve the rule that all validation completes before observable mutation.

### Commit 7: Extract Vector construction

- Move collection and inference, target-class selection, storage selection,
  cloning, known-dtype construction, copying, and filled construction to
  `_vector/construction.py`.
- Keep dtype inference and promotion in the existing dtype module.
- Keep physical storage implementations in the existing storage module.
- Preserve subtype, categorical, nullable, name, and storage-selection rules.

### Commit 8: Move the concrete Vector class to `serif/vector.py`

- Move the now-thin concrete `Vector` definition from `_vector/base.py` to
  `serif/vector.py`.
- Update internal imports and public exports.
- Remove `_vector/base.py`; do not leave a second Vector definition behind.
- Preserve `from serif import Vector` and all documented behavior.
- Treat compatibility for private imports from `serif._vector.base` as out of
  scope unless an actual supported consumer is identified before this commit.

## PR 2 ordered commit plan

PR 2 decomposes Table lifting and row-aware structure without touching
transpose, joins, aggregation, windowing, or backend dispatch. Table remains a
Vector: Table modules coordinate the outer column structure and invoke the
ordinary Vector operations on each column; they do not implement a second
scalar algebra.

### Commit 1: Extract column traversal and naming coordination

- Add the `_table` package and `_table/columns.py`.
- Establish one explicit internal column traversal that never relies on public
  Table row iteration.
- Extract column lookup, sanitized/indexed attribute resolution, column maps,
  column metadata, `to_dict()`, rename/drop coordination, and column
  composition.
- Leave public Table methods as thin delegates and preserve warnings, frozen
  ownership, names, and lookup errors.

### Commit 2: Extract recursive lifting

- Add `_table/lifting.py`.
- Extract pointwise transforms, comparisons, arithmetic, reverse arithmetic,
  unary operations, logical/bitwise operations, and name coordination.
- The module may pair, broadcast, traverse, and rebuild columns, but actual
  scalar operations must run through Vector semantics.
- Preserve Table-to-Table width checks, left-biased naming, consolidated
  warnings, result types, and reverse-operation direction.
- Leave reductions on the inherited Vector path; do not create alternate Table
  reduction implementations.

### Commit 3: Extract row views

- Add `_table/row.py`.
- Move the `Row` view and Table row iteration coordination out of `table.py`.
- Preserve row reuse, indexed and attribute lookup, repr, shape, read-only
  behavior, and public Table iteration semantics.

### Commit 4: Extract Table selection

- Add `_table/selection.py`.
- Move string and multi-column lookup, row and cell reads, slices, masks,
  integer takes, and two-dimensional selection planning.
- Preserve exact selector precedence, exceptions, warnings, laziness, names,
  and result types.

### Commit 5: Extract Table mutation

- Add `_table/mutation.py`.
- Move column replacement, owner-addressed assignment planning and validation,
  write application, and the batch scope.
- Preserve validate-before-write atomicity, swap-on-write snapshots, frozen
  ownership, batch thaw/refreeze behavior, and partial writes on exceptions
  inside a batch.

### Commit 6: Extract row-aware transforms and composition

- Add `_table/rows.py`.
- Move row-wise `dropna()`, stable row-wise `unique()`, and row concatenation.
- Express each result as one shared row selection or permutation applied to
  every column.

### Commit 7: Extract Table sorting

- Add `_table/sort.py`.
- Move `sort_by()` planning and coordinated column permutation.
- Preserve stable ordering, multi-key and mixed-direction rules, null
  placement, categorical ordering, column subclasses, schemas, and storage
  preservation.

### Commit 8: Extract deferred-mask coordination

- Add `_table/deferred.py`.
- Move `MaskedTable`, snapshot ownership, lazy column gathering, mask
  composition, name freshness, and materialization coordination.
- Preserve laziness outside `batch()`, eager behavior inside `batch()`, and all
  snapshot/value-semantics guarantees.

## PR 3 ordered commit plan

PR 3 extracts the distinct Table algebra that cannot be derived by recursive
Vector lifting. It preserves the existing transpose, join, aggregation, and
window semantics and the existing accelerator call-throughs. It does not
redesign backend dispatch, add Table reduction behavior, or add new join,
aggregation, or window features.

### Commit 1: Extract Table transpose algebra

- Record this approved PR 3 commit plan and current handoff position.
- Add `_table/transpose.py` and move axis-transposition orchestration there.
- Traverse structural columns explicitly rather than relying on public Table
  row iteration.
- Leave `Table.T` as a thin delegate.
- Preserve value orientation, shape, current Vector construction and dtype
  inference, unnamed result columns, empty behavior, deferred-table
  materialization, and the plain-Table result type.
- Strengthen characterization coverage for transposed values and heterogeneous
  rows while retaining deferred/eager equivalence coverage.

### Commit 2: Extract Table joins

- Add `_table/joins.py`.
- Move join-key normalization and validation, runtime hashability checks, pure
  row probing, cardinality enforcement, padded column gathering, result-schema
  wrapping, and the shared join orchestrator.
- Leave `inner_join()`, `left_join()`, and `full_join()` on `Table` as thin
  delegates with their existing signatures and documentation.
- Preserve validation and raise order, exact diagnostics, row and match order,
  many-to-many fan-out, unmatched-row placement, and the current empty-result
  shape.
- Preserve identity-based right-key removal, nullable widening, names, column
  subclasses, storage backends, and computed or external join keys.
- Keep `_accel_join_probe`, `_accel_group`, and `_accel_take_pad` as unchanged
  implementation details; update only internal source comments that identify
  the pure semantic authority.

### Commit 3: Extract shared grouping machinery

- Add `_table/grouping.py` as the neutral dependency shared by aggregation and
  windowing.
- Move partition-index construction, first-appearance ordering, optional
  per-row keys, group slicing, output-name uniquification, aggregation-spec
  evaluation, scalar enforcement, and contextual empty-reduction errors.
- Preserve bound one-dimensional Vector methods, aggregate-only bound block
  fan-out, and callables that receive each group as a Table.
- Preserve length validation, exact errors, raw block-name prefixing,
  schema-aware slice reconstruction, and accelerator decline/fallback behavior.
- Route the still-inline `aggregate()` and `window()` orchestration through the
  shared functions so neither final public-operation module owns the other's
  common grouping semantics.

### Commit 4: Extract Table aggregation

- Add `_table/aggregation.py`.
- Move `aggregate()` orchestration, grouped-sum fast-path recognition, and
  group-key result wrapping.
- Leave `Table.aggregate()` as a thin delegate.
- Preserve the positional aggregation-dict overload, whole-table grouping,
  groupby-only results, first-appearance group order, block fan-out, flat-only
  result cells, and name uniquification across keys and outputs.
- Preserve group-key schemas and the existing Arrow grouped-sum call and pure
  fallback without redesigning dispatch.

### Commit 5: Extract Table windowing

- Add `_table/window.py`.
- Move `window()` orchestration and reuse `_table/grouping.py` for partitioning
  and aggregation evaluation.
- Leave `Table.window()` as a thin delegate.
- Preserve source row order and count, key-column cloning and subclasses,
  per-group broadcast, output-name uniquification, callable behavior,
  empty-table behavior, and the current rejection of block window
  aggregations.
- Remove the PR 3 imports that are no longer owned by `table.py`.

## Verification protocol

No source item is complete until the user has run the relevant tests and
reviewed the diff. Codex does not run terminal commands or Git operations.

After each implementation commit, the default verification request is:

```cmd
python -m pytest tests\ -q
```

During development, a smaller focused command may be requested first, but the
full suite must pass before the user commits the item. Warnings are
load-bearing; a warnings summary is a failure to investigate, even when pytest
otherwise reports success.

PR 1 is complete only when:

- the full local suite passes;
- CI passes in pure Python, NumPy-only, Arrow-only, and combined environments;
- `Vector` lives in `serif/vector.py`;
- `_vector/base.py` is gone;
- public Vector methods are thin delegates;
- no intended behavior or backend policy changed.

## Resume instructions

At the beginning of a later working session:

1. Read `AGENTS.md`.
2. Read `docs/table-is-a-vector.md`.
3. Read this file completely.
4. Ask the user which commit was last completed if the status below is stale.
5. Inspect only the next approved commit's scope.
6. Implement one approved commit item, then stop for user inspection, tests,
   and Git work.

Current position: PR 1 and PR 2 are complete, green, committed, and pushed.
PR 3's five-commit plan is approved. PR 3, Commit 1, "Extract Table transpose
algebra," has been implemented together with this durable plan update and is
awaiting user inspection, user-run verification, and commit. Do not begin PR 3,
Commit 2 until the user reports Commit 1 complete.
