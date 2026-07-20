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

### PR 4: Introduce execution dispatch

Implement the backend contract above and migrate kernel families one at a
time: reductions, operators, mask/take, grouping and joining, then Arrow string
kernels.

### PR 5: Reorganize physical foundations and clean up

Split storage implementations where useful, decide whether dtype rules merit a
package, remove temporary compatibility paths, and enforce the final import
direction. This PR is optional in scope: file splits that no longer provide a
clear benefit should not happen merely because they appeared in an early tree.

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

Status: drafted; awaiting user inspection and commit.

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

Current position: PR 1, Commit 1 drafted. No runtime refactor has started.
