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
grouping, joining, and grouped aggregation. PR 4 combines the Vector and Table
execution migrations in one pull request, but each family remains a separate
user-reviewed commit. Shared Arrow string work moves with the operation family
that owns its semantics: comparison with Vector operators, bucketing with
Table grouping, and probing with Table joins.

### PR 5: Reorganize physical foundations and clean up

Enforce the final import direction and consider further physical-foundation
cleanup only where a concrete maintenance benefit remains. Split storage
implementations or make dtype a package only when actual ownership pressure
justifies it. File splits must not happen merely because they appeared in an
early tree.

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
- Remove stale resume-checklist references to repository files that do not
  exist.
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

## PR 4 ordered commit plan

PR 4 introduces one explicit execution contract and migrates Vector and Table
physical implementations one operation family at a time. It is structural:
existing public behavior remains authoritative, and any semantic change needs
separate proposal, tests, and approval. `serif/vector.py` and `serif/table.py`
retain class identity, state, invariants, metadata, API shape, and thin public
method shells.

Dependencies must flow as follows:

```text
public Vector/Table classes
        -> semantic operation modules
        -> explicit per-operation backend selection
        -> useful _python, _numpy, or _arrow physical modules
        -> Serif storage

Table semantic modules
        -> Vector semantic primitives for shared filter/take work
```

Semantic modules own validation, dtype and null rules, exceptions, dispatch
order, capability decisions, and public-result wrapping. Physical modules do
not import `Vector` or `Table`, do not construct public results, and return
Serif storage, canonical Python values, or the unique `DECLINED` sentinel.
`None` is never decline. Optional-backend defects raise; dispatch must not
catch broad exceptions and disguise them as unsupported cases. The `_python`
implementation is the semantic authority and guaranteed final path.

Each item below is a separate user-reviewed commit. Codex implements only one
approved item at a time, then stops for inspection and user-run verification.
The user owns staging, commits, branches, pushes, and pull requests.

### Commit 0: Record the combined PR 4 execution plan

- Correct the stale handoff: PR 3 is complete, green, committed, and pushed.
- Record this approved combined Vector-and-Table PR 4 plan.
- Make no runtime or test changes.

### Commit 1: Define the execution contract

Files:

- add `src/serif/_execution.py`;
- update `src/serif/_accel/__init__.py` and `_accel/arrow.py` during the
  transition;
- add `tests/test_execution_dispatch.py`.

Work:

- Define the unique `DECLINED` identity and centralize optional-library
  availability imports without introducing a dispatcher, registry, or service
  object.
- Temporarily re-export the same sentinel from `_accel` so there is one
  identity while families migrate.
- Preserve zero-dependency imports and the existing private backend switches
  used by conformance tests until their families move.
- Pin that `DECLINED is not None`, execution imports no public class, and
  unavailable libraries decline normally.

Focused verification:

```cmd
python -m pytest tests -q -k "execution_dispatch or accel or structural_refactor"
```

### Commit 2: Route Vector operators through deterministic backends

Files:

- update `_vector/operators.py`;
- add `_vector/_python/operators.py`;
- add `_vector/_numpy/operators.py` and `_vector/_numpy/storage.py`;
- add `_vector/_arrow/operators.py` and `_vector/_arrow/storage.py`;
- add only the required backend-package `__init__.py` files;
- update `_accel/api.py`, `_accel/__init__.py`, and `_accel/arrow.py`;
- remove `_accel/ops.py`;
- update `tests/test_execution_dispatch.py`, `test_accel_arrow.py`,
  `test_accel_arrow_arith.py`, `test_accel_arrow_div.py`,
  `test_accel_logical.py`, `test_accel_ops.py`, and
  `test_accel_string_compare.py`.

Work:

- Keep operand normalization, shape validation, schema promotion,
  nullability, exception translation, warning emission, dispatch, and result
  wrapping in `_vector/operators.py`.
- Move canonical loops and storage-level unary work to `_python/operators.py`.
- Move NumPy arithmetic, comparison, Kleene logic, and inversion to
  `_numpy/operators.py`.
- Move checked Arrow integer arithmetic, checked true division, and string
  comparison to `_arrow/operators.py`.
- Optional kernels return Serif storage or `DECLINED`, never `Vector`, NumPy
  arrays/scalars, or Arrow arrays/scalars.
- Dispatch deterministically: Arrow then NumPy then Python for supported true
  division; NumPy then Arrow then Python for supported add/subtract/multiply;
  NumPy then Python for floor-division/modulo; NumPy then Arrow then Python for
  comparisons; NumPy then Python for Kleene logic and inversion. Do not expand
  the currently accelerated reverse, bitwise, power, or unary surface.
- Perform length and invalid-operation validation before acceleration.
  Executing zero divisors raise before a backend call; zero under a null lane
  remains non-executing. Physical integer overflow may decline so Python can
  preserve bigint semantics.

Invariants:

- Preserve exact promotion, nullable schemas, names, wild-name tracking,
  storage normalization, Python scalar types, exception text and order,
  Kleene truth tables, string-subclass behavior, and Table lifting.
- Preserve the null-comparison warning text and stack level and all Table
  naming-warning behavior.

Focused verification:

```cmd
python -m pytest tests\test_execution_dispatch.py tests\test_accel_arrow.py tests\test_accel_arrow_arith.py tests\test_accel_arrow_div.py tests\test_accel_logical.py tests\test_accel_ops.py tests\test_accel_string_compare.py tests\test_arithmetic_edges.py tests\test_null_semantics.py tests\test_table_arithmetic_naming.py tests\test_type_promotion.py -q
```

### Commit 3: Route Vector reductions through deterministic backends

Files:

- update `_vector/reductions.py`;
- add `_vector/_python/reductions.py` and `_vector/_numpy/reductions.py`;
- update `_accel/api.py` and `_accel/__init__.py`;
- remove `_accel/reduce.py`;
- update `tests/test_execution_dispatch.py` and `test_accel_reduce.py`.

Work:

- Keep rank lifting, empty-case rules, `on_empty` validation, dispatch, and
  scalar-result ownership in `_vector/reductions.py`.
- Move canonical reductions to `_python/reductions.py` and existing buffer
  reductions plus the integer-residue proof to `_numpy/reductions.py`.
- Dispatch NumPy then Python for max, min, sum, mean, and stdev. Keep first,
  last, all, any, and count on Python only.
- Replace the `(ok, value)` adapter with direct identity comparison against
  `DECLINED`. A returned `None` is a successful reduction result.

Invariants:

- Preserve empty/all-null results, the exact integer zero sum identity,
  arbitrary-precision integer sums, float `math.fsum` authority, NaN min/max
  ordering behavior, stdev conventions, Python scalar types, and all/no-verdict
  errors.
- Rank-two reductions still lift through columns before scalar dispatch.

Focused verification:

```cmd
python -m pytest tests\test_execution_dispatch.py tests\test_accel_reduce.py tests\test_null_semantics.py tests\test_pyvector_math.py tests\test_table_vector_surface.py -q
```

### Commit 4: Route Vector selection and gathers through deterministic backends

Files:

- update `_vector/selection.py` and `_vector/transforms.py`;
- add `_vector/_python/selection.py` and `_vector/_numpy/selection.py`;
- update `_table/rows.py`, `_table/sort.py`, `_table/grouping.py`,
  `_table/joins.py`, and `_table/deferred.py`;
- update `io/parquet.py`;
- update `_accel/api.py` and `_accel/__init__.py`;
- remove `_accel/mask.py`;
- update `tests/test_execution_dispatch.py`, `test_accel_mask.py`,
  `test_accel_take.py`, `test_accel_group.py`, `test_deferred_mask.py`,
  `test_pyvector_indexing.py`, and `test_structural_refactor.py`.

Work:

- Keep selector recognition, type/length validation, nullable-mask rules,
  warnings, dispatch, and public wrapping in `_vector/selection.py`.
- Move pure filter/take/padded-take/popcount work to `_python/selection.py` and
  the existing NumPy gather implementation to `_numpy/selection.py`.
- Route Table sorting, row uniqueness, group slicing, join padding, deferred
  popcount, and Parquet filtering through narrow Vector semantic primitives.
- Dispatch filter NumPy then Python, take NumPy then `storage.take()`, and
  popcount NumPy then Python. Preserve the current caller-owned pure wrapping
  for padded joins.
- Keep slice and warned integer-subscript reads on their existing Python paths.

Invariants:

- Nullable mask nulls exclude rows; all validation precedes gathering.
- Preserve names, schemas, storage types, subclasses, stable Table sort,
  categorical behavior, deferred snapshots, join padding/widening, Parquet
  filtering, and the exact two large positional-index warning messages.

Focused verification:

```cmd
python -m pytest tests\test_execution_dispatch.py tests\test_accel_mask.py tests\test_accel_take.py tests\test_accel_group.py tests\test_deferred_mask.py tests\test_pyvector_indexing.py tests\test_table_sort.py tests\test_structural_refactor.py -q
```

```cmd
python -m pytest tests\test_accel_arrow_grouped_sum.py tests\test_accel_string_group.py tests\test_accel_string_join.py tests\test_accel_unique_join.py tests\test_aggregate_window.py tests\test_joins.py tests\test_parquet.py tests\test_parquet_deferred.py -q
```

### Commit 5: Route Table grouping through deterministic backends

Files:

- update `_table/grouping.py`;
- add `_table/_python/grouping.py`, `_table/_numpy/grouping.py`, and
  `_table/_arrow/grouping.py`, plus only their required package files;
- update `_accel/api.py` and `_accel/arrow.py`;
- remove `_accel/group.py`;
- update `tests/test_execution_dispatch.py`, `test_accel_group.py`,
  `test_accel_string_group.py`, `test_accel_take.py`,
  `test_aggregate_blocks.py`, `test_aggregate_ordered_pick.py`,
  `test_aggregate_sort_order.py`, and `test_aggregate_window.py`.

Work:

- Keep group-key resolution, length validation, first-appearance ordering,
  row-key requirements, aggregation slicing, scalar enforcement, empty-error
  context, and dispatch in `_table/grouping.py`.
- Move canonical bucketing to `_python/grouping.py`, dense int64 bucketing to
  `_numpy/grouping.py`, and Arrow string encoding/bucketing to
  `_arrow/grouping.py`.
- Dispatch supported single keys deterministically through the useful numeric
  or string backend and then Python. Multi-key, nullable, float, object,
  categorical, and window row-key cases retain the canonical Python path where
  required by current semantics.
- Normalize backend keys and row-index buckets to Python values before they
  return to the semantic module; no NumPy arrays or Arrow objects cross the
  boundary.

Invariants:

- Preserve first-appearance group order, ascending row order within groups,
  None/NaN key behavior, hashability failures, slice schemas/subclasses,
  bound-method and callable behavior, block fan-out, exact errors, and
  aggregate/window warning behavior.

Focused verification:

```cmd
python -m pytest tests\test_execution_dispatch.py tests\test_accel_group.py tests\test_accel_string_group.py tests\test_accel_take.py tests\test_aggregate_blocks.py tests\test_aggregate_ordered_pick.py tests\test_aggregate_sort_order.py tests\test_aggregate_window.py -q
```

### Commit 6: Route Table joins through deterministic backends

Files:

- update `_table/joins.py`;
- add `_table/_python/joins.py`, `_table/_numpy/joins.py`, and
  `_table/_arrow/joins.py`;
- update `_accel/api.py` and `_accel/arrow.py`;
- remove `_accel/join.py`;
- update `tests/test_execution_dispatch.py`, `test_accel_group.py`,
  `test_accel_string_group.py`, `test_accel_string_join.py`,
  `test_accel_unique_join.py`, `test_joins.py`, `test_semantic_fixes.py`, and
  `test_structural_refactor.py`.

Work:

- Keep key normalization, dtype/length/hashability validation, cardinality
  diagnostics, dispatch, padded gathering, nullable widening, right-key
  removal, schema/name ownership, and result wrapping in `_table/joins.py`.
- Move the canonical right-index/probe program to `_python/joins.py`, dense and
  sorted int64 probes to `_numpy/joins.py`, and shared-dictionary string probes
  to `_arrow/joins.py`.
- Preserve the effective cascade: dense int probe, string hash probe, sorted
  int probe, sorted string probe, then Python, with inapplicable capabilities
  returning `DECLINED`.
- Cardinality violations return a diagnostic outcome for the semantic layer to
  raise; they are not decline. Unexpected backend exceptions propagate.
- Normalize take indices and duplicate keys to Python values before returning
  from physical modules.

Invariants:

- Preserve validation and raise order, exact diagnostics, match and row order,
  many-to-many fan-out, unmatched placement, empty-result shape,
  identity-based right-key removal, nullable widening, names, schemas,
  subclasses, storage, and computed/external keys.

Focused verification:

```cmd
python -m pytest tests\test_execution_dispatch.py tests\test_accel_group.py tests\test_accel_string_group.py tests\test_accel_string_join.py tests\test_accel_unique_join.py tests\test_joins.py tests\test_semantic_fixes.py tests\test_structural_refactor.py -q
```

### Commit 7: Route grouped aggregation through Arrow and retire `_accel`

Files:

- update `_table/aggregation.py`;
- add `_table/_arrow/aggregation.py`;
- update `tests/test_execution_dispatch.py`,
  `test_accel_arrow_grouped_sum.py`, `test_aggregate_blocks.py`,
  `test_aggregate_ordered_pick.py`, `test_aggregate_sort_order.py`,
  `test_aggregate_window.py`, and `test_structural_refactor.py`;
- remove the remaining `_accel/api.py`, `_accel/arrow.py`,
  `_accel/__init__.py`, and the superseded `_accel` package after verifying
  that no caller remains.

Work:

- Keep grouped-sum recognition, source/key validation, deterministic Arrow
  selection, name uniquification, key-schema preservation, result wrapping,
  and ordinary grouped fallback in `_table/aggregation.py`.
- Move only the useful Arrow grouped-sum physical implementation to
  `_table/_arrow/aggregation.py`; do not create Python or NumPy aggregation
  mirrors because existing grouping plus Vector reductions are the canonical
  path.
- Return Python keys and value columns or `DECLINED`, never Arrow objects.
- Preserve exact integer residue reconstruction and decline the whole physical
  operation when any group cannot be proven exact.
- Remove all legacy `_accel` imports and assert the final dependency direction.

Invariants:

- Preserve recognized-bound-sum scope, first-appearance group order, all-null
  sum identity, integer exactness, accepted float reduction-order tolerance,
  positional overloads, block/callable behavior, flat-only results, schemas,
  names, and warnings.

Focused verification:

```cmd
python -m pytest tests\test_execution_dispatch.py tests\test_accel_arrow_grouped_sum.py tests\test_aggregate_blocks.py tests\test_aggregate_ordered_pick.py tests\test_aggregate_sort_order.py tests\test_aggregate_window.py tests\test_structural_refactor.py -q
```

## PR 5 ordered commit plan

PR 5 enforces the final physical dependency direction and addresses only the
ownership problems demonstrated by the current implementation. It adds no
public semantics, public APIs, backend mirrors, registries, or service objects.
Existing class identity, state, storage, schemas, names, metadata, warnings,
exceptions, and result types remain authoritative.

Each item is a separate user-reviewed commit. Codex implements only one
approved item at a time, then stops for inspection, user-run verification, and
Git work.

### Commit 0: Record the PR 5 physical-foundations plan

Files:

- update this document only.

Work:

- Record the approved PR 5 sequence, boundaries, invariants, and focused
  verification commands.
- Correct the stale handoff that still described PR 4 as pending.
- Make no runtime changes and do not edit `docs/table-is-a-vector.md`.

Focused verification:

```cmd
python -m pytest tests\test_structural_refactor.py -q
```

### Commit 1: Apply the `DECLINED` contract to Arrow I/O

Files:

- update `src/serif/io/_arrow.py` and `src/serif/io/parquet.py`;
- update `tests/test_parquet_arrow.py`.

Work:

- Return the unique `DECLINED` sentinel when an Arrow I/O physical read is
  unsupported or an expected Arrow parse/conversion failure requires the pure
  reader.
- Compare decline by identity in whole-file and projected-column dispatch;
  `None` remains legitimate cell data and is never dispatch control flow.
- Preserve cheap whole-file decline, projected-column fallback, and the pure
  reader's user-facing refusals.
- Narrow accelerator exception handling so expected Arrow failures decline but
  unexpected Serif or backend defects propagate.

Invariants:

- Preserve public Parquet values, schemas, names, decimal metadata, null slots,
  concrete Python scalar types, lazy materialization, and mask pushdown.
- Preserve unsupported-type rejection and Serif errors reached through ordinary
  fallback. Add no warnings and change no public result type.

Focused verification:

```cmd
python -m pytest tests\test_parquet_arrow.py tests\test_parquet_deferred.py tests\test_execution_dispatch.py -q
```

### Commit 2: Move kind promotion under dtype ownership

Files:

- update `src/serif/_vector/dtype.py`, `src/serif/_vector/operators.py`, and
  `src/serif/_vector/numeric.py`.

Work:

- Move the existing kind-promotion table into `dtype.py` behind a small
  dtype-owned function.
- Remove the semantic operator module's dependency on the concrete numeric
  subclass module.
- Leave `_Float`, `_Int`, their identities, and storage construction untouched.

Invariants:

- Preserve the exact promotion matrix, division and reverse-operation rules,
  power behavior, string addition, nullable propagation, result schemas,
  inference, scalar validation, warnings, and exceptions.

Focused verification:

```cmd
python -m pytest tests\test_type_promotion.py tests\test_inference_order.py tests\test_arithmetic_edges.py tests\test_accel_ops.py tests\test_accel_arrow_arith.py tests\test_execution_dispatch.py -q
```

### Commit 3: Centralize physical storage concatenation

Files:

- update `src/serif/_vector/storage.py` and `src/serif/io/parquet.py`;
- update `tests/test_storage_protocol.py` and `tests/test_parquet_foreign.py`.

Work:

- Add one storage-owned concatenation function for `ArrayStorage`,
  `BoolStorage`, `StringStorage`, `DecimalStorage`, and `TupleStorage`.
- Move raw-buffer, offset, and null-mask combination out of Parquet and remove
  its duplicate storage-concatenation mechanics.
- Keep row-group orchestration, dtype selection, heterogeneous fallback,
  naming, and public `Vector` wrapping in Parquet I/O.

Invariants:

- Preserve storage classes, array typecodes, value order, string offsets, raw
  bytes, Arrow-compatible null bitmaps, decimal scale/precision, and input
  immutability.
- Preserve the all-valid `mask is None` representation, heterogeneous iterable
  fallback, lazy Parquet state, and mask pushdown.

Focused verification:

```cmd
python -m pytest tests\test_storage_protocol.py tests\test_bool_storage.py tests\test_nullable.py tests\test_parquet.py tests\test_parquet_deferred.py tests\test_parquet_foreign.py -q
```

### Commit 4: Share Arrow-to-Serif storage reconstruction

Files:

- update `src/serif/_vector/_arrow/storage.py` and `src/serif/io/_arrow.py`;
- update `tests/test_accel_arrow.py` and `tests/test_parquet_arrow.py`.

Work:

- Extend the physical Arrow storage adapter to reconstruct string and decimal
  storage alongside its existing numeric, boolean, and bitmap conversions.
- Make Parquet I/O reuse those conversions and remove its duplicate buffer
  copying, boolean unpacking, string-offset reconstruction, and decimal
  byte-order conversion.
- Keep supported-type policy, schema decisions, Arrow casting, date/datetime
  treatment, file fallback, and public-result wrapping in I/O.
- Load optional NumPy through the shared execution layer rather than through the
  NumPy backend package.

Invariants:

- Physical functions return Serif storage or `DECLINED`, never `Vector`,
  `Table`, Arrow results, or NumPy results.
- Preserve empty-array storage classes, decimal metadata and byte order, null
  masks, NumPy-disabled behavior, and canonical Python output scalars.

Focused verification:

```cmd
python -m pytest tests\test_accel_arrow.py tests\test_accel_arrow_arith.py tests\test_accel_arrow_div.py tests\test_accel_logical.py tests\test_parquet_arrow.py tests\test_bool_storage.py tests\test_nullable.py tests\test_execution_dispatch.py -q
```

### Commit 5: Lock the physical dependency graph

Files:

- update `tests/test_structural_refactor.py`.

Work:

- Add AST-based assertions for the final import direction.
- Ensure execution, dtype, nullable, storage, and physical backend modules do
  not import public `Vector` or `Table`, and physical modules do not import
  semantic operation or I/O modules.
- Preserve the canonical-class and retired-`_accel` assertions.
- Explicitly allow the existing, conformance-tested Arrow-plus-NumPy physical
  composition for dictionary grouping and join probing.

Focused verification:

```cmd
python -m pytest tests\test_structural_refactor.py tests\test_execution_dispatch.py -q
```

The intended dependency direction after PR 5 is:

```text
public Vector / Table
        -> semantic Vector / Table modules
        -> explicit backend selection
        -> useful _python / _numpy / _arrow physical code
        -> dtype / storage / nullable foundations
```

Table semantic code may depend on Vector semantic code. I/O may depend on
public classes and lower-level physical storage adapters. The existing hybrid
Arrow-plus-NumPy grouping and join implementations remain deliberate physical
composition; no new hybrid backend package is introduced.

PR 5 deliberately does not turn `dtype.py` into a package, split `storage.py`
into a package, move categorical storage, revisit remaining public Vector
matrix/compose methods, change Table algebra, or redesign the hybrid grouping
and join kernels. The demonstrated ownership problems can be fixed with less
churn while preserving existing private storage import paths.

After every runtime commit, the full-suite gate remains:

```cmd
python -m pytest tests\ -q
```

Any unexpected warnings summary is a failure to investigate. PR completion
also requires the existing pure-Python, NumPy-only, Arrow-only, and combined CI
environments.

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

PR 4 is complete only when:

- every focused command and the full local suite pass without an unexpected
  warnings summary;
- CI passes in pure Python, NumPy-only, Arrow-only, and combined environments;
- backend choice and decline behavior are deterministic and directly tested;
- `None` is never used as decline;
- physical modules return only Serif storage, canonical Python values, or
  `DECLINED`, and import neither `Vector` nor `Table`;
- public result construction and all semantic validation remain in semantic
  modules;
- pure Python remains the guaranteed final path;
- the superseded `_accel` package is gone and no empty backend mirrors were
  introduced;
- no public semantics, warnings, exceptions, construction, mutation, dtype
  rules, or unrelated Table algebra changed.

## Resume instructions

At the beginning of a later working session:

1. Read `docs/table-is-a-vector.md`.
2. Read this file completely.
3. Ask the user which commit was last completed if the status below is stale.
4. Inspect only the next approved commit's scope.
5. Implement one approved commit item, then stop for user inspection, tests,
   and Git work.

Current position: PR 1, PR 2, PR 3, and PR 4 are complete, green, committed,
pushed, and merged. The six-item PR 5 plan (documentation-only Commit 0
followed by implementation Commits 1 through 5) is approved, including applying
the unique `DECLINED` contract to Arrow I/O. PR 5, Commit 0, "Record the PR 5
physical-foundations plan," has been implemented and is awaiting user
inspection and commit. Do not begin PR 5, Commit 1 until the user reports
Commit 0 complete and explicitly says to proceed.
