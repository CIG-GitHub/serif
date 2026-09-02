# Engineering polish PR plan

I still agree with the feedback, with two qualifications: the existing test
suite and CI matrix are substantial, and benchmarks should inform reviews
rather than make ordinary PR CI timing-sensitive.

# PR 1: Repair development-guide drift

- Commit 1: Replace the removed monolithic module map with the current
  `_vector`, `_table`, storage, dispatch, and backend layout; correct the
  accelerator extra from `arrow` to `pyarrow`.
- Commit 2: Stop hard-coding version `0.1.6` and remove the nonexistent
  publishing-workflow claim; describe only the current version source and
  test/package workflow.

# PR 2: Add property-based semantic tests

- Commit 1: Add Hypothesis to the development extra and define reusable
  strategies for supported values, null masks, indices, and small tables.
- Commit 2: Add properties for round trips, selection, mutation isolation,
  schemas, and null semantics through the public API.
- Commit 3: Differentially test eligible NumPy and Arrow operations against
  the pure-Python reference behavior.

# PR 3: Enforce coverage non-regression

- Commit 1: Add branch-coverage configuration and a reproducible local report.
- Commit 2: Record the measured baseline and make CI fail below that floor;
  ratchet it upward as uncovered behavior receives tests.

# PR 4: Enforce linting

- Commit 1: Add a narrowly configured Ruff check and fix the initial findings
  without unrelated formatting churn.
- Commit 2: Run Ruff in CI and document the matching local command.

# PR 5: Introduce incremental type checking

- Commit 1: Type the stable execution, schema, storage, and dispatch contracts
  without changing runtime behavior.
- Commit 2: Configure mypy to check function bodies package-wide and apply
  stricter rules only to the typed modules, avoiding blanket ignores.
- Commit 3: Add the scoped mypy check to CI.

# PR 6: Add a benchmark harness

- Commit 1: Add repeatable benchmarks for construction, arithmetic, filtering,
  strings, joins, and grouping across applicable backends and data sizes.
- Commit 2: Add a manual benchmark workflow that preserves machine-readable
  results for comparison, with no wall-clock gate in normal PR CI.
