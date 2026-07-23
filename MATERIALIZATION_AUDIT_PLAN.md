# Materialization Audit Implementation Plan

This is a temporary execution plan for removing avoidable tuple/list
materialization at Serif storage, NumPy, Arrow, CSV, and Parquet boundaries.
Delete it after the series lands.

## Series rules

- Preserve current public semantics, exception types/messages, null behavior,
  dtype promotion, ordering, and optional-dependency fallbacks.
- Keep the zero-dependency Python path first-class.
- Land one approved commit at a time. Stop after each commit for inspection,
  testing, and user-owned commit creation.
- Put behavior coverage in the same commit as the behavior it protects, unless
  a characterization-only commit is called out explicitly.
- Run the full test suite after every commit. Exercise NumPy-only,
  Arrow-only, NumPy+Arrow, and no-optional-dependency environments for PRs that
  touch dispatch.
- Treat row-count-sized Python collections as the target. Column-count-sized
  metadata lists and semantically necessary hash keys are out of scope.
- Do not combine unrelated naming, API, documentation, or style cleanup with
  this series.

## Proposed order

| PR | Theme | Depends on |
|---|---|---|
| 1 | Preserve NumPy indexers through joins, grouping, and string gather | — |
| 2 | Add direct storage and validity builders | — |
| 3 | Keep Parquet reads in canonical storage | PR 2 |
| 4 | Encode and write Parquet from storage buffers | PR 2 |
| 5 | Stream CSV into column-oriented construction | PR 2 |
| 6 | Make pure-Python vector kernels return storage | PR 2 |
| 7 | Keep Arrow grouped results in storage | PR 2 |
| 8 | Remove remaining full-column boxing in key algorithms and transforms | PRs 2, 6 |

---

## PR 1 — Preserve NumPy indexers end to end

Goal: stop converting NumPy-produced row indexers to Python lists only for the
selection layer to turn them back into NumPy arrays.

### Commit 1.1 — Return NumPy join indexers without `tolist()`

- Change both NumPy join probes to return `ndarray` indexers.
- Confirm typed columns pass those arrays directly through
  `take_pad_storage()`.
- Confirm object/untyped fallback selection can iterate NumPy integer scalars
  without changing results.
- Cover inner, left, and full joins; empty outputs; unmatched padding;
  duplicate diagnostics; dense and sorted probes; and Arrow string joins that
  delegate to the NumPy probes.

### Commit 1.2 — Retain NumPy group row-index arrays

- Keep each group’s row indices as an `ndarray`.
- Iterate `appearance` directly instead of calling `appearance.tolist()`.
- Continue boxing unique group keys under the current Python tuple-key
  partition contract.
- Cover first-appearance ordering, aggregation, windowing, typed slicing,
  object slicing, and Arrow string grouping.

### Commit 1.3 — Remove boundary boxing from NumPy string gather

- Stop calling `tolist()` on string start/end arrays.
- Replace the list of byte fragments with a single destination-buffer strategy
  for the large-span branch.
- Preserve duplicate indices, reverse order, empty strings, null padding,
  Unicode, and uint32 overflow decline behavior.

Definition of done: join/group indexers remain NumPy arrays until a genuinely
Python-only consumer iterates them, with identical public results in every
optional-dependency configuration.

---

## PR 2 — Add direct storage and validity builders

Goal: provide one internal way to append payload values and Arrow-layout
validity bits together, without row-count-sized staging lists.

### Commit 2.1 — Introduce and test an internal validity builder

- Add a private append-oriented validity builder that writes packed
  Arrow-layout bits directly.
- Allow lazy mask creation: no `BitMask` object when every lane is valid.
- Preserve empty, all-valid, all-null, partial final byte, negative indexing,
  slicing, and Arrow bitmap interoperability.
- Keep `BitMask.from_iterable()` as the compatibility entry point.

### Commit 2.2 — Build numeric storage directly

- Change `ArrayStorage.from_iterable()` to append to `array.array` directly.
- Build validity alongside the payload rather than retaining `data_list` and
  `null_flags`.
- Preserve type errors, int64 overflow, null sentinels, and constructor
  fallback to `TupleStorage` for oversized Python integers.

### Commit 2.3 — Build boolean and decimal validity directly

- Retain the existing direct `bytearray` payload construction.
- Replace their `null_flags` lists with the validity builder.
- Preserve boolean coercion rules, decimal scale/precision, rounding, and raw
  storage adapters.

### Commit 2.4 — Build string payload and offsets directly

- Append UTF-8 bytes and uint32 offsets to their final-form builders.
- Remove `buf_parts`, Python offset lists, and null-flag lists.
- Make buffer ownership explicit when freezing or wrapping the completed
  payload.
- Preserve null versus empty-string distinction, Unicode, uint32 overflow
  behavior, slicing, and Arrow buffer compatibility.

### Commit 2.5 — Migrate storage gather and mask concatenation

- Apply the validity builder to numeric, boolean, string, and decimal `take()`.
- Concatenate validity buffers without materializing one Python boolean per
  lane.
- Preserve negative/duplicate indices, empty gathers, null ordering, and
  homogeneous-storage validation.

Definition of done: known-dtype construction and storage gather do not retain
row-count-sized Python payload or null-flag lists before producing canonical
storage.

---

## PR 3 — Keep Parquet reads in canonical storage

Goal: prevent the pure Parquet reader from boxing packed data or repeatedly
copying completed column prefixes.

### Commit 3.1 — Decode nullable numeric pages to `ArrayStorage`

- Combine decoded `array.array` values with definition-level validity directly.
- Do not create `[None if ...]` page-result lists for int64/float64 pages.
- Preserve optional-column schema, all-null pages, mixed null positions,
  malformed definition-level errors, and int/float physical semantics.

### Commit 3.2 — Accumulate page storages and concatenate once

- Collect homogeneous page storages during a column-chunk decode.
- Perform one `concatenate_storages()` call after the page loop.
- Avoid repeatedly copying the full string, decimal, boolean, or nullable
  numeric prefix for every page.
- Preserve empty chunks and mixed reader fallback behavior.

### Commit 3.3 — Accumulate row-group chunks without repeated copies

- Use direct `array.extend()` for locally owned packed numeric arrays.
- Collect other homogeneous storage chunks and concatenate once per result
  column.
- Avoid `existing + chunk_values` and pairwise storage concatenation.

### Commit 3.4 — Narrow or remove mixed list fallbacks

- Prove which storage/list transitions are reachable for a fixed Parquet
  column kind.
- Replace reachable cases with an explicit canonical-storage conversion.
- Remove only branches proven unreachable by the reader’s type invariants.
- Preserve corruption and unsupported-encoding diagnostics.

Definition of done: supported Parquet physical types reach `Vector` as
canonical storage, and page/row-group assembly is linear in total payload size.

---

## PR 4 — Encode and write Parquet from storage buffers

Goal: stop decoding Serif storage to Python objects before writing it back to a
physical format.

### Commit 4.1 — Characterize writer output and failure semantics

- Lock round-trip behavior for every supported dtype and nullable variant.
- Cover empty tables, empty columns, Unicode, booleans, dates/timestamps,
  decimals, oversized/unsupported values, duplicate names, and write failures.
- Record whether a failing write must leave an existing destination unchanged;
  preserve that behavior through later commits.

### Commit 4.2 — Encode definition levels from `BitMask`

- Read validity directly from storage masks.
- Remove the full-column `null_flags` list.
- Preserve Parquet definition-level encoding and required/optional metadata.

### Commit 4.3 — Add storage-aware PLAIN encoders

- Encode `ArrayStorage` without `to_tuple()` or numeric boxing.
- Encode `StringStorage` from offsets and UTF-8 bytes without decode/re-encode.
- Pack `BoolStorage` bytes directly into Parquet boolean bits.
- Encode `DecimalStorage` from its fixed-width physical buffer.
- Keep object-backed date/timestamp handling on the necessary Python-object
  path.

### Commit 4.4 — Remove `to_tuple()`/`non_null` writer staging

- Dispatch each supported column to its storage-aware encoder.
- Retain a narrow generic fallback only where the canonical storage is
  intentionally object-backed.
- Preserve validation order and existing exception types/messages.

### Commit 4.5 — Stream validated pages instead of buffering the whole file

- Validate the table before modifying the destination.
- Write page bytes incrementally while retaining only footer metadata and
  offsets.
- If current failure semantics require destination preservation, write to a
  sibling temporary file and atomically replace only after successful footer
  completion.
- Clean up temporary files on failure without obscuring the original error.

Definition of done: fixed-width, string, boolean, and decimal columns are never
boxed during writing, and peak writer memory is bounded by a column page plus
footer metadata rather than the full table/file.

---

## PR 5 — Stream CSV into column-oriented construction

Goal: remove the full row matrix and overlapping per-column conversion lists
while preserving CSV inference semantics.

### Commit 5.1 — Characterize CSV inference and diagnostics

- Cover header/no-header input, header-only files, jagged short rows, long-row
  errors with physical row numbers, BOM handling, whitespace, Unicode,
  integer/float forms, leading-zero identifiers, and file-like inputs.
- Include a leading-zero identifier discovered late in a column.

### Commit 5.2 — Accumulate raw cells column-first

- Consume `csv.reader` once.
- Validate row width while distributing cells into per-column raw builders.
- Pad short rows as they are read.
- Remove `all_rows` and the later row-to-column transpose.

### Commit 5.3 — Classify each column without overlapping converted lists

- Determine each column’s final inference mode from its raw cells.
- Preserve the whole-column leading-zero identifier rule.
- Feed normalized values directly into known-dtype storage construction.
- Do not create `raw_cells`, `column_data`, and a replacement `column_data`
  simultaneously.

### Commit 5.4 — Construct the result table without constructor recopy

- Wrap completed column storage directly as Vectors.
- Build the Table through its no-copy column path where ownership permits.
- Preserve names, dtype/nullability, empty-column behavior, and public API.

Definition of done: CSV reading retains one raw representation per unresolved
column and one final storage representation, not the full row matrix plus
multiple converted column copies.

---

## PR 6 — Make pure-Python vector kernels return storage

Goal: make the zero-dependency execution path write known-result payloads
directly to canonical storage.

### Commit 6.1 — Return `BoolStorage` from comparison kernels

- Replace tuple results for vector/scalar comparisons with direct boolean
  payload and validity construction.
- Preserve unknown propagation, iterable RHS handling, null warnings, length
  errors, and comparison result schema.

### Commit 6.2 — Return `BoolStorage` from Kleene logical kernels

- Build logical vector/scalar and boolean inversion results directly.
- Preserve all Kleene truth-table combinations and nullable schema behavior.

### Commit 6.3 — Return known numeric storage from binary kernels

- When `_pre_compute_op_schema()` resolves the result kind, append directly to
  the appropriate storage builder.
- Preserve Python exception timing, division-by-zero behavior, int64 overflow
  degradation, reverse operators, and unsupported-type diagnostics.
- Keep materialized inference only for genuinely unknown/object result kinds.

### Commit 6.4 — Route known-result date, string, and element transforms through builders

- Migrate operations whose output schema is known before iteration.
- Avoid broad refactoring of operations that deliberately require result
  inference or arbitrary Python objects.
- Preserve names, wildcard/type-safe state, null behavior, and subclass
  selection.

Definition of done: known-schema pure-Python kernels return storage, while
unknown-schema operations retain their semantic inference path.

---

## PR 7 — Keep Arrow grouped results in storage

Goal: avoid `to_pylist()` followed by immediate Vector/storage reconstruction
for Arrow grouped aggregation output.

### Commit 7.1 — Return grouped key storage

- Convert grouped int64/string key arrays through the existing Arrow storage
  adapters.
- Pass source schema and storage through aggregation orchestration.
- Preserve first-appearance ordering, names, duplicate-name uniquification,
  and Python-facing scalar behavior.

### Commit 7.2 — Return float sum storage directly

- Normalize all-null group sums to Serif’s current zero result in Arrow.
- Convert the normalized Arrow array directly to `ArrayStorage`.
- Preserve nullable input behavior and output dtype.

### Commit 7.3 — Build integer sums adaptively

- Keep the Python reconstruction required for exact overflow-safe sums.
- Append results directly to int64 storage while values fit.
- Degrade to object/tuple storage when exact Python integers exceed int64,
  preserving existing promotion semantics.

Definition of done: Arrow grouped keys and float results avoid Python boxing;
integer results perform only the correctness-required Python arithmetic pass.

---

## PR 8 — Remove remaining full-column boxing

Goal: address lower-priority sites after the shared builders and primary I/O
paths have settled.

### Commit 8.1 — Stop boxing storage in Python grouping and joins

- Replace eager `storage.to_tuple()` snapshots with direct storage access where
  it does not alter key semantics.
- Retain per-row Python key tuples because hashing requires them.
- Preserve unhashable-key diagnostics, duplicate ordering, null keys,
  multi-key behavior, and join cardinality rules.

### Commit 8.2 — Remove sort’s unconditional `to_tuple()`

- Sort against storage directly, or add a storage-aware accelerated ordering
  path where justified.
- Benchmark the zero-dependency path before choosing direct indexing over
  cached Python objects.
- Preserve stable ordering, reverse flags, null behavior, and multi-column
  precedence.

### Commit 8.3 — Remove redundant categorical rebuild lists

- Replace `list(storage) -> Vector(...)` paths with storage-aware selection or
  known-dtype construction.
- Preserve category ordering, code stability, unknown categories, nulls,
  mutation semantics, and comparison behavior.

### Commit 8.4 — Clean up remaining known-dtype tuple producers

- Audit `dates.py`, `string.py`, `element_api.py`, `transforms.py`, and
  `Vector.filled()`.
- Migrate only result-sized tuple/list staging that can use the builders
  without changing inference.
- Leave display previews, column-count metadata, public conversion APIs, and
  semantically required row/hash tuples alone.

Definition of done: remaining `to_tuple()`, `list(storage)`, and tuple-producing
loops either represent a public/semantic boundary or have an explicit
justification.

---

## Explicit non-targets

- Arrow date/timestamp `to_pylist()` while those dtypes intentionally use
  Python-object `TupleStorage`.
- Python tuple group/join keys required for hashing and public diagnostics.
- Column-count-sized tuples/lists used to hold Table columns or operation
  specifications.
- Bounded display formatting collections.
- Public APIs whose purpose is to materialize Python values.
- Zero-copy Serif-to-Arrow buffer wrapping and the existing one-copy
  Arrow-to-canonical-Serif adapters.

## Final series acceptance

- Public behavior and diagnostics are unchanged except for separately approved
  bug fixes.
- No supported fast path performs a full `storage -> tuple/list -> NumPy/Arrow`
  round trip.
- No I/O path boxes fixed-width or raw string/decimal payloads without a
  semantic requirement.
- Multi-page and multi-row-group Parquet assembly is linear in total payload
  size.
- The no-optional-dependency path remains correct and does not regress
  materially on representative vector operations.
- The temporary plan file is deleted after the final PR lands.
