# Development Guide

## Setup

```bash
git clone https://github.com/CIG-GitHub/serif.git
cd serif
pip install -e ".[dev]"
```

Requires Python 3.10+. The only dev dependency is pytest; the base library
has zero dependencies. Install optional accelerators independently or
together when working on their conformance suites:

```bash
pip install -e ".[dev,numpy]"
pip install -e ".[dev,pyarrow]"
pip install -e ".[dev,numpy,pyarrow]"
```

The optional Rust extension (`serif._rust`) builds during install whenever a
Rust toolchain is on the PATH. `SERIF_RUST` chooses the behavior: `optional`
(the default) falls back to pure Python if the build fails, `required` fails
the install, and `off` skips the extension. Re-run the install after editing
anything under `rust/`, and run the kernels' own unit tests from there:

```bash
SERIF_RUST=required pip install -e ".[dev]"
SERIF_RUST=off pip install -e ".[dev]"
cd rust && cargo test
```

On Windows, close every Python process that has imported serif before
rebuilding. The build writes `_rust.*.pyd~` and renames it over the loaded
`.pyd`; while that file is in use the rename fails, the old extension stays
in place, and the `.pyd~` is left behind.

## Running Tests

```bash
python -m pytest tests/ -q          # full suite
python -m pytest tests/ -v          # verbose (what CI runs)
python -m pytest tests/test_joins.py -v   # one file
```

CI runs all four supported environments on Python 3.10: pure Python, NumPy
only, PyArrow only, and NumPy + PyArrow. Python 3.11–3.14 run with both
accelerators installed; conformance tests exercise their pure fallbacks
internally (`.github/workflows/tests.yml`). The 3.10 jobs pin the declared
minimum accelerator versions, and CI also builds and inspects the distribution
artifacts. Those jobs build with `SERIF_RUST=off`, so they keep testing what
they name. Rust jobs build with `SERIF_RUST=required`: Rust only on Python
3.10, and Rust + NumPy + PyArrow on Python 3.14 under Linux, Windows, and
macOS. Each job asserts that exactly its named backends are importable.

`.github/workflows/wheels.yml` builds every release artifact: compiled wheels
through cibuildwheel (configured under `[tool.cibuildwheel]` in
`pyproject.toml`), each tested against the full suite, plus the pure-Python
wheel and sdist, tested without the extension. It runs on version tags, on
manual dispatch, and on pull requests that touch `rust/` or the build
configuration.

### Warnings are load-bearing

Warnings in serif are deliberate signals, not noise. A green test run
must have an empty warnings summary: a test that intentionally exercises
warning-producing behavior wraps it in `pytest.warns(...)`, which both
silences it and pins the warning as required behavior. A warning
appearing in the summary means something to investigate.

## Project Structure

```
src/serif/
├── __init__.py        # public API exports
├── vector.py          # public Vector API and semantic dispatch
├── table.py           # public Table API and semantic dispatch
├── display.py         # repr logic, footer dtype grouping, _SchemaView (t._)
├── naming.py          # column-name sanitization and disambiguation
├── errors.py          # Serif* exception hierarchy
├── _execution.py      # shared execution-backend selection
├── _vector/
│   ├── construction.py, selection.py, mutation.py
│   ├── operators.py, reductions.py, transforms.py
│   ├── dtype.py, storage.py, nullable.py
│   ├── numeric.py, string.py, dates.py, categorical.py
│   ├── math.py, statistics.py
│   └── _python/, _numpy/, _arrow/, _rust/  # Vector execution backends
├── _table/
│   ├── columns.py, row.py, rows.py
│   ├── selection.py, mutation.py, sort.py
│   ├── grouping.py, aggregation.py, window.py, joins.py
│   └── _python/, _numpy/, _arrow/, _rust/  # Table execution backends
└── io/
    ├── csv.py         # read_csv
    ├── parquet.py     # serif-native read_parquet / write_parquet
    └── _arrow.py      # optional projected PyArrow reader

rust/                  # serif._rust kernels (int math, join probe, argsort)
setup.py               # SERIF_RUST build hook; metadata lives in pyproject.toml
tests/                 # pytest suite
docs/                  # documentation (start with design-philosophy.md)
```

The package uses a `src/` layout (`package-dir` in `pyproject.toml`), so
an editable install (`pip install -e .`) is the way to work on it — the
installed package then tracks your working tree.

## Key Modules

- **`vector.py`** — the public `Vector` class. Its methods delegate construction,
  selection, mutation, operators, reductions, and transforms to focused modules
  under `_vector/`. Typed subclasses (`_Int`, `_Float`, `_String`, `_Date`, and
  `_Category`) layer dtype-specific capabilities on top.
- **`table.py`** — the public `Table` class, a Vector of column Vectors. Table
  operations delegate to focused modules under `_table/`, including selection,
  mutation, sorting, grouping, joins, aggregation, and windows.
- **`errors.py`** — `SerifError` base plus `SerifKeyError`,
  `SerifValueError`, `SerifTypeError`, `SerifIndexError`, and the
  `SerifEmptyReductionWarning` warning. See docs/exceptions.md.
- **`naming.py`** — the sanitization pipeline. The exact rules live in
  docs/naming.md.
- **`display.py`** — everything repr: head/tail truncation,
  `set_repr_rows`, the grouped-dtype footer, and the `t._` schema view.

## Versioning

Release versions are declared in `pyproject.toml`, `src/serif/__init__.py`,
and `rust/Cargo.toml` (with the matching entry in `rust/Cargo.lock`), with
release notes in `CHANGELOG.md`. A release lands as its own pull request that
bumps all of these and dates the changelog section.

After it merges, tag the merge commit `vX.Y.Z` and push the tag. That runs
`wheels.yml`, which builds and tests every artifact and uploads them to the
run; it does not publish. Download the artifacts, check that every expected
platform is present, validate them with `python -m twine check`, and publish
them to PyPI with Twine.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines and the PR
process, and [docs/design-philosophy.md](design-philosophy.md) for the
principles a change must satisfy — features must earn their place.
