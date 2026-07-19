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
pip install -e ".[dev,arrow]"
pip install -e ".[dev,numpy,arrow]"
```

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
artifacts.

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
├── table.py           # Table: construction, indexing, joins, aggregate/window
├── display.py         # repr logic, footer dtype grouping, _SchemaView (t._)
├── naming.py          # column-name sanitization and disambiguation
├── errors.py          # Serif* exception hierarchy
├── _accel/            # optional NumPy/PyArrow compute accelerators
├── _vector/
│   ├── base.py        # core Vector: operators, masks, aggregations, fingerprints
│   ├── dtype.py       # dtype inference and validation
│   ├── storage.py     # ArrayStorage / TupleStorage / StringStorage
│   ├── numeric.py     # _Int, _Float typed subclasses
│   ├── string.py      # _String
│   ├── dates.py       # _Date
│   ├── categorical.py # _Category
│   └── nullable.py    # null-mask storage support
└── io/
    ├── csv.py         # read_csv
    ├── parquet.py     # serif-native read_parquet / write_parquet
    └── _arrow.py      # optional projected PyArrow reader

tests/                 # pytest suite
docs/                  # documentation (start with design-philosophy.md)
```

The package uses a `src/` layout (`package-dir` in `pyproject.toml`), so
an editable install (`pip install -e .`) is the way to work on it — the
installed package then tracks your working tree.

## Key Modules

- **`_vector/base.py`** — the `Vector` class: elementwise operators,
  boolean masks, aggregations, copy-on-write mutation, fingerprinting.
  Typed subclasses (`_Int`, `_Float`, `_String`, `_Date`, `_Category`)
  layer dtype-specific methods on top.
- **`table.py`** — `Table` (a vector of column vectors): construction,
  single- and two-axis indexing, joins, `aggregate()`/`window()`, and
  the column map behind dot access.
- **`errors.py`** — `SerifError` base plus `SerifKeyError`,
  `SerifValueError`, `SerifTypeError`, `SerifIndexError`, and
  `SerifEmptyReductionError`. See docs/exceptions.md.
- **`naming.py`** — the sanitization pipeline. The exact rules live in
  docs/naming.md.
- **`display.py`** — everything repr: head/tail truncation,
  `set_repr_rows`, the grouped-dtype footer, and the `t._` schema view.

## Versioning

Versioned releases (currently `0.1.6` in `pyproject.toml`) with a
changelog at `CHANGELOG.md`. Publishing runs through
`.github/workflows/publish.yml`.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines and the PR
process, and [docs/design-philosophy.md](design-philosophy.md) for the
principles a change must satisfy — features must earn their place.
