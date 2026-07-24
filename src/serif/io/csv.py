"""CSV reading utilities for Vector/Table."""

import csv
import re
import warnings
from typing import TextIO

from .._vector.dtype import Schema
from .._vector.dtype import promote_kinds
from .._vector.storage import ArrayStorage
from .._vector.storage import StringStorage
from .._vector.storage import TupleStorage
from ..errors import SerifValueError


def read_csv(file, *, delimiter=',', has_header=True, encoding='utf-8-sig'):
    """
    Read a CSV file and return a Table.

    Parameters
    ----------
    file : str or file-like
        Path to CSV file or file-like object
    delimiter : str, default ','
        Field delimiter
    has_header : bool, default True
        Whether first row contains column names
    encoding : str, default 'utf-8-sig'
        File encoding (used only if file is a path). The default reads
        plain UTF-8 unchanged and additionally strips a leading BOM so it
        can never become part of the first column name.

    Raises
    ------
    SerifValueError
        If a data row has more fields than the header — extra fields would
        otherwise be dropped silently.

    Returns
    -------
    Table
        Table with columns from the CSV file

    Examples
    --------
    >>> t = read_csv("data.csv")
    >>> t = read_csv("data.tsv", delimiter='\\t')
    >>> with open("data.csv") as f:
    ...     t = read_csv(f)
    """
    # Handle file path vs file object
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding, newline='') as f:
            return _read_csv_from_file(f, delimiter=delimiter, has_header=has_header)
    else:
        return _read_csv_from_file(file, delimiter=delimiter, has_header=has_header)


def _read_csv_from_file(file_obj: TextIO, *, delimiter: str, has_header: bool):
    """Read CSV data from an open file object."""
    from ..table import Table
    from ..vector import Vector

    reader = csv.reader(file_obj, delimiter=delimiter)

    first_row = next(reader, None)
    if first_row is None:
        return Table()

    # Determine the header from the first record, then accumulate every data
    # cell directly into its column's raw builder.
    if has_header:
        header = first_row
        raw_columns = [[] for _ in header]
        has_data = False
    else:
        # Generate default column names: col_0, col_1, etc.
        header = [f"col_{i}" for i in range(len(first_row))]
        raw_columns = [[cell] for cell in first_row]
        has_data = True

    num_cols = len(header)

    for row_num, row in enumerate(reader, start=2):
        row_width = len(row)
        if row_width > num_cols:
            raise SerifValueError(
                f"Row {row_num} has {row_width} fields, but the header has "
                f"{num_cols} columns."
            )

        for col_idx, cell in enumerate(row):
            raw_columns[col_idx].append(cell)
        for col_idx in range(row_width, num_cols):
            raw_columns[col_idx].append(None)
        has_data = True

    if not has_data:
        # Header only, no data
        # Preserve the existing dict-construction behavior for duplicate
        # header names while avoiding the constructor's column-shell copies.
        empty_columns = [Vector(name=col) for col in dict.fromkeys(header)]
        return Table._from_columns_nocopy(empty_columns)

    columns = []

    for col_idx, raw_cells in enumerate(raw_columns):
        dtype, identifier_mode, degradation = _classify_column(raw_cells)
        if degradation is not None:
            previous_kind, incompatible_kind = degradation
            warnings.warn(
                f"Degrading column<{previous_kind.__name__}> to column<object> "
                f"due to incompatible value of type {incompatible_kind.__name__}",
                stacklevel=2,
            )

        storage = _build_column_storage(raw_cells, dtype, identifier_mode)
        raw_columns[col_idx] = None
        del raw_cells
        column = Vector._from_storage(
            storage,
            dtype,
            name=header[col_idx],
        )
        column.vector_name = header[col_idx]
        columns.append(column)

    return Table._from_columns_nocopy(columns)


# A cell is numeric only if it LOOKS like a number: optional sign, ASCII
# digits, optional decimal point, optional exponent. Deliberately narrower
# than what int()/float() accept — Python-literal quirks that a CSV cell
# almost never means (underscores in "1_000", words like "nan"/"inf",
# non-ASCII digits) must stay strings.
_NUMERIC_RE = re.compile(r'^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$', re.ASCII)
_IDENTIFIER = object()


def _cell_inference_mode(value: str):
    """Return None, a scalar kind, or the leading-zero identifier sentinel."""
    if not value or value.strip() == '':
        return None

    value = value.strip()
    if not _NUMERIC_RE.match(value):
        return str

    digits = value.lstrip('+-')
    if digits.isdigit():
        if len(digits) > 1 and digits[0] == '0':
            return _IDENTIFIER
        return int

    return float


def _classify_column(raw_cells):
    """Resolve final dtype and identifier handling without retaining conversions."""
    kind = None
    nullable = False
    identifier_mode = False
    degradation = None

    for raw_cell in raw_cells:
        mode = None if raw_cell is None else _cell_inference_mode(raw_cell)
        if mode is None:
            nullable = True
            continue

        if mode is _IDENTIFIER:
            identifier_mode = True
            value_kind = str
        else:
            value_kind = mode

        if kind is None:
            kind = value_kind
        elif kind is not object and value_kind is not kind:
            promoted_kind = promote_kinds(kind, value_kind)
            if promoted_kind is None:
                degradation = (kind, value_kind)
                kind = object
            else:
                kind = promoted_kind

    if identifier_mode:
        kind = str
        degradation = None
    elif kind is None:
        kind = object
        nullable = True

    return Schema(kind, nullable), identifier_mode, degradation


def _normalized_cells(raw_cells, identifier_mode):
    """Yield final scalar values from one retained raw column."""
    for raw_cell in raw_cells:
        if raw_cell is None:
            yield None
        elif identifier_mode:
            value = raw_cell.strip()
            yield None if value == '' else value
        else:
            yield _infer_type(raw_cell)


def _build_column_storage(raw_cells, dtype, identifier_mode):
    """Build the selected final storage directly from normalized raw cells."""
    values = _normalized_cells(raw_cells, identifier_mode)

    if dtype.kind is int:
        try:
            return ArrayStorage.from_iterable(
                values,
                typecode='q',
                nullable=dtype.nullable,
            )
        except OverflowError:
            # Preserve exact Python integers outside int64 without retaining a
            # converted list solely to make the fallback re-iterable.
            return TupleStorage.from_iterable(
                _normalized_cells(raw_cells, identifier_mode),
                nullable=dtype.nullable,
            )
    if dtype.kind is float:
        return ArrayStorage.from_iterable(
            values,
            typecode='d',
            nullable=dtype.nullable,
        )
    if dtype.kind is str:
        return StringStorage.from_iterable(values)
    return TupleStorage.from_iterable(values, nullable=dtype.nullable)


def _infer_type(value: str):
    """
    Convert a CSV cell to int, float, None, or leave as string.

    - Empty / whitespace-only cells → None
    - Integer-looking cells → int, EXCEPT leading-zero forms ("0123"),
      which are identifiers — converting would destroy the zeros.
    - Decimal / exponent forms → float
    - Everything else (including "nan"/"inf", "True", dates) → string
    """
    mode = _cell_inference_mode(value)
    if mode is None:
        return None

    value = value.strip()
    if mode is str or mode is _IDENTIFIER:
        return value
    if mode is int:
        return int(value)
    return float(value)
