"""CSV reading utilities for Vector/Table."""

import csv
import re
from typing import TextIO

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
        return Table({col: Vector() for col in header})

    columns = []

    for col_idx, raw_cells in enumerate(raw_columns):
        column_data = [None if v is None else _infer_type(v) for v in raw_cells]

        # Identifier-column rule: _infer_type keeps a numeric-LOOKING string
        # only for leading-zero forms ("0123"). One such cell marks the whole
        # column as identifiers — numeric cells revert to their raw strings,
        # so '90210' doesn't become an int alongside '01234'.
        if any(isinstance(v, str) and _NUMERIC_RE.match(v) for v in column_data):
            column_data = [
                None if v is None or v.strip() == '' else v.strip()
                for v in raw_cells
            ]

        columns.append(Vector(column_data, name=header[col_idx]))

    return Table(columns)


# A cell is numeric only if it LOOKS like a number: optional sign, ASCII
# digits, optional decimal point, optional exponent. Deliberately narrower
# than what int()/float() accept — Python-literal quirks that a CSV cell
# almost never means (underscores in "1_000", words like "nan"/"inf",
# non-ASCII digits) must stay strings.
_NUMERIC_RE = re.compile(r'^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$', re.ASCII)


def _infer_type(value: str):
    """
    Convert a CSV cell to int, float, None, or leave as string.

    - Empty / whitespace-only cells → None
    - Integer-looking cells → int, EXCEPT leading-zero forms ("0123"),
      which are identifiers — converting would destroy the zeros.
    - Decimal / exponent forms → float
    - Everything else (including "nan"/"inf", "True", dates) → string
    """
    if not value or value.strip() == '':
        return None

    value = value.strip()

    if not _NUMERIC_RE.match(value):
        return value

    digits = value.lstrip('+-')
    if digits.isdigit():
        # Pure integer form. Leading zeros mean identifier, not number.
        if len(digits) > 1 and digits[0] == '0':
            return value
        return int(value)

    return float(value)
