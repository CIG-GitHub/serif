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

    # Read all rows first
    all_rows = list(reader)

    if not all_rows:
        return Table()

    # Determine header and data rows
    if has_header:
        header = all_rows[0]
        rows = all_rows[1:]
        first_data_row = 2  # 1-based physical line of the first data row
    else:
        # Generate default column names: col_0, col_1, etc.
        header = [f"col_{i}" for i in range(len(all_rows[0]))]
        rows = all_rows
        first_data_row = 1

    num_cols = len(header)

    # Rows longer than the header would silently lose their extra fields in
    # the transpose below — refuse loudly instead. (Shorter rows are padded
    # with None: additive, not destructive.)
    for row_num, row in enumerate(rows, start=first_data_row):
        if len(row) > num_cols:
            raise SerifValueError(
                f"Row {row_num} has {len(row)} fields, but the header has "
                f"{num_cols} columns."
            )

    if not rows:
        # Header only, no data
        return Table({col: Vector() for col in header})

    # Transpose rows into columns
    columns = []

    for col_idx in range(num_cols):
        # Handle jagged (short) rows: missing cells read as None
        raw_cells = [(row[col_idx] if col_idx < len(row) else None) for row in rows]
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
