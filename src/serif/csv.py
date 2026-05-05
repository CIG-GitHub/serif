"""CSV reading utilities for Vector/Table."""

import csv
from typing import TextIO


def read_csv(file, *, delimiter=',', has_header=True, encoding='utf-8'):
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
    encoding : str, default 'utf-8'
        File encoding (used only if file is a path)
    
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
    from .table import Table
    from ._vector import Vector
    
    # Handle file path vs file object
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding, newline='') as f:
            return _read_csv_from_file(f, delimiter=delimiter, has_header=has_header)
    else:
        return _read_csv_from_file(file, delimiter=delimiter, has_header=has_header)


def _read_csv_from_file(file_obj: TextIO, *, delimiter: str, has_header: bool):
    """Read CSV data from an open file object."""
    from .table import Table
    from ._vector import Vector
    
    reader = csv.reader(file_obj, delimiter=delimiter)
    
    # Read all rows first
    all_rows = list(reader)
    
    if not all_rows:
        return Table()
    
    # Determine header and data rows
    if has_header:
        header = all_rows[0]
        rows = all_rows[1:]
    else:
        # Generate default column names: col_0, col_1, etc.
        header = [f"col_{i}" for i in range(len(all_rows[0]))]
        rows = all_rows
    
    if not rows:
        # Header only, no data
        return Table({col: Vector() for col in header})
    
    # Transpose rows into columns
    num_cols = len(header)
    columns = []
    
    for col_idx in range(num_cols):
        column_data = []
        for row in rows:
            # Handle jagged rows
            if col_idx < len(row):
                value = row[col_idx]
                # Try to infer type
                column_data.append(_infer_type(value))
            else:
                column_data.append(None)
        columns.append(Vector(column_data, name=header[col_idx]))
    
    return Table(columns)


def _infer_type(value: str):
    """
    Attempt to convert string value to int, float, or leave as string.
    
    Returns None for empty strings.
    """
    if not value or value.strip() == '':
        return None
    
    value = value.strip()
    
    # Try int
    try:
        return int(value)
    except ValueError:
        pass
    
    # Try float
    try:
        return float(value)
    except ValueError:
        pass
    
    # Keep as string
    return value

