"""Display and repr logic for Vector and Table."""

from __future__ import annotations
import math
from datetime import date
from typing import List
from .naming import _get_reserved_names
from .naming import _sanitize_user_name
from .naming import _disambiguate


# How many rows/columns to show before inserting "..."
_REPR_ROWS_DEFAULT = 12  # Global default for total rows shown (head+tail)
MAX_HEAD_COLS = 5


def set_repr_rows(n: int | None):
    """Set the default number of rows shown in Table.__repr__.
    
    Parameters
    ----------
    n : int | None
        Total rows to show (head+tail). If None, reset to library default (12).
        
    Examples
    --------
    >>> import serif
    >>> serif.set_repr_rows(20)  # Show 20 rows total (10 head + 10 tail)
    >>> # All future table reprs will show 20 rows
    >>> serif.set_repr_rows(None)  # Reset to default
    """
    if n is not None and (not isinstance(n, int) or isinstance(n, bool) or n <= 0):
        from .errors import SerifTypeError
        raise SerifTypeError(
            f"set_repr_rows expects a positive int or None, got {n!r}"
        )
    global _REPR_ROWS_DEFAULT
    _REPR_ROWS_DEFAULT = n if n is not None else 12


def _needs_quote(name: str) -> bool:
    """Determine if a column name needs quoting in repr output.
    
    A name needs quoting if:
    - It's empty
    - It's not a valid Python identifier
    - It starts with a digit
    - It parses as a number
    - It collides with Vector/Table reserved method names
    """
    # Always quote empty names
    if not name:
        return True

    # If it's not a valid identifier (spaces, punctuation, etc.)
    if not name.isidentifier():
        return True

    # If it starts with a digit, even if isidentifier() somehow allowed it
    if name[0].isdigit():
        return True

    # If it parses as a number (int, float, maybe with sign), quote it
    try:
        float(name)
        return True
    except ValueError:
        pass

    # If the name collides with a method/attribute name, quote it
    if name.lower() in _get_reserved_names():
        return True

    return False


def _format_column(col, max_preview: int | None = None) -> List[str]:
    """Returns a list of strings representing that column, truncated for display."""
    # Use global default if not specified
    if max_preview is None:
        max_preview = _REPR_ROWS_DEFAULT // 2
    
    # Truncate with symmetric preview
    vals = col._storage.to_tuple()
    if len(vals) > max_preview * 2:
        preview = list(vals[:max_preview]) + ['...'] + list(vals[-max_preview:])
    else:
        preview = list(vals)

    # Type-sensitive formatting
    out = []
    for v in preview:
        if v == '...':
            out.append('...')
        elif v is None:
            out.append('None')
        elif col._dtype and col._dtype.kind is float:
            if not math.isfinite(v):
                out.append(str(v))  # 'nan', 'inf', '-inf' — int(v) would raise
            else:
                out.append(f"{v:.1f}" if v == int(v) else f"{v:g}")
        elif col._dtype and col._dtype.kind is int:
            out.append(str(v))
        elif col._dtype and col._dtype.kind is date:
            out.append(v.isoformat())
        elif col._dtype and col._dtype.kind is str:
            # Pure str columns: no quotes (type already known from footer)
            out.append(str(v) if v is not None else 'None')
        else:
            # Object type - quote strings to distinguish from other types
            if isinstance(v, str):
                out.append(repr(v))
            else:
                out.append(str(v))

    # Align: numeric right, others left
    max_len = max(len(s) for s in out) if out else 0
    if col._dtype and col._dtype.kind in (int, float):
        return [s.rjust(max_len) for s in out]
    return [s.ljust(max_len) for s in out]


def _compute_headers(cols, col_indices):
    """Given Table columns and indices, returns display_names, sanitized_names, dtypes."""
    display_names = []
    sanitized_names = []
    dtypes = []
    seen = set()

    for idx in col_indices:
        col = cols[idx]

        # Display name
        disp = col._name or ""
        display_names.append(disp)

        # Sanitized dot name
        if col._name:
            san = _sanitize_user_name(col._name)
            if san is None:
                san = f"col{idx}_"
            elif san in seen:
                san = _disambiguate(san, idx)
            else:
                seen.add(san)
        else:
            san = f"col{idx}_"
        sanitized_names.append(san)

        dtypes.append(_dtype_str(col))

    return display_names, sanitized_names, dtypes


def _dtype_str(col) -> str:
    """Display string for a column's dtype: 'category', kind name, or 'object',
    with a '?' suffix when nullable."""
    if not col._dtype:
        return "object"
    from serif._vector.categorical import _Category
    if isinstance(col, _Category):
        dtype_str = 'category'
    else:
        dtype_str = col._dtype.kind.__name__
    if col._dtype.nullable:
        dtype_str += '?'
    return dtype_str


def _group_dtypes(dtypes: List[str]) -> str:
    """Summarize per-column dtypes as counted groups for the table footer.

    Groups appear in column order (first appearance), so the footer reads
    like the table does. A count prefix (e.g. '6×str') is used when a dtype
    repeats, unless the table is homogeneous, where the bare dtype suffices.
    With five or more groups, only the first three are shown followed by
    '+N' — the number of columns folded away.

        <int>
        <str, int, date>
        <6×str, 2×int, date>
        <18×str, 12×int, 6×float, +4>
    """
    counts = {}
    for dt in dtypes:
        counts[dt] = counts.get(dt, 0) + 1

    # dict preserves insertion order == first appearance == column order
    groups = list(counts)

    if len(groups) == 1:
        return groups[0]

    if len(groups) > 4:
        hidden = sum(counts[dt] for dt in groups[3:])
        groups = groups[:3]
    else:
        hidden = 0

    parts = [f"{counts[dt]}×{dt}" if counts[dt] > 1 else dt for dt in groups]
    if hidden:
        parts.append(f"+{hidden}")
    return ", ".join(parts)


def _is_structural_change(display_name: str, sanitized_name: str) -> bool:
    """Check if sanitization involved structural changes beyond just case normalization.
    
    Returns True if there were meaningful transformations like:
    - Character removal/substitution (spaces, punctuation)
    - Prefix addition (digit handling)
    - Suffix addition (reserved name collision)
    
    Returns False if only case changed.
    """
    if not display_name or not sanitized_name:
        return True
    
    # If lowercasing the display name equals sanitized, it's just case change
    if display_name.lower() == sanitized_name:
        return False
    
    # Otherwise there was a structural transformation
    return True


def _header_rows(display_names, sanitized_names, dtypes):
    """Decide which header rows to show based on display vs sanitized names.

    Returns (header_rows, show_types_in_header) where show_types_in_header indicates
    whether types are heterogeneous and shown per-column in the header.
    """
    any_display = any(n for n in display_names if n != "...")
    
    # Only show dot-access row if there's a structural change, not just case
    any_structural_change = any(
        _is_structural_change(disp, san)
        for disp, san in zip(display_names, sanitized_names)
        if disp != "..." and san != "..."
    )

    # Check if types are homogeneous (excluding "...")
    unique_types = set(dt for dt in dtypes if dt != "...")
    show_types_in_header = len(unique_types) > 1

    rows = []

    # Row 1: display names (quoted if needed)
    if any_display:
        row = []
        for name in display_names:
            if name == "...":
                row.append("...")
            elif _needs_quote(name):
                row.append(repr(name))
            else:
                row.append(name if name else "")
        rows.append(row)

    # Row 2: sanitized names (only if structural change or no display names)
    if any_structural_change or not any_display:
        rows.append([("." + san) if san and san != "..." else san for san in sanitized_names])

    # Row 3 (or 2): type annotations (only if heterogeneous)
    if show_types_in_header:
        rows.append([f"[{dt}]" if dt != "..." else "..." for dt in dtypes])

    return rows, show_types_in_header


def _align_columns(formatted_cols, header_rows, col_dtypes):
    """Pad columns and headers to consistent widths."""
    num_cols = len(formatted_cols)
    col_widths = []

    # Compute desired width per column
    for c in range(num_cols):
        body_width = max(len(s) for s in formatted_cols[c]) if formatted_cols[c] else 0
        header_width = max(
            len(header_rows[r][c]) for r in range(len(header_rows))
        ) if header_rows else 0
        col_widths.append(max(body_width, header_width))

    # Re-pad columns based on dtype
    aligned_cols = []
    for c in range(num_cols):
        col = formatted_cols[c]
        w = col_widths[c]
        dtype = col_dtypes[c]
        if dtype in ('int', 'float'):
            aligned_cols.append([s.rjust(w) for s in col])
        else:
            aligned_cols.append([s.ljust(w) for s in col])

    # Re-pad headers
    aligned_headers = []
    for row in header_rows:
        aligned_headers.append([h.rjust(col_widths[c]) if col_dtypes[c] in ('int', 'float') else h.ljust(col_widths[c]) 
                                for c, h in enumerate(row)])

    return aligned_cols, aligned_headers


def _footer(pv, dtype_text=None) -> str:
    """Generate footer line based on shape and dtypes.

    dtype_text, when given, overrides the computed dtype string for the
    2-D case (the grouped per-column summary from _group_dtypes)."""
    shape = pv.shape
    if not shape:
        return "# empty"
    
    if len(shape) == 1:
        from serif._vector.categorical import _Category
        if isinstance(pv, _Category):
            dt = 'category'
            if pv._dtype and pv._dtype.nullable:
                dt += '?'
        elif pv._dtype:
            dt = pv._dtype.kind.__name__
            if pv._dtype.nullable:
                dt += "?"
        else:
            dt = "object"
        return f"# {len(pv)} element vector <{dt}>"
    
    if len(shape) == 2:
        if dtype_text is not None:
            d = dtype_text
        else:
            d = pv._dtype.kind.__name__ if pv._dtype else "object"
        rows, cols = shape
        return f"# {rows}×{cols} table <{d}>"
    
    shape_str = "×".join(str(s) for s in shape)
    if pv._dtype:
        dt = pv._dtype.kind.__name__
        if pv._dtype.nullable:
            dt += "?"
    else:
        dt = "object"
    return f"# {shape_str} tensor <{dt}>"


def _repr_vector(v) -> str:
    """Pretty repr for a 1D Vector."""
    formatted = _format_column(v)
    
    # Compute width: max of data and header (if present)
    data_width = max(len(s) for s in formatted) if formatted else 0
    header_width = 0
    if v._name:
        header_text = repr(v._name) if _needs_quote(v._name) else v._name
        header_width = len(header_text)
    
    width = max(data_width, header_width)
    
    # Re-align data to match combined width
    if v._dtype and v._dtype.kind in (int, float):
        formatted = [s.rjust(width) for s in formatted]
    else:
        formatted = [s.ljust(width) for s in formatted]
    
    lines = []

    # Optional vector name
    if v._name:
        lines.append(header_text.ljust(width) if not v._dtype or v._dtype.kind not in (int, float) else header_text.rjust(width))

    lines.extend(formatted)
    lines.append("")
    lines.append(_footer(v))
    return "\n".join(lines)


def _repr_table(tbl) -> str:
    """Pretty repr for a 2D Table."""
    
    # Check if table has custom repr_rows setting
    max_preview = None
    if hasattr(tbl, '_repr_rows') and tbl._repr_rows is not None:
        max_preview = tbl._repr_rows // 2
    
    cols = tbl.cols()
    num_cols = len(cols)

    if num_cols == 0:
        return "# 0×0 table"

    truncated = num_cols > MAX_HEAD_COLS * 2

    if truncated:
        col_indices = list(range(MAX_HEAD_COLS)) + list(range(num_cols - MAX_HEAD_COLS, num_cols))
    else:
        col_indices = list(range(num_cols))

    # Headers + dtypes
    disp, san, dtypes_displayed = _compute_headers(cols, col_indices)

    # Get all dtypes for footer
    dtypes_all = [_dtype_str(col) for col in cols]

    # Format columns
    formatted_cols = [_format_column(cols[i], max_preview=max_preview) for i in col_indices]

    # Insert "..." column if truncated
    if truncated:
        ellipsis_col = ["..." for _ in range(len(formatted_cols[0]))]
        formatted_cols.insert(MAX_HEAD_COLS, ellipsis_col)
        disp.insert(MAX_HEAD_COLS, "...")
        san.insert(MAX_HEAD_COLS, "...")
        dtypes_displayed.insert(MAX_HEAD_COLS, "...")

    # Build header rows
    header_rows, _ = _header_rows(disp, san, dtypes_displayed)

    # Align everything
    aligned_cols, aligned_headers = _align_columns(formatted_cols, header_rows, dtypes_displayed)

    # Build output
    lines = []
    for hrow in aligned_headers:
        lines.append("  ".join(hrow))

    # Table body
    nrows = len(aligned_cols[0]) if aligned_cols else 0
    for r in range(nrows):
        row = "  ".join(col[r] for col in aligned_cols)
        lines.append(row)

    lines.append("")

    # Footer: grouped dtype summary, e.g. <6×str, 2×int, date>
    lines.append(_footer(tbl, dtype_text=_group_dtypes(dtypes_all)))

    return "\n".join(lines)


def _printr(pv) -> str:
    """Entry point used by Vector.__repr__ and Table.__repr__."""
    nd = len(pv.shape)
    if nd == 1:
        return _repr_vector(pv)
    if nd == 2:
        return _repr_table(pv)
    return _footer(pv) + " (repr not yet implemented)"

