"""Display and repr logic for Vector and Table."""

from __future__ import annotations
from datetime import date
from typing import List
from .naming import _get_reserved_names
from .naming import _sanitize_user_name


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
	vals = col._underlying
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
				sep = "" if san.endswith("_") else "_"
				san = f"{san}{sep}_{idx}"
			else:
				seen.add(san)
		else:
			san = f"col{idx}_"
		sanitized_names.append(san)

		# Dtype (with nullable indicator)
		if col._dtype:
			dtype_str = col._dtype.kind.__name__
			if col._dtype.nullable:
				dtype_str += "?"
			dtypes.append(dtype_str)
		else:
			dtypes.append("object")

	return display_names, sanitized_names, dtypes


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
	whether types are heterogeneous and should be shown in header instead of footer.
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


def _footer(pv, dtype_list=None, truncated=False, shown=MAX_HEAD_COLS) -> str:
	"""Generate footer line based on shape and dtypes."""
	shape = pv.shape
	if not shape:
		return "# empty"
	
	if len(shape) == 1:
		if pv._dtype:
			dt = pv._dtype.kind.__name__
			if pv._dtype.nullable:
				dt += "?"
		else:
			dt = "object"
		return f"# {len(pv)} element vector <{dt}>"
	
	if len(shape) == 2:
		if dtype_list:
			if truncated:
				d = ", ".join(dtype_list[:shown]) + ", ..., " + ", ".join(dtype_list[-shown:])
			else:
				d = ", ".join(dtype_list)
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
	dtypes_all = []
	for col in cols:
		if col._dtype:
			dtype_str = col._dtype.kind.__name__
			if col._dtype.nullable:
				dtype_str += "?"
			dtypes_all.append(dtype_str)
		else:
			dtypes_all.append("object")

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
	header_rows, show_types_in_header = _header_rows(disp, san, dtypes_displayed)

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
	
	# Footer: use <mixed> if types are in header, otherwise show type info
	if show_types_in_header:
		lines.append(_footer(tbl, None, False, MAX_HEAD_COLS).replace(f"<{tbl._dtype.kind.__name__ if tbl._dtype else 'object'}>", "<mixed>"))
	else:
		# Check if all dtypes are the same (homogeneous table)
		unique_dtypes = set(dtypes_all)
		if len(unique_dtypes) == 1:
			# Homogeneous - show single type
			lines.append(_footer(tbl, None, False, MAX_HEAD_COLS).replace(f"<{tbl._dtype.kind.__name__ if tbl._dtype else 'object'}>", f"<{dtypes_all[0]}>"))
		else:
			# Keep showing all types
			lines.append(_footer(tbl, dtypes_all, truncated, MAX_HEAD_COLS))

	return "\n".join(lines)


def _printr(pv) -> str:
	"""Entry point used by Vector.__repr__ and Table.__repr__."""
	nd = len(pv.shape)
	if nd == 1:
		return _repr_vector(pv)
	if nd == 2:
		return _repr_table(pv)
	return _footer(pv) + " (repr not yet implemented)"

